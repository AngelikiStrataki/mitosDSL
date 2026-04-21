# -*- coding: utf-8 -*-
import math
import datetime
import textwrap
from collections import defaultdict

# --- Runtime helpers (shared by generated script + API functions) ---
from collections import defaultdict
import datetime

def _safe_round(x, p=None):
    try:
        if x is None:
            return None
        return round(x, p) if p is not None else round(x)
    except Exception:
        return x

def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).strip()
        if s.count('.') == 1 and s.replace('.', '', 1).replace('-', '', 1).isdigit():
            return float(s)
        if s.replace('-', '', 1).isdigit():
            return float(int(s))
    except Exception:
        pass
    return None

def _norm_key(v):
    try:
        if isinstance(v, str):
            return v.strip()
        return v
    except Exception:
        return v

def _get_any(d, key):
    if key in d and d.get(key) is not None:
        return d.get(key)
    if isinstance(key, str):
        lk = key.lower()
        for k in d.keys():
            if isinstance(k, str) and k.lower() == lk and d.get(k) is not None:
                return d.get(k)
    return d.get(key) if key in d else None

def _get_q(d, full, base=None):
    v = _get_any(d, full) if full else None
    if v is not None:
        return v
    if base:
        v = _get_any(d, base)
        if v is not None:
            return v
    return None

def _parse_ts(v):
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            x = float(v)
            if x > 10_000_000_000:
                return x / 1000.0
            return x
        s = str(v).strip()
        if not s:
            return None
        if s.replace('.', '', 1).replace('-', '', 1).isdigit():
            x = float(s)
            if x > 10_000_000_000:
                return x / 1000.0
            return x
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.timestamp()
            return dt.timestamp()
        except Exception:
            return None
    except Exception:
        return None

def _pick_ts_field(row):
    for k in ("ts", "timestamp", "time", "datetime", "date"):
        if k in row:
            return k
    for k in row.keys():
        if isinstance(k, str) and k.lower() in ("ts", "timestamp", "time", "datetime", "date"):
            return k
    return None

def _apply_window(rows, win_spec, kind, value, unit=None):
    if not rows:
        return rows

    if kind == "samples":
        try:
            n = int(value)
            if n <= 0:
                return []
            return rows[-n:] if len(rows) > n else rows
        except Exception:
            return rows

    try:
        mult = 1.0
        if unit == "ms":
            mult = 0.001
        elif unit == "s":
            mult = 1.0
        elif unit == "m":
            mult = 60.0
        window_seconds = float(value) * mult

        ts_key = _pick_ts_field(rows[0])
        if not ts_key:
            return rows

        ts_vals = []
        for r in rows:
            t = _parse_ts(_get_any(r, ts_key))
            if t is not None:
                ts_vals.append(t)
        if not ts_vals:
            return rows

        tmax = max(ts_vals)
        tmin = tmax - window_seconds
        out = []
        for r in rows:
            t = _parse_ts(_get_any(r, ts_key))
            if t is None:
                continue
            if t >= tmin:
                out.append(r)
        return out if out else rows
    except Exception:
        return rows

def _apply_alias_prefix(rows, alias):
    if not rows or not alias:
        return rows
    out = []
    for r in rows:
        rr = dict(r)
        for k, v in r.items():
            kk = f"{alias}.{k}"
            if kk not in rr:
                rr[kk] = v
        out.append(rr)
    return out


# ==============================================================================
# MQTT helpers (continuous mode, NO time-based retention)
# The ONLY time-based filtering is done later by WINDOW in your query logic.
# ==============================================================================
import threading
import time
import json
import re
from collections import deque

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

_MQTT_STARTED = set()
_MQTT_CLIENTS = {}   # source_name -> client
_MQTT_BUFFERS = {}   # source_name -> deque[dict]
_MQTT_LOCKS = {}     # source_name -> threading.Lock()


def _parse_host_port(host_str, default_port=1883):
    """
    Accepts:
      - "localhost:1883"
      - "localhost"
    Returns: (host, port_int)
    """
    try:
        s = str(host_str).strip()
        if ":" in s:
            h, p = s.rsplit(":", 1)
            return h.strip(), int(p.strip())
        return s, int(default_port)
    except Exception:
        return str(host_str), int(default_port)


# --------- MQTT payload parsing (FIX): JSON -> fallback key:value parsing ----------
def _to_float_or_none(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            return float(s)
        return None
    except Exception:
        return None

def _parse_kv_payload(payload_s):
    """
    Accepts:
      - proper JSON: {"a":1,"b":2}
      - relaxed kv:  a:1,b:2
      - relaxed dict-ish: {a:1, b:2}
    Returns dict.
    """
    if isinstance(payload_s, bytes):
        payload_s = payload_s.decode("utf-8", errors="replace")
    txt = str(payload_s).strip()

    # 1) strict JSON
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return obj
        # if JSON but not dict, wrap
        return {"value": obj}
    except Exception:
        pass

    # 2) relaxed "{a:1,b:2}" -> "a:1,b:2"
    if txt.startswith("{") and txt.endswith("}"):
        txt = txt[1:-1].strip()

    out = {}
    if not txt:
        return out

    # 3) parse comma-separated key:value
    for part in txt.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().strip('"').strip("'")
        v = v.strip().strip('"').strip("'")
        fv = _to_float_or_none(v)
        out[k] = fv if fv is not None else v

    # if nothing parsed, keep raw
    return out if out else {"raw": str(payload_s)}
# -------------------------------------------------------------------------------


def _start_mqtt_continuous(source_name, host, port=None, topic=None, qos=0, client_id=None, max_messages=200000):
    """
    Starts a background MQTT consumer ONCE per source_name and keeps messages in memory.
    NO time-based pruning is applied. A non-time-based cap (max_messages) protects RAM.

    - If payload lacks a timestamp field, injects 'ts' as epoch seconds to enable WINDOW filtering.
    """
    if mqtt is None:
        raise RuntimeError("paho-mqtt is not installed. Run: pip install paho-mqtt")

    if source_name in _MQTT_STARTED:
        return

    if port is None:
        # allow passing host like "localhost:1883"
        host_only, port_val = _parse_host_port(host)
    else:
        host_only, port_val = str(host).strip(), int(port)

    if not topic:
        raise ValueError("MQTT topic is required")

    # No time-based retention. Keep ALL messages, but cap by count (non-time).
    # deque(maxlen=N) automatically drops oldest when full.
    buf = deque(maxlen=int(max_messages) if max_messages is not None else None)
    lock = threading.Lock()

    _MQTT_BUFFERS[source_name] = buf
    _MQTT_LOCKS[source_name] = lock

    def on_connect(client, userdata, flags, rc):
        try:
            client.subscribe(topic, qos=qos)
        except Exception as e:
            print(f"[MQTT][{source_name}] subscribe error:", e)

    def on_message(client, userdata, msg):
        now = time.time()
        try:
            payload = msg.payload.decode("utf-8", errors="replace")

            # FIX: tolerant parsing (JSON or relaxed kv)
            row = _parse_kv_payload(payload)
            if not isinstance(row, dict):
                row = {"value": row}

            # Keep useful debug fields
            row.setdefault("topic", getattr(msg, "topic", None))
            row.setdefault("raw", payload)

            # Ensure there is a timestamp field for your WINDOW logic
            if _pick_ts_field(row) is None:
                row["ts"] = now

            with lock:
                buf.append(row)

        except Exception as e:
            # If something still goes wrong, do NOT spam parse errors for every message.
            # Just store raw.
            fallback = {"topic": getattr(msg, "topic", None), "raw": None, "ts": now}
            try:
                fallback["raw"] = msg.payload.decode("utf-8", errors="replace")
            except Exception:
                fallback["raw"] = str(msg.payload)
            with lock:
                buf.append(fallback)
            print(f"[MQTT][{source_name}] parse error:", e)

    try:
        if client_id:
            client = mqtt.Client(client_id=str(client_id))
        else:
            client = mqtt.Client()
    except Exception:
        client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(host_only, int(port_val), keepalive=60)
    client.loop_start()

    _MQTT_CLIENTS[source_name] = client
    _MQTT_STARTED.add(source_name)


def mqtt_continuous_rows(source_name):
    """
    Returns a snapshot list[dict] of all currently buffered messages for this source.
    """
    buf = _MQTT_BUFFERS.get(source_name)
    lock = _MQTT_LOCKS.get(source_name)
    if not buf or not lock:
        return []
    with lock:
        return list(buf)

# --- Query result cache (prevents re-running dependencies) ---
_QUERY_CACHE = {}
_QUERY_IN_PROGRESS = set()


CORE_plotRaw = textwrap.dedent(r"""\














SEL_ALIAS = {
}


BASE_ALIAS = {
}

import pymongo

_host_part = "localhost:27017"
_user      = ""
_pass      = ""
_database  = "smartfarm"
_dataset   = ""

client = pymongo.MongoClient(
    f"mongodb://localhost:27017"
)

db_name = _database if _database else _dataset
db = client[db_name]
collection = db["Plots"]

data = list(collection.find({}, {"_id": 0}))


data = _apply_alias_prefix(data, "p")


try:
    client.close()
except Exception:
    pass




filtered = data

def __to_num_if_numeric(v):
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in ('true','false'):
            return s.lower() == 'true'
        if s.isdigit():
            try: return int(s)
            except: pass
        try:
            if s.count('.') == 1 and s.replace('.', '', 1).replace('-', '', 1).isdigit():
                return float(s)
        except: pass
    return v

def __cmp(op, lv, rv):
    try:
        if op == "==": return lv == rv
        elif op == "!=": return lv != rv
        elif op == ">":  return lv > rv
        elif op == "<":  return lv < rv
        elif op == ">=": return lv >= rv
        elif op == "<=": return lv <= rv
        else: return False
    except: return False

def __AND(vals):
    for b in vals:
        if not b: return False
    return True

def __OR(vals):
    return any(bool(b) for b in vals)





filtered = data

GB_FULL = [
]

GB_BASE = [
]

GB_OUT = [
]

HAS_GROUPS = True if GB_FULL else False
HAS_AGGS   = False

def _compute_aggs_plotRaw(rows):
    result_aggs = {}
    return result_aggs

if not HAS_GROUPS and not HAS_AGGS:
    results = []
    for row in filtered:
        out = {}
        for k, v in row.items():
            out[k] = v
        results.append(out)
else:
    groups = defaultdict(list)
    if HAS_GROUPS:
        for row in filtered:
            key = tuple(_get_q(row, GB_FULL[i], GB_BASE[i]) for i in range(len(GB_FULL)))
            groups[key].append(row)
    else:
        groups[None] = filtered

    results = []
    for key, rows in groups.items():
        result = {}

        if HAS_GROUPS:
            if isinstance(key, tuple):
                for i, g in enumerate(GB_OUT):
                    val = key[i]
                    result[g] = val
                    for _base, _alias in BASE_ALIAS.items():
                        if _alias == g and _base not in result:
                            result[_base] = val

        if HAS_AGGS:
            result.update(_compute_aggs_plotRaw(rows))

        results.append(result)



results = results[: 2 ]

""").lstrip()

def run_plotRaw(force: bool = False):
    """
    Auto-generated from DSL.
    Returns: list[dict] or dict (JSON-serializable)
    """
    # MQTT queries are "live": do not serve from cache unless user explicitly wants it
    if (not force) and (not False) and "plotRaw" in _QUERY_CACHE:
        return _QUERY_CACHE["plotRaw"]

    if "plotRaw" in _QUERY_IN_PROGRESS:
        raise RuntimeError("Cyclic dependency detected while running: plotRaw")

    _QUERY_IN_PROGRESS.add("plotRaw")
    try:
        # IMPORTANT: exec in ONE shared namespace so lambdas can see helpers defined in CORE (e.g. _sort_key)
        _env = dict(globals())
        _env["__name__"] = __name__
        exec(CORE_plotRaw, _env, _env)

        res = _env.get("results", [])
        _QUERY_CACHE["plotRaw"] = res
        return res
    finally:
        _QUERY_IN_PROGRESS.discard("plotRaw")


CORE_liveByPlot = textwrap.dedent(r"""\














SEL_ALIAS = {
"plotId": ("t.plotId", "plotId"),"moisture": ("t.moisture", "moisture"),"temp": ("t.temp", "temp"),}


BASE_ALIAS = {
"plotId": "plotId",
"moisture": "moisture",
"temp": "temp",
}


_host_part   = "localhost:1883"
_topic       = "farm/+/telemetry"
_client_id   = "dsl-client-farm"

if not _topic:
    data = []
else:
    _start_mqtt_continuous(
        "farmStream",
        host=_host_part,
        topic=_topic,
        client_id=_client_id,
        # max_messages=200000
    )
    data = mqtt_continuous_rows("farmStream")
    if not data:
        import time as _t
        _t.sleep(0.25)
        data = mqtt_continuous_rows("farmStream")

data = _apply_alias_prefix(data, "t")




filtered = data

def __to_num_if_numeric(v):
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in ('true','false'):
            return s.lower() == 'true'
        if s.isdigit():
            try: return int(s)
            except: pass
        try:
            if s.count('.') == 1 and s.replace('.', '', 1).replace('-', '', 1).isdigit():
                return float(s)
        except: pass
    return v

def __cmp(op, lv, rv):
    try:
        if op == "==": return lv == rv
        elif op == "!=": return lv != rv
        elif op == ">":  return lv > rv
        elif op == "<":  return lv < rv
        elif op == ">=": return lv >= rv
        elif op == "<=": return lv <= rv
        else: return False
    except: return False

def __AND(vals):
    for b in vals:
        if not b: return False
    return True

def __OR(vals):
    return any(bool(b) for b in vals)





_tmp_filtered = []
for row in data:
    if __OR([ __AND([ ( ( __OR([ __AND([ ( ( __cmp(">",
      __to_num_if_numeric(_get_q(row, "t.moisture", "moisture")),
      __to_num_if_numeric(0) ) ) ), (not ( ( __OR([ __AND([ ( ( __cmp("==",
      __to_num_if_numeric(_get_q(row, "t.status", "status")),
      __to_num_if_numeric("maintenance") ) ) ) ]) ]) ) )) ]) ]) ) ) ]), __AND([ ( ( __OR([ __AND([ ( ( __cmp("==",
      __to_num_if_numeric(_get_q(row, "t.sensorOk", "sensorOk")),
      __to_num_if_numeric(_get_q(row, "true", "true")) ) ) ) ]) ]) ) ) ]) ]):
        _tmp_filtered.append(row)
filtered = _tmp_filtered

GB_FULL = [
"t.plotId"]

GB_BASE = [
"plotId"]

GB_OUT = [
"plotId"]

HAS_GROUPS = True if GB_FULL else False
HAS_AGGS   = True

def _compute_aggs_liveByPlot(rows):
    result_aggs = {}
    _rows_for_agg = rows
    _rows_for_agg = _apply_window(rows, None, "time", 10, "s")

    _func = "avg"
    _field_full = "t.moisture"
    _field_base = "moisture"
    if _func == 'count':
        _val = sum(1 for _r in _rows_for_agg if _get_q(_r, _field_full, _field_base) is not None)
    else:
        _vals = []
        for _r in _rows_for_agg:
            _nv = _num(_get_q(_r, _field_full, _field_base))
            if _nv is not None:
                _vals.append(_nv)

        if _func == 'sum':
            _val = sum(_vals) if _vals else 0
        elif _func == 'avg':
            _val = (sum(_vals) / len(_vals)) if _vals else None
        elif _func == 'min':
            _val = min(_vals) if _vals else None
        elif _func == 'max':
            _val = max(_vals) if _vals else None
        else:
            _val = None

    try:
        if _val is not None and isinstance(_val, (int, float)):
            _val = _safe_round(_val, 2)
    except Exception:
        pass

    result_aggs["liveMoisture10s"] = _val
    _rows_for_agg = rows
    _rows_for_agg = _apply_window(rows, None, "time", 10, "s")

    _func = "avg"
    _field_full = "t.temp"
    _field_base = "temp"
    if _func == 'count':
        _val = sum(1 for _r in _rows_for_agg if _get_q(_r, _field_full, _field_base) is not None)
    else:
        _vals = []
        for _r in _rows_for_agg:
            _nv = _num(_get_q(_r, _field_full, _field_base))
            if _nv is not None:
                _vals.append(_nv)

        if _func == 'sum':
            _val = sum(_vals) if _vals else 0
        elif _func == 'avg':
            _val = (sum(_vals) / len(_vals)) if _vals else None
        elif _func == 'min':
            _val = min(_vals) if _vals else None
        elif _func == 'max':
            _val = max(_vals) if _vals else None
        else:
            _val = None

    try:
        if _val is not None and isinstance(_val, (int, float)):
            _val = _safe_round(_val, 2)
    except Exception:
        pass

    result_aggs["liveTemp10s"] = _val
    return result_aggs

if not HAS_GROUPS and not HAS_AGGS:
    results = []
    for row in filtered:
        out = {}
        out["plotId"] = _get_q(row, "t.plotId", "plotId")
        out["moisture"] = _get_q(row, "t.moisture", "moisture")
        out["temp"] = _get_q(row, "t.temp", "temp")
        results.append(out)
else:
    groups = defaultdict(list)
    if HAS_GROUPS:
        for row in filtered:
            key = tuple(_get_q(row, GB_FULL[i], GB_BASE[i]) for i in range(len(GB_FULL)))
            groups[key].append(row)
    else:
        groups[None] = filtered

    results = []
    for key, rows in groups.items():
        result = {}

        if HAS_GROUPS:
            if isinstance(key, tuple):
                for i, g in enumerate(GB_OUT):
                    val = key[i]
                    result[g] = val
                    for _base, _alias in BASE_ALIAS.items():
                        if _alias == g and _base not in result:
                            result[_base] = val

        if HAS_AGGS:
            result.update(_compute_aggs_liveByPlot(rows))

        results.append(result)

def _sort_key(val):
    if val is None:
        return (2, "")
    if isinstance(val, (int, float)):
        return (0, val)
    s = str(val).strip()
    num_like = s.replace('-', '', 1).replace('.', '', 1)
    if num_like.isdigit():
        try:
            return (0, float(s))
        except Exception:
            pass
    return (1, s.casefold())

results.sort(
    key=lambda r: _sort_key(_get_q(r, "liveMoisture10s", "liveMoisture10s")),
    reverse=False
)


results = results[: 20 ]

""").lstrip()

def run_liveByPlot(force: bool = False):
    """
    Auto-generated from DSL.
    Returns: list[dict] or dict (JSON-serializable)
    """
    # MQTT queries are "live": do not serve from cache unless user explicitly wants it
    if (not force) and (not True) and "liveByPlot" in _QUERY_CACHE:
        return _QUERY_CACHE["liveByPlot"]

    if "liveByPlot" in _QUERY_IN_PROGRESS:
        raise RuntimeError("Cyclic dependency detected while running: liveByPlot")

    _QUERY_IN_PROGRESS.add("liveByPlot")
    try:
        # IMPORTANT: exec in ONE shared namespace so lambdas can see helpers defined in CORE (e.g. _sort_key)
        _env = dict(globals())
        _env["__name__"] = __name__
        exec(CORE_liveByPlot, _env, _env)

        res = _env.get("results", [])
        _QUERY_CACHE["liveByPlot"] = res
        return res
    finally:
        _QUERY_IN_PROGRESS.discard("liveByPlot")


CORE_histByPlot = textwrap.dedent(r"""\
















SEL_ALIAS = {
"plotId": ("h.plotId", "plotId"),"moisture": ("h.moisture", "moisture"),}


BASE_ALIAS = {
"plotId": "plotId",
"moisture": "moisture",
}

import mysql.connector

_host_part = "localhost:3306"
_user      = "root"
_pass      = "Agg2704!"
_database  = ""
_dataset   = "smartfarm"

_host, _port = _host_part.split(':') if ':' in _host_part else (_host_part, "3306")
_db = _database if _database else (_dataset or None)

conn = mysql.connector.connect(
    host=_host,
    port=int(_port),
    database=_db,
    user=_user,
    password=_pass
)
cursor = conn.cursor()






query_sql = "SELECT h.plotId AS plotId, ROUND(AVG(h.moisture), 2) AS histMoistureAvg, ROUND(MIN(h.moisture), 2) AS histMoistureMin, ROUND(MAX(h.moisture), 2) AS histMoistureMax FROM SoilHistory AS h"




query_sql += " WHERE ( ( ( ( ( ( ( ( ( h.moisture >= 0 ) ) ) ) ) ) ) ) )"

query_sql += " GROUP BY h.plotId"



cursor.execute(query_sql)
rows = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]
data = [dict(zip(column_names, row)) for row in rows]
conn.close()





results = data
""").lstrip()

def run_histByPlot(force: bool = False):
    """
    Auto-generated from DSL.
    Returns: list[dict] or dict (JSON-serializable)
    """
    # MQTT queries are "live": do not serve from cache unless user explicitly wants it
    if (not force) and (not False) and "histByPlot" in _QUERY_CACHE:
        return _QUERY_CACHE["histByPlot"]

    if "histByPlot" in _QUERY_IN_PROGRESS:
        raise RuntimeError("Cyclic dependency detected while running: histByPlot")

    _QUERY_IN_PROGRESS.add("histByPlot")
    try:
        # IMPORTANT: exec in ONE shared namespace so lambdas can see helpers defined in CORE (e.g. _sort_key)
        _env = dict(globals())
        _env["__name__"] = __name__
        exec(CORE_histByPlot, _env, _env)

        res = _env.get("results", [])
        _QUERY_CACHE["histByPlot"] = res
        return res
    finally:
        _QUERY_IN_PROGRESS.discard("histByPlot")


CORE_smartFarmRiskDashboard = textwrap.dedent(r"""\














SEL_ALIAS = {
"plotId": ("h.plotId", "plotId"),"histMoistureAvg": ("h.histMoistureAvg", "histMoistureAvg"),"histMoistureMin": ("h.histMoistureMin", "histMoistureMin"),"histMoistureMax": ("h.histMoistureMax", "histMoistureMax"),}


BASE_ALIAS = {
"plotId": "plotId",
"histMoistureAvg": "histMoistureAvg",
"histMoistureMin": "histMoistureMin",
"histMoistureMax": "histMoistureMax",
}

data = run_liveByPlot()
data = _apply_alias_prefix(data, "r")


from collections import defaultdict as _ddict

_left_full  = "r.plotId"
_right_full = "h.plotId"
_left_base  = "plotId"
_right_base = "plotId"
_join_type = "right"
_join_alias = "h"


_right = run_histByPlot()
_right = _apply_alias_prefix(_right, _join_alias)

def _jkey_norm(v):
    try:
        return v.strip().casefold() if isinstance(v, str) else v
    except Exception:
        return v

_right_index = _ddict(list)
for _r in _right:
    _rk = _jkey_norm(_get_q(_r, _right_full, _right_base))
    if _rk is not None:
        _right_index[_rk].append(_r)

_new_data = []
if _join_type in ("inner", "left"):
    for _l in data:
        _lk = _jkey_norm(_get_q(_l, _left_full, _left_base))
        _matches = _right_index.get(_lk, [])
        if _matches:
            for _r in _matches:
                _m = dict(_l)
                for _k, _v in _r.items():
                    if _k not in _m:
                        _m[_k] = _v
                _new_data.append(_m)
        elif _join_type == "left":
            _new_data.append(dict(_l))
elif _join_type == "right":
    _left_index = _ddict(list)
    for _l in data:
        _left_index[_jkey_norm(_get_q(_l, _left_full, _left_base))].append(_l)
    for _r in _right:
        _rk = _jkey_norm(_get_q(_r, _right_full, _right_base))
        _lms = _left_index.get(_rk, [])
        if _lms:
            for _l in _lms:
                _m = dict(_l)
                for _k, _v in _r.items():
                    if _k not in _m:
                        _m[_k] = _v
                _new_data.append(_m)
        else:
            _new_data.append(dict(_r))
else:
    _seen = set()
    for _l in data:
        _lk = _jkey_norm(_get_q(_l, _left_full, _left_base))
        _matches = _right_index.get(_lk, [])
        if _matches:
            for _r in _matches:
                _m = dict(_l)
                for _k, _v in _r.items():
                    if _k not in _m:
                        _m[_k] = _v
                _new_data.append(_m)
                _seen.add(id(_r))
        else:
            _new_data.append(dict(_l))
    for _r in _right:
        if id(_r) not in _seen:
            _new_data.append(dict(_r))

data = _new_data


filtered = data

def __to_num_if_numeric(v):
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in ('true','false'):
            return s.lower() == 'true'
        if s.isdigit():
            try: return int(s)
            except: pass
        try:
            if s.count('.') == 1 and s.replace('.', '', 1).replace('-', '', 1).isdigit():
                return float(s)
        except: pass
    return v

def __cmp(op, lv, rv):
    try:
        if op == "==": return lv == rv
        elif op == "!=": return lv != rv
        elif op == ">":  return lv > rv
        elif op == "<":  return lv < rv
        elif op == ">=": return lv >= rv
        elif op == "<=": return lv <= rv
        else: return False
    except: return False

def __AND(vals):
    for b in vals:
        if not b: return False
    return True

def __OR(vals):
    return any(bool(b) for b in vals)





filtered = data

GB_FULL = [
]

GB_BASE = [
]

GB_OUT = [
]

HAS_GROUPS = True if GB_FULL else False
HAS_AGGS   = False

def _compute_aggs_smartFarmRiskDashboard(rows):
    result_aggs = {}
    return result_aggs

if not HAS_GROUPS and not HAS_AGGS:
    results = []
    for row in filtered:
        out = {}
        out["plotId"] = _get_q(row, "h.plotId", "plotId")
        out["histMoistureAvg"] = _get_q(row, "h.histMoistureAvg", "histMoistureAvg")
        out["histMoistureMin"] = _get_q(row, "h.histMoistureMin", "histMoistureMin")
        out["histMoistureMax"] = _get_q(row, "h.histMoistureMax", "histMoistureMax")
        results.append(out)
else:
    groups = defaultdict(list)
    if HAS_GROUPS:
        for row in filtered:
            key = tuple(_get_q(row, GB_FULL[i], GB_BASE[i]) for i in range(len(GB_FULL)))
            groups[key].append(row)
    else:
        groups[None] = filtered

    results = []
    for key, rows in groups.items():
        result = {}

        if HAS_GROUPS:
            if isinstance(key, tuple):
                for i, g in enumerate(GB_OUT):
                    val = key[i]
                    result[g] = val
                    for _base, _alias in BASE_ALIAS.items():
                        if _alias == g and _base not in result:
                            result[_base] = val

        if HAS_AGGS:
            result.update(_compute_aggs_smartFarmRiskDashboard(rows))

        results.append(result)

def _sort_key(val):
    if val is None:
        return (2, "")
    if isinstance(val, (int, float)):
        return (0, val)
    s = str(val).strip()
    num_like = s.replace('-', '', 1).replace('.', '', 1)
    if num_like.isdigit():
        try:
            return (0, float(s))
        except Exception:
            pass
    return (1, s.casefold())

results.sort(
    key=lambda r: _sort_key(_get_q(r, "histMoistureAvg", "histMoistureAvg")),
    reverse=False
)


results = results[ 2 : 12 ]

""").lstrip()

def run_smartFarmRiskDashboard(force: bool = False):
    """
    Auto-generated from DSL.
    Returns: list[dict] or dict (JSON-serializable)
    """
    # MQTT queries are "live": do not serve from cache unless user explicitly wants it
    if (not force) and (not False) and "smartFarmRiskDashboard" in _QUERY_CACHE:
        return _QUERY_CACHE["smartFarmRiskDashboard"]

    if "smartFarmRiskDashboard" in _QUERY_IN_PROGRESS:
        raise RuntimeError("Cyclic dependency detected while running: smartFarmRiskDashboard")

    _QUERY_IN_PROGRESS.add("smartFarmRiskDashboard")
    try:
        # IMPORTANT: exec in ONE shared namespace so lambdas can see helpers defined in CORE (e.g. _sort_key)
        _env = dict(globals())
        _env["__name__"] = __name__
        exec(CORE_smartFarmRiskDashboard, _env, _env)

        res = _env.get("results", [])
        _QUERY_CACHE["smartFarmRiskDashboard"] = res
        return res
    finally:
        _QUERY_IN_PROGRESS.discard("smartFarmRiskDashboard")


AVAILABLE_QUERIES = [
    "plotRaw",     "liveByPlot",     "histByPlot",     "smartFarmRiskDashboard"]
