from textx import metamodel_from_file
from textx.export import metamodel_export
from jinja2 import Environment, FileSystemLoader
import os
import shutil
import sys
import textwrap

# 1) Φόρτωση grammar και δημιουργία metamodel
mm = metamodel_from_file('grammar.tx')

# 2) Εξαγωγή .dot γραφήματος (διάγραμμα κανόνων grammar)
metamodel_export(mm, 'metamodel.dot')

# 3) Φόρτωση μοντέλου από DSL αρχείο
model = mm.model_from_file('uc3.model')

# Προαιρετικό: αν υπάρχει το Graphviz 'dot', παράγαγε PNG
if shutil.which("dot"):
    os.system("dot -Tpng metamodel.dot -o metamodel.png")
else:
    print("ℹ️  Δεν βρέθηκε το 'dot' στο PATH. Παράλειψη παραγωγής metamodel.png.")

# 4) Συλλογή όλων των QueryStmt στοιχείων
queries = [s for s in model.statements if s.__class__.__name__ == 'QueryStmt']
print(f"🔎 Βρέθηκαν {len(queries)} queries: {[q.name for q in queries]}")

if not queries:
    print("⚠️  Δεν υπάρχουν δηλωμένα 'query' στο uc1.model. Έλεγξε το αρχείο μοντέλου.")
    sys.exit(0)

# 5) Ρύθμιση Jinja2 για rendering Python κώδικα από template
env = Environment(
    loader=FileSystemLoader('templates'),
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)

print("OPENAPI CWD:", os.getcwd())
print("ABS generated_queries.py:", os.path.abspath("generated_queries.py"))
print("ABS api_server.py:", os.path.abspath("api_server.py"))

def _fix_py_file(path: str):
    with open(path, "rb") as f:
        b = f.read()
    try:
        s = b.decode("utf-8-sig")
    except Exception:
        s = b.decode("utf-8", errors="replace")

    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = textwrap.dedent(s)
    s = s.lstrip("\ufeff \t\n")

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(s)

# 7) Παραγωγή generated_queries.py (API mode: on-demand functions)
queries_template = env.get_template('generated_queries.py.j2')
with open('generated_queries.py', 'w', encoding='utf-8', newline='\n') as f:
    f.write(queries_template.render(queries=queries))
_fix_py_file("generated_queries.py")
print("✅ Module με functions γράφτηκε στο 'generated_queries.py'")

# 8) Παραγωγή api_server.py (FastAPI server)
api_template = env.get_template('api_server.py.j2')
with open('api_server.py', 'w', encoding='utf-8', newline='\n') as f:
    f.write(api_template.render(queries=queries))
_fix_py_file("api_server.py")
print("✅ API server γράφτηκε στο 'api_server.py'")

# quick sanity check (πριν το OpenAPI!)
print("HEAD api_server.py:", open("api_server.py", "rb").read(120))
print("HEAD generated_queries.py:", open("generated_queries.py", "rb").read(120))

# 9) Παραγωγή OpenAPI spec file (JSON + προαιρετικά YAML)
try:
    import importlib.util
    import json

    spec = importlib.util.spec_from_file_location("api_server", "api_server.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    openapi_schema = mod.app.openapi()

    with open("openapi.json", "w", encoding="utf-8") as f:
        json.dump(openapi_schema, f, ensure_ascii=False, indent=2)
    print("✅ OpenAPI JSON γράφτηκε στο 'openapi.json'")

    try:
        import yaml
        with open("openapi.yaml", "w", encoding="utf-8") as f:
            yaml.safe_dump(openapi_schema, f, sort_keys=False, allow_unicode=True)
        print("✅ OpenAPI YAML γράφτηκε στο 'openapi.yaml'")
    except Exception:
        print("ℹ️  Δεν βρέθηκε PyYAML. Παράλειψη παραγωγής openapi.yaml (pip install pyyaml).")

except Exception as e:
    print(f"⚠️  Αποτυχία παραγωγής OpenAPI spec: {e}")

print("✅ Grammar diagram γράφτηκε στο 'metamodel.dot'")
print("Το 'metamodel.png' δημιουργήθηκε.")
