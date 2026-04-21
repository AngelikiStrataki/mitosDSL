# MitosDSL – Complete Installation & Execution Guide

## 1. Introduction

MitosDSL is a Model-Driven Engineering (MDE) system that enables:

- Definition of data sources and queries using a DSL
- Automatic validation of models
- Generation of executable Python code
- Automatic creation of a REST API (FastAPI)
- Access to queries via HTTP endpoints

---

## 2. Supported Data Sources

MitosDSL supports querying and integrating data from multiple heterogeneous sources:

### 1. Relational Databases (SQL)

- Example: MySQL  
- Structured data (tables, rows, columns)  
- Supports aggregations, filtering, joins  

---

### 2. Document Databases (NoSQL)

- Example: MongoDB  
- Semi-structured data (JSON-like documents)  
- Supports nested fields and flexible schemas  

---

### 3. Streaming Data Sources

- Example: MQTT  
- Real-time data streams  
- Supports continuous data ingestion and time-based processing  

---

## Unified Execution Model

A key feature of the system is the ability to **combine these sources in a single query**.

This is achieved through:
- A common intermediate representation
- Unified execution semantics
- Application-level joins across heterogeneous data

Example:

SQL (historical data) + MongoDB (metadata) + MQTT (real-time data)

The system enables seamless integration and analysis across all three types.

---

## 3. System Requirements

To run the system, the following are required:

- Python 3.10+
- pip

### Core Libraries

- **textX** → DSL parsing and model creation  
- **Jinja2** → Model-to-Text (M2T) code generation  
- **FastAPI** → REST API creation  
- **Uvicorn** → ASGI server for running the API  
- **PyYAML** → OpenAPI export  

Install all dependencies with:

```bash
pip install textX jinja2 fastapi uvicorn pyyaml
```

---

## 4. Installation

```bash
unzip mitosDSL.zip
cd mitosDSL
python -m venv venv
source venv/bin/activate
pip install textX jinja2 fastapi uvicorn pyyaml
```

---

## 5. Running the System

```bash
python generate_from_model.py
uvicorn api_server:app --reload
```

Open:

http://127.0.0.1:8000/docs

---

## 6. API Usage

The system exposes the following endpoints:

```http
GET /queries
GET /query/{queryName}
```

Each DSL-defined query is automatically exposed as a REST endpoint.

---

### GET /queries

```json
[
  "/query/salesAnalysis",
  "/query/energyConsumption",
  "/query/soilMoisture"
]
```

Returns all available queries defined in the DSL model.

---

### GET /query/{queryName}

Example:

GET /query/salesAnalysis

Executes the selected query and returns the results.

---

## 7. Workflow

DSL Models → Parsing → Validation → M2T → Code → API → Execution

---

## 8. Author

Angeliki Strataki
