# ⚙️ Enterprise Metadata-Driven Dynamic ETL Engine & Analytics

A dynamic, zero-code-addition data engineering framework built with **Azure Data Factory**, **SQL Server**, and **Databricks (PySpark & Delta Lake)**. Instead of creating bespoke pipelines for every new table, this framework ingests, validates, and models any dataset dynamically using centralized metadata tables and automated data quality quarantine routing.

---

## 🏛️ Architecture Overview

```mermaid
flowchart TD
    A[(Metadata Control DB<br/>SQL Server)] -->|1. Lookup Active Datasets| B[Azure Data Factory]
    B -->|2. Parallel ForEach Loop| C[Databricks Generic Engine]
    
    subgraph Databricks Processing
        C --> D{Dynamic DQ Validation}
        D -->|Pass| E[(Silver Delta Tables<br/>Star Schema)]
        D -->|Fail| F[(Quarantine Isolation Table)]
    end

    C -->|3. Update Execution Logs & Watermarks| A
    E --> G[Power BI Dashboard with RLS]
```

---

## 🛠️ Tech Stack & Key Features

| Component | Technology | Purpose |
|---|---|---|
| **Control Database** | SQL Server (T-SQL) | Central metadata catalog (`PipelineControl`), execution audit logs (`PipelineExecutionLogs`), and stored procedures. |
| **Dynamic Orchestrator** | Azure Data Factory | Single parameterized master pipeline replacing dozens of individual pipelines via dynamic expressions. |
| **ETL & DQ Processor** | Databricks (PySpark) | Reusable engine reading dynamic schemas, evaluating rules (not null, range, format), and merging into Delta Lake. |
| **Error Handling** | Delta Lake Isolation | Automatic quarantine routing: corrupt records never break downstream models; tagged with failure reason. |
| **Reporting & Security** | Power BI | Executive supply chain dashboards with On-Time In-Full (OTIF) metrics and Row-Level Security (RLS). |

---

## 📂 Repository Structure

```
metadata-driven-etl-engine/
├── adf_orchestration/
│   └── dynamic_master_pipeline.json              # Parameterized ADF master pipeline template
├── data_generator/
│   └── generate_erp_data.py                      # Synthetic ERP generator with injected defect records
├── databricks_engine/
│   └── generic_etl_processor.py                  # Dynamic PySpark rule validator & Delta MERGE engine
├── metadata_db/
│   ├── 01_metadata_tables.sql                    # SQL Server control & audit tables DDL
│   └── 02_metadata_stored_procs.sql              # Stored procedures for ADF orchestration
├── powerbi/
│   └── POWERBI_SPECS.md                          # Data model, DAX measures & Row-Level Security setup
├── tests/
│   └── test_generic_engine.py                    # Unit tests for data quality assertion logic
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start Guide

### 1. Set Up Environment
```bash
git clone https://github.com/<your-username>/metadata-driven-etl-engine.git
cd metadata-driven-etl-engine
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate Sample ERP Data (with intentional defect records)
```bash
python3 data_generator/generate_erp_data.py
```

### 3. Review Database Metadata Controls
* Check `metadata_db/01_metadata_tables.sql` to view how rules like `{"rule": "range", "column": "TotalAmount", "min": 0}` are registered in JSON format.

### 4. Run Unit Tests
```bash
python3 -m unittest discover tests
```

---

## 💼 Resume Bullet Points

* *Architected a metadata-driven dynamic ingestion framework using Azure Data Factory, SQL Server, and Databricks, reducing onboarding time for new source tables from 3 days to under 15 minutes.*
* *Built a generic PySpark validation engine that dynamically parses JSON-defined quality assertions and routes invalid records to an isolated Delta Lake quarantine table.*
* *Implemented centralized audit logging and dynamic watermarking in SQL Server stored procedures, providing visibility across 20+ parallel data pipelines.*
* *Developed an executive Power BI reporting suite with Row-Level Security (RLS) restricting regional visibility, measuring On-Time In-Full (OTIF) fulfillment rates.*
