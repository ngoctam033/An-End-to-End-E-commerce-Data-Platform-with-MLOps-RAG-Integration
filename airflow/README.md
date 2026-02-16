# Apache Airflow Orchestration

This module manages the **Ingestion**, **Transformation**, and **Data Mart** layers of the E-commerce Data Platform. It orchestrates the flow of data from the PostgreSQL operational database to the Data Lakehouse (Iceberg/MinIO) using Spark jobs.

## 🏗 Architecture

The airflow pipeline is divided into three main stages:

1.  **Ingestion (Bronze/Raw Layer)**:
    -   Extracts data from PostgreSQL.
    -   Loads data into **Iceberg Raw** tables in MinIO.
    -   Supports both **Full Load** (snapshot) and **Incremental Load** (daily delta based on `created_at` or `updated_at`).

2.  **Transformation (Silver Layer)**:
    -   Reads from Iceberg Raw tables.
    -   Cleans, casts types, and standardizes data.
    -   Writes to **Iceberg Silver** tables.
    -   Handles schema evolution and partitioning.

3.  **Data Marts (Gold Layer)**:
    -   Aggregates Silver data into business-ready subject areas.
    -   Metrics: Sales, Customer Behavior, Inventory Performance.
    -   Optimized for analytical queries (Star/Snowflake Schema).

## 📂 Directory Structure

```text
airflow/
├── dags/
│   ├── 01_ingestion/       # DAGs for extracting data to Raw Layer
│   ├── 02_transformation/  # DAGs for processing Raw to Silver Layer
│   ├── 03_data_marts/      # DAGs for building Gold Layer aggregates
│   ├── common/             # Shared DAG components (Registry)
│   ├── scripts/            # Spark scripts (PySpark) executed by DAGs
│   ├── utils/              # Helper utilities (Path management, Schema)
│   └── master_dag.py       # Main orchestrator DAG
├── config/                 # Airflow configuration files
└── Dockerfile              # Custom Airflow image with Spark/Java dependencies
```

## 🔑 Key Components

### 1. DAG Registry (`common/dag_registry.py`)
Centralized management of all DAG IDs. This allows the `master_dag` to dynamically discover and trigger all child DAGs without hardcoding.
-   **Usage**: Register new DAG IDs here to include them in the master orchestration.

### 2. Path Manager (`utils/path_node.py`)
Defines the "Single Source of Truth" for all Iceberg table paths.
-   **Structure**: `iceberg.<layer>.<table>` (e.g., `iceberg.raw.orders`)
-   **Usage**: Spark scripts import `path_manager` to resolve S3 paths, ensuring consistency across environments.

### 3. Spark Scripts (`scripts/`)
-   `ingest_table_to_iceberg.py`: Generic script for EL (Extract-Load). Handles JDBC connection and Iceberg writes.
-   `transform_table.py`: Generic script for ETL. Uses a **Transformer Registry** to apply table-specific logic (e.g., masking PII, calculating totals).

## 📊 Data Marts (Gold Layer)

The Gold Layer is designed for business intelligence. Key Marts include:

| Data Mart | Description | Key Metrics |
| :--- | :--- | :--- |
| **Sales Mart** | Daily sales performance | GMV, AOV, Orders Count, Net Revenue |
| **Customer 360** | Customer behavior profile | LTV, Retention Rate, Churn Risk, Segmentation |
| **Inventory** | Stock health & movement | Turnover Rate, Out-of-Stock Incidents, Days on Hand |
| **Logistics** | Delivery performance | Delivery Time, Return Rate, Shipping Cost Efficiency |

## 🚀 Developer Guide: Adding a New Table

To add a new table (e.g., `new_table`) to the pipeline:

### 1. Update Path Manager
Edit `airflow/dags/utils/path_node.py`:
```python
TABLES = [..., "new_table"]
```

### 2. Create Ingestion DAG
Create `airflow/dags/01_ingestion/ingest_new_table.py`.
-   Copy an existing pattern (e.g., `ingest_orders.py`).
-   Update `DAG_ID` and `SQL_QUERY` (Full vs Incremental).

### 3. Create Transformation DAG
Create `airflow/dags/02_transformation/transform_new_table.py`.
-   Copy an existing pattern.
-   If custom logic is needed, add a Transformer class in `airflow/dags/scripts/transform_table.py`.

### 4. Register DAGs
Edit `airflow/dags/common/dag_registry.py`:
```python
self.dag_ids.extend([
    'ingest_new_table_to_minio',
    'transform_new_table_iceberg'
])
```

### 5. Deploy
Airflow will automatically pick up the new files. Use the Airflow UI to unpause and trigger the DAGs.
