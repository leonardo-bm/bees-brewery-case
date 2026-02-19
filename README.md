# BEES Data Engineering Case - Brewery Pipeline

This repository contains a complete Data Engineering pipeline developed for the BEES / Ambev technical case. The project extracts data from the Open Brewery DB API, transforms it using PySpark, and orchestrates the entire workflow using Apache Airflow within a Dockerized environment, following the **Medallion Architecture** principles.

## 🏗️ Architecture & Tech Stack
* **Language:** Python 3.10
* **Data Processing:** Apache Spark (PySpark)
* **Orchestration:** Apache Airflow
* **Containerization:** Docker & Docker Compose
* **Data Quality:** Pytest

## 🏅 Medallion Architecture Workflow

1. **Bronze Layer (`scripts/bronze.py`)**
   * Extracts data from the [Open Brewery DB API](https://api.openbrewerydb.org/).
   * Handles pagination automatically to retrieve all available records.
   * Saves the raw data as a JSON file.

2. **Silver Layer (`scripts/silver.py`)**
   * Reads the raw JSON using PySpark.
   * Cleans the data and adds a metadata column (`_load_date`).
   * Saves the data in **Parquet** format.
   * *Note on Partitioning:* The project requirement asks for partitioning by `country` and `state`. In a production Cloud environment (AWS S3, ADLS, Databricks), this is done using `.partitionBy("country", "state")`. However, due to known I/O lock issues with Spark's `_temporary` directories when running locally on Docker Desktop (Windows/WSL2), the local execution saves a single optimized Parquet file to ensure pipeline stability. The production-ready partitioned code is documented in the script.

3. **Gold Layer (`scripts/gold.py`)**
   * Reads the clean Parquet data from the Silver layer.
   * Performs the required business aggregation: **Counts the number of breweries per type and location (country, state)**.
   * Saves the final aggregated view in Parquet format, ready for BI consumption.

## 🚀 How to Run the Project

### 1. Start the Environment
Ensure you have Docker and Docker Compose installed. Run the following command in the project root:

```bash
docker compose up -d
```

### 2. Trigger the Pipeline
* Open the Airflow UI in your browser at `http://localhost:8080`.
* The default login is usually `airflow` / `airflow` (if prompted).
* Find the `brewery_medallion_pipeline` DAG.
* Unpause the DAG (toggle the switch to blue) and click **Trigger DAG** (the play button).
* Watch the tasks execute sequentially (Bronze -> Silver -> Gold).

### 3. Check the Output
Once the DAG succeeds, the processed files will be available in the `data/` directory mapped to your local machine:
* `data/bronze/`: Raw JSON data.
* `data/silver/`: Cleaned Parquet data.
* `data/gold/`: Aggregated Parquet data (brewery count by type and location).

## 🧪 Data Quality Tests
To ensure the integrity of the Gold layer, run the automated data quality tests using `pytest`:

```bash
docker compose exec airflow pytest /opt/airflow/scripts/tests/test_data_quality.py -v
```

This validates that the final aggregation is not empty and contains no null values in the count metrics.