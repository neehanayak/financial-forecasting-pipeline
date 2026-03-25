# Financial Data Lakehouse & Forecasting Pipeline (GCP)

<p align="center">

<img src="https://img.shields.io/badge/Cloud-GCP-blue?style=for-the-badge&logo=googlecloud"/>
<img src="https://img.shields.io/badge/Compute-Spark-orange?style=for-the-badge&logo=apachespark"/>
<img src="https://img.shields.io/badge/Storage-Iceberg-blue?style=for-the-badge"/>
<img src="https://img.shields.io/badge/ML-Prophet-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/Python-3.10+-yellow?style=for-the-badge&logo=python"/>

</p>



## Project Overview
This project builds an **end-to-end data pipeline on Google Cloud** to forecast asset prices using a **lakehouse architecture**.

It demonstrates how to:
- Ingest data from **Google Cloud Storage (GCS)**
- Process data using **Dataproc Serverless (Spark)**
- Store data in an **Apache Iceberg table (GCS-backed)**
- Perform **time-series forecasting using Prophet**
- Evaluate models using **MAE, RMSE, and MAPE**

---

## What This Project Does
GCS CSV → Spark (Serverless) → Data Cleaning → Iceberg Table → Forecasting → Evaluation

### Key Features
- Serverless data processing (no cluster management)
- Iceberg-based lakehouse storage
- Modular, production-style Python code
- Train/test split for realistic modeling
- Model comparison (Prophet vs Naive baseline)
- Evaluation metrics (MAE, RMSE, MAPE)

---

## Tech Stack

- **Cloud:** Google Cloud Platform (GCP)
- **Compute:** Dataproc Serverless (Spark)
- **Storage:** GCS + Apache Iceberg
- **ML Model:** Prophet
- **Language:** Python

---

## How to Run

### 1. Clone the repo
```bash
git clone <your-repo-url>
cd financial-data_lakehouse_forecasting-pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
PROJECT_ID=your-project-id
REGION=us-east1
SERVICE_ACCOUNT=your-service-account

CSV_FILE_PATH=gs://dataproc-metastore-public-binaries/asset_price_forecast/asset_price_forecast.csv

ICEBERG_CATALOG=biglake
ICEBERG_SCHEMA=finance
ICEBERG_TABLE=asset_prices
ICEBERG_WAREHOUSE=gs://your-bucket/iceberg/
```

### 5. Authenticate with GCP
```bash
gcloud auth application-default login
```

### 6. Run the pipeline
``` bash
python src/pipeline.py
```
---

## Sample Output
- Cleaned data written to Iceberg
- Forecast generated using Prophet
### Model Performance Comparison

| Model            | MAE       | RMSE      | MAPE (%) |
|------------------|----------|----------|----------|
| Prophet          | 438.97   | 498.40   | 15.01    |
| Naive Baseline   | 460.11   | 575.54   | 15.29    |

- **MAE (Mean Absolute Error):** Average absolute difference between predictions and actual values.
- **RMSE (Root Mean Squared Error):** Penalizes larger errors more heavily.
- **MAPE (Mean Absolute Percentage Error):** Error expressed as a percentage.

- **Prophet** outperforms the naive baseline across all metrics.
- Lower MAE, RMSE, and MAPE indicate better forecasting accuracy.

---

## Design Decisions
### Why Iceberg?
Enables scalable, versioned data storage with schema evolution on GCS.

### Why Prophet?
Simple and effective for time-series forecasting with seasonality.

### Why Batch (not streaming)?
Historical financial data is processed periodically, not in real-time.

---

## Project Structure
```bash
financial-data_lakehouse_forecasting-pipeline/
│
├── src/
│   ├── data_loader.py
│   ├── preprocess.py
│   ├── model.py
│   ├── evaluate.py
│   └── pipeline.py
│
├── notebook/
│   └── your_notebook.ipynb
│
├── README.md
├── requirements.txt
├── .gitignore
└── .env.example

```
---
### Future Improvements
- Add real-time ingestion (Pub/Sub)
- Hyperparameter tuning for Prophet
- Store forecasts back into Iceberg
- Add visualization dashboard (Streamlit)
---
### Key Takeaway

This project showcases a real-world data engineering + ML pipeline using modern cloud and lakehouse tools, with a focus on scalability, modularity, and model evaluation.
