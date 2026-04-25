# 01_ETL — Data Extraction, Transformation and Loading

This folder contains the ETL part of the FlyOnTime project.

The ETL layer prepares the datasets used by both:

- the offline training pipeline;
- the online single-flight inference pipeline.

The objective is to transform raw flight and contextual data into clean, structured, model-ready datasets.

---

## Role in the full project

The ETL layer sits before modeling and before real-time inference.

```text
External APIs / S3 data
        ↓
Extraction
        ↓
Transformation
        ↓
Loading to S3
        ↓
Model training or FastAPI inference
```

---

## Folder structure

```text
01_ETL/
├── 01_Extraction.py       # Reads the relevant source data
├── 02_Transformation.py   # Cleans, repairs and transforms the dataset
└── 03_Load.py             # Uploads the transformed parquet to S3
```

---

## Main data sources

The project uses several sources to build a rich flight dataset:

- flight information from AeroDataBox;
- weather data;
- airport traffic / congestion indicators;
- French holidays and public calendar information;
- contextual data used for feature enrichment.

The ETL process is designed for several use cases:

| Use case | Description |
|---|---|
| Historical dataset | Used to train and evaluate the models |
| Future dataset | Used to prepare upcoming flight predictions |
| Single-flight dataset | Used when a user asks for one flight prediction in Streamlit |

---

## 01_Extraction.py

### Purpose

`01_Extraction.py` is responsible for retrieving the data needed by the pipeline.

In the deployment-oriented version, extraction can read the single-flight parquet produced by the flight lookup pipeline from S3.

The logic expects a request-specific path such as:

```text
raw/YYYY-MM-DD/requete_FLIGHTNUMBER_TIMESTAMP/
```

Example:

```text
raw/2026-04-21/requete_AF1234_20260421_183500/
```

The extraction layer searches for a parquet file named like:

```text
SignoffFlightsDataset_Single_<request_id>.parquet
```

### Main responsibilities

- connect to S3 using AWS credentials;
- locate the parquet file for one specific request;
- load the parquet into a pandas dataframe;
- validate minimal required columns;
- normalize the flight number;
- run basic data quality checks.

### Minimal expected columns

The extracted dataset should include at least:

```text
flight_number
flight_date
airport_origin
airport_destination
```

These columns are required because they are used later to identify the flight and align the row with the prediction pipeline.

---

## 02_Transformation.py

### Purpose

`02_Transformation.py` cleans and transforms the extracted dataset.

This step is very important because the online inference data must remain compatible with the training schema.

### Main responsibilities

- inspect missing values;
- detect empty strings and placeholder values;
- repair duplicated departure/arrival rows when needed;
- harmonize column names;
- keep useful model features;
- create date-based features;
- optionally encode categorical variables;
- save the final transformed parquet locally.

### Why repair duplicated rows?

Some flight data can appear as two complementary rows:

```text
Row 1: departure information
Row 2: arrival information
```

The model needs one observation per flight.  
The transformation layer can merge departure and arrival rows into one complete flight observation when the structure requires it.

### Feature engineering

The transformation layer extracts temporal features from datetime columns, for example:

```text
flight_date_month
flight_date_weekday
flight_date_hour
scheduled_departure_month
scheduled_departure_weekday
scheduled_departure_hour
scheduled_arrival_month
scheduled_arrival_weekday
scheduled_arrival_hour
```

These features make date and time information usable by tree-based machine learning models.

### Target creation

When `arrival_delay_min` is available, the transformation layer can create a binary target:

```text
retard arrivée = 1 if arrival_delay_min > 15 else 0
```

This target is used for classification.

---

## 03_Load.py

### Purpose

`03_Load.py` uploads the transformed local parquet to S3.

This separates:

- raw files;
- transformed files;
- inference-ready files.

### Output naming

The local transformed file follows this convention:

```text
single_flight_model_input_<request_id>.parquet
```

The S3 destination follows this convention:

```text
processed/YYYY-MM-DD/<request_id>/single_flight_model_input_<request_id>.parquet
```

Example:

```text
processed/2026-04-21/requete_AF1234_20260421_183500/single_flight_model_input_requete_AF1234_20260421_183500.parquet
```

---

## How to run

From the repository root:

```bash
cd src/01_ETL
```

Then run the scripts in order:

```bash
python 01_Extraction.py
python 02_Transformation.py
python 03_Load.py
```

In practice, some scripts are also called by the deployment pipeline, especially when FastAPI receives an on-demand prediction request.

---

## Required environment variables

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=ppml2026
S3_PREFIX=raw
TRANSFORMED_S3_PREFIX=processed
```

---

## ETL outputs

The ETL produces model-ready files such as:

```text
SignoffFlightsDataset_YYMMDD_HHMMSS_CLEAN.csv
SignoffFlightsDataset_future_YYMMDD_HHMMSS_CLEAN.csv
SignoffFlightsDataset_Single_YYMMDD_HHMMSS_CLEAN.csv
single_flight_model_input_<request_id>.parquet
```

The final parquet format is preferred for deployment because it is faster and preserves types better than CSV.

---

## Error handling

For on-demand requests, the pipeline produces logs and status files such as:

```text
flight_request_status.json
API_Single_ERR.log
```

These files allow the backend to distinguish between:

- flight not found;
- time mismatch;
- API temporary unavailability;
- technical error;
- codeshare warning.

This improves the final user experience because Streamlit can display a clean, human-readable message instead of a raw technical error.

---

## Why this ETL design matters

This ETL was not designed only for exploration.  
It supports a production-like workflow:

- every request gets a unique request ID;
- raw and processed files are stored with explicit paths;
- S3 acts as a data lake;
- transformed files are reusable by FastAPI;
- logs are preserved for debugging.

This makes the data pipeline traceable, reproducible and easier to operate.
