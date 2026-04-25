# 02_MODELING — Model Development and Evaluation

This folder contains the modeling work for the FlyOnTime project.

The objective is to train models able to answer two questions:

1. Is the flight likely to be delayed at arrival?
2. If yes, how many minutes of delay should be expected?

To answer these questions, we train two models:

- a classifier;
- a regressor.

---

## Folder structure

```text
02_MODELING/
├── notebook/
│   ├── randomforest_classifier.ipynb
│   ├── randomforest_regressor.ipynb
│   ├── xgboost_classifier.ipynb
│   └── xgboost_regressor.ipynb
├── output/
│   ├── csv/
│   └── html/
├── training_scripts/
│   ├── train_xgboost_classifier.py
│   └── train_xgboost_regressor.py
└── README.md
```

---

## Modeling strategy

The modeling phase was done in two steps.

### Step 1 — Random Forest MVP

We first developed Random Forest models as a Minimum Viable Product.

This MVP phase was useful because Random Forest models are:

- robust on tabular data;
- easy to train;
- simple to interpret as a first baseline;
- good enough to validate the end-to-end pipeline quickly.

The Random Forest notebooks helped us validate:

- data preprocessing;
- target definition;
- model training;
- MLflow logging;
- API model loading;
- first end-to-end prediction tests.

### Step 2 — XGBoost final direction

After validating the MVP, we moved to XGBoost.

The reason was not only model performance.  
XGBoost was also more suitable for the deployed user experience.

In the Streamlit + FastAPI workflow, inference time matters because the user is waiting for the result.  
XGBoost helped reduce the waiting time by around 20 seconds compared with the previous setup, while keeping stronger and more reliable metrics.

---

## Targets

### Classification target

```text
retard_arrivee
```

This is a binary target:

```text
0 = flight not delayed
1 = flight delayed
```

The business goal is to alert the passenger when the flight is likely to arrive late.

### Regression target

```text
arrival_delay_min
```

This is a continuous target expressed in minutes.

The business goal is to estimate the expected arrival delay duration.

---

## Recommended development order

The modeling work can be understood in two layers: notebooks for experimentation, then clean Python scripts for reproducible training.

### 1. Notebook exploration

Recommended notebook order:

```text
1. randomforest_classifier.ipynb
2. randomforest_regressor.ipynb
3. xgboost_classifier.ipynb
4. xgboost_regressor.ipynb
```

Why this order?

- Random Forest notebooks document the MVP phase.
- XGBoost notebooks document the final model improvement phase.
- The classifier should be understood before the regressor because the classifier answers the first business question: “Is there a risk?”
- The regressor answers the second business question: “How long is the delay?”

### 2. Clean training scripts

After the notebooks were validated, the XGBoost training logic was moved into clean Python scripts:

```text
training_scripts/
├── train_xgboost_classifier.py
└── train_xgboost_regressor.py
```

These scripts keep the useful notebook logic while removing exploratory cells, plots and manual debugging code. They are designed to make training easier to reproduce and easier to explain in a production-oriented workflow.

The execution order is important:

```bash
python training_scripts/train_xgboost_classifier.py
python training_scripts/train_xgboost_regressor.py
```

The classifier script must run first because it loads the official clean training CSV from S3, applies the feature engineering logic, and saves the local parquet file reused by the regressor.

---

## Training data loading flow

The final training scripts keep the same modeling logic as the notebooks, but the data loading step was made cleaner and more explicit.

### Classifier input

The XGBoost classifier training script loads the official cleaned historical dataset directly from S3:

```text
s3://ppml2026/datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv
```

This file is the starting point for the classifier training pipeline. The script then applies the same preparation logic used in the notebook:

- duplicate handling;
- cleaning and column selection;
- target creation for `retard_arrivee`;
- datetime processing;
- categorical encoding;
- missing value handling;
- train/test split;
- randomized hyperparameter search;
- MLflow logging and model registration.

At the end of the classifier script, the final processed dataframe is saved locally as:

```text
data/df_train_final.parquet
```

This parquet file is also useful for transparency because it shows the dataset after feature engineering and preparation.

### Regressor input

The XGBoost regressor does not reload the original CSV from S3.

Instead, it reads the local parquet produced by the classifier script:

```text
data/df_train_final.parquet
```

This mirrors the notebook workflow: once the shared dataset has been cleaned and feature-engineered, it can be reused by the regressor without repeating the full loading and preparation process.

This also keeps both models aligned on the same final feature base.

### Environment variables for S3 loading

The classifier script requires AWS credentials with read access to the S3 bucket:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=ppml2026
```

---

## How to run the clean XGBoost training scripts

From the modeling folder, run:

```bash
python training_scripts/train_xgboost_classifier.py
```

This will:

1. download the cleaned historical CSV from S3;
2. prepare the dataframe;
3. train the XGBoost classifier;
4. log the run to MLflow;
5. register the model;
6. save `data/df_train_final.parquet`.

Then run:

```bash
python training_scripts/train_xgboost_regressor.py
```

This will:

1. load `data/df_train_final.parquet`;
2. train the XGBoost regressor;
3. log the run to MLflow;
4. register the model.

To point the scripts to the remote MLflow server deployed on Hugging Face Spaces:

```bash
python training_scripts/train_xgboost_classifier.py   --tracking-uri https://ppml2026-ppml-mlflow.hf.space

python training_scripts/train_xgboost_regressor.py   --tracking-uri https://ppml2026-ppml-mlflow.hf.space
```

---

## Preprocessing logic

The modeling notebooks include several key preprocessing steps:

- removal of target columns from the feature matrix;
- datetime feature engineering;
- categorical encoding;
- missing value imputation;
- train/test split;
- stratification for classification;
- cross-validation;
- random search for hyperparameter optimization;
- MLflow tracking.

Important columns commonly removed before training:

```python
cols_a_virer = [
    "departure_delay_min",
    "arrival_delay_min",
    "time_dep",
    "time_arr"
]
```

For the classifier:

```python
X = df_train_final.drop(columns=cols_a_virer + ["retard_arrivee"], errors="ignore")
y = df_train_final["retard_arrivee"].astype(int)
```

For the regressor:

```python
X = df_train_final.drop(columns=cols_a_virer + ["retard_arrivee"], errors="ignore")
y = df_train_final["arrival_delay_min"]
```

---

## XGBoost Classifier

### Objective

The classifier predicts whether the flight will be delayed at arrival.

It acts as the first decision layer in the application:

```text
Input flight features → XGBoost Classifier → delay risk
```

### Main metric

The main metric is the **F1 score** because the dataset is not perfectly balanced and because we want to balance:

- precision: avoid too many false alarms;
- recall: detect as many real delays as possible.

### Reported performance

| Metric | Value |
|---|---:|
| Cross-validation F1 | ~0.86 |
| Test F1 | ~0.87 |
| Test Accuracy | ~0.92 |
| Test Precision | ~0.92 |
| Test Recall | ~0.83 |
| Test ROC AUC | ~0.96 |

Interpretation:

- The classifier is strong enough to detect delay risk.
- Precision is high, meaning alerts are not too noisy.
- Recall is acceptable, meaning the model catches a large share of delayed flights.
- ROC AUC is high, meaning the model separates delayed and non-delayed flights well.

---

## XGBoost Regressor

### Objective

The regressor estimates the expected arrival delay in minutes.

It is used after the classification model to provide a more actionable result:

```text
Input flight features → XGBoost Regressor → predicted delay in minutes
```

### Main metric

The main metric is **MAE**: Mean Absolute Error.

MAE is easy to explain:

```text
MAE = average prediction error in minutes
```

### Reported performance

| Metric | Value |
|---|---:|
| MAE | ~6.98 minutes |

Interpretation:

- On average, the model is wrong by about 7 minutes.
- This is understandable for users and useful for passenger-level decision making.
- The prediction should be seen as an estimate, not as an exact operational truth.

---

## MLflow integration

The modeling notebooks and scripts are designed to log experiments into MLflow.

MLflow is used to track:

- model parameters;
- training metrics;
- test metrics;
- artifacts;
- model versions;
- registered models.

The deployed FastAPI application loads the selected model version through the MLflow model registry.

Typical alias:

```text
challenger
```

This makes the deployment flexible: the backend can load the current selected model without hardcoding a local model file.

---

## Why XGBoost was selected

XGBoost was selected because it offered the best trade-off between:

- prediction quality;
- inference speed;
- reliability;
- deployment practicality;
- user waiting time.

This matters because the final application is not only evaluated in a notebook.  
It is used through Streamlit, where the user waits while the backend performs:

1. flight lookup,
2. ETL,
3. feature alignment,
4. model loading,
5. classification,
6. regression,
7. response formatting.

Reducing model computation time improves the final user experience.

---

## Outputs

The `output/` folder stores model-related exports, for example:

```text
output/csv/
output/html/
```

These outputs may include metrics, visualizations, explainability reports or exported analysis files.

---

## Next improvements

Possible improvements:

- add a single reproducible training command that runs classifier then regressor;
- save preprocessing artifacts explicitly;
- add model signatures and input examples in MLflow;
- add SHAP outputs for feature-level explanation;
- automate champion/challenger model promotion;
- add tests to check training/inference schema consistency.
