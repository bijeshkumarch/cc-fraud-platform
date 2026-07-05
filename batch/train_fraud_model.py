import json
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta
import platform


MIN_FRAUD_SAMPLES = 50


BASE_DIR = os.environ.get("PROJECT_BASE_PATH")
if not BASE_DIR:
    BASE_DIR = "/opt/project"


# -------------------------
# Rolling 30-day window
# -------------------------
end_date = datetime.now().date()
start_date = end_date - timedelta(days=30)


train_date = datetime.now().strftime("%Y%m%d")
model_path = f"{BASE_DIR}/model/fraud_model_{train_date}.pkl"

# -------------------------
# Load data
# -------------------------
transactions_df = pd.read_parquet(
    f"{BASE_DIR}/data/raw/transactions_pca",
    filters=[
        ("event_date", ">=", start_date),
        ("event_date", "<=", end_date)
        ]
    )

labels_df = pd.read_csv(f"{BASE_DIR}/data/source/labels.csv") # assuming we are getting confirmed labels from human/external system

train_df = transactions_df.merge(
    labels_df,
    on="transaction_id",
    how="inner"
)

fraud_count = train_df["label"].sum()
total_rows = len(train_df)


print(f"Training rows: {total_rows}, Fraud rows: {fraud_count}")

if fraud_count < MIN_FRAUD_SAMPLES:
    raise ValueError(
        f"Insufficient fraud samples in training window: {fraud_count}"
    )

if train_df["label"].nunique() < 2:
    raise ValueError(
        "Training data has only one class — aborting retrain"
    )



# -------------------------
# Flatten PCA features

# -------------------------
features_df = pd.json_normalize(train_df["features"])
features_df["amount"] = train_df["amount"]
features_df["label"] = train_df["label"]

X = features_df.drop(columns=["label"])
y = train_df["label"]


# -------------------------
# Train / test split
# -------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# -------------------------
# Handle class imbalance
# -------------------------
model = LogisticRegression(
    max_iter=1000,
    class_weight="balanced"
)


# -------------------------
# Train model
# -------------------------
model.fit(X_train, y_train)

# -------------------------
# Evaluate
# -------------------------
y_pred_prob = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_prob)

print("AUC:", auc)
print(classification_report(y_test, model.predict(X_test)))

# -------------------------
# Save model
# -------------------------
joblib.dump(model, model_path)

# -------------------------
# Save metadata
# -------------------------
metadata = {
    "model_type": "LogisticRegression",
    "features": list(X.columns),
    "auc": auc,
    "training_rows": len(X_train),
    "fraud_ratio": float(y.mean())
}

with open(f"{BASE_DIR}/model/model_metadata_{train_date}.json", "w") as f:
    json.dump(metadata, f, indent=2)

print(f"Model saved at {model_path}")