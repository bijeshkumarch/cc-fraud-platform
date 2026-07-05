import json
import platform
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.model_selection import train_test_split

print("started training script")

BASE_DIR = os.environ.get("PROJECT_BASE_PATH")
if not BASE_DIR:
    BASE_DIR = "/opt/project"

# -------------------------
# Load data
# -------------------------
df = pd.read_csv(f"{BASE_DIR}/data/source/creditcard.csv")


# -------------------------
# Flatten PCA features

# -------------------------

X = df.drop(columns=["Class", "Time"])
y = df["Class"]

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
joblib.dump(model, f"{BASE_DIR}/model/fraud_model.pkl")

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

with open(f"{BASE_DIR}/model/model_metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)

print("Model saved successfully")
