import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support
import joblib
import mlflow
import mlflow.sklearn
import os
from datetime import datetime
from pathlib import Path

ml_flow_uri = os.getenv(
    "MLFLOW_TRACKING_URI", "https://dagshub.com/youl1/supplylens_ml.mlflow"
)
ml_flow_exp = os.getenv("MLFLOW_EXPERIMENT_NAME")
if not ml_flow_exp:
    raise RuntimeError("MLFLOW_EXPERIMENT_NAME must be set (pass via git secrets).")

mlflow.set_tracking_uri(ml_flow_uri)
mlflow.set_experiment(ml_flow_exp)

FEATURE_COLUMNS = [
    "po_qty",
    "po_price",
    "asn_qty",
    "inv_qty",
    "inv_price",
    "has_po_ref",
    "is_repeat",
    "qty_delta",
    "price_diff_pct",
]

def load_data(data_path: str) -> pd.DataFrame:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"training data not found at {data_path}")
    df = pd.read_csv(data_path)
    if "qty_delta" not in df.columns:
        df["qty_delta"] = df["inv_qty"] - df["po_qty"]
    if "price_diff_pct" not in df.columns:
        df["price_diff_pct"] = (df["inv_price"] - df["po_price"]) / df["po_price"]
        df["price_diff_pct"] = df["price_diff_pct"].replace([np.inf, -np.inf], 0).fillna(0)
    return df

def train_model(data_path: str, model_output_dir: str) -> dict:
    df = load_data(data_path)
    target_cols = ["label_what", "label_who", "label_mitigation"]
    for col in target_cols:
        df[col] = df[col].astype(str)

    le_what, le_who, le_mit = LabelEncoder(), LabelEncoder(), LabelEncoder()
    df["target_what"] = le_what.fit_transform(df["label_what"])
    df["target_who"] = le_who.fit_transform(df["label_who"])
    df["target_mit"] = le_mit.fit_transform(df["label_mitigation"])

    X = df[FEATURE_COLUMNS]
    y = df[["target_what", "target_who", "target_mit"]]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y["target_what"]
    )

    lgbm_params = {
        "n_estimators": 300,
        "learning_rate": 0.02,
        "num_leaves": 40,
        "class_weight": "balanced",
        "min_data_in_leaf": 100,
        "random_state": 42,
        "verbose": -1,
    }

    run_name = f"lgbm_multioutput_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(lgbm_params)
        mlflow.log_params(
            {
                "data_path": data_path,
                "features_count": len(FEATURE_COLUMNS),
                "train_size": len(X_train),
                "test_size": len(X_test),
            }
        )

        lgbm_clf = lgb.LGBMClassifier(**lgbm_params)
        multi_target_model = MultiOutputClassifier(lgbm_clf, n_jobs=-1)
        multi_target_model.fit(X_train, y_train)

        predictions = multi_target_model.predict(X_test)
        encoders = [le_what, le_who, le_mit]
        for i, name in enumerate(["WHAT", "WHO", "MITIGATION"]):
            y_true = y_test.iloc[:, i]
            y_pred = predictions[:, i]
            acc = accuracy_score(y_true, y_pred)
            macro_f1 = f1_score(y_true, y_pred, average="macro")

            mlflow.log_metric(f"{name}_accuracy", float(acc))
            mlflow.log_metric(f"{name}_macro_f1", float(macro_f1))

            pr, rc, f1s, _ = precision_recall_fscore_support(
                y_true, y_pred, labels=range(len(encoders[i].classes_)), zero_division=0
            )

            for idx, cls_name in enumerate(encoders[i].classes_):
                clean_name = str(cls_name).replace(" ", "_").replace("&", "and").replace("/", "_")
                mlflow.log_metric(f"test_f1__{name}__{clean_name}", float(f1s[idx]))

        model_dir = Path(model_output_dir)
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / "supply_chain_model_final.pkl"
        joblib.dump(
            {
                "model": multi_target_model,
                "encoders": {"what": le_what, "who": le_who, "mit": le_mit},
                "features": FEATURE_COLUMNS,
            },
            model_path,
        )

        mlflow.sklearn.log_model(multi_target_model, "model")
        mlflow.log_artifact(str(model_path))

    return {
        "run_name": run_name,
        "mlflow_run_id": mlflow.active_run().info.run_id,
        "model_path": str(model_path),
        "features": FEATURE_COLUMNS,
    }

