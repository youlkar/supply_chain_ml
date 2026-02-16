import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import json
import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

# -----------------------------
# MLflow (DAGsHub) config
# -----------------------------
ml_flow_uri = os.getenv("MLFLOW_TRACKING_URI", "https://dagshub.com/youl1/supplylens_ml.mlflow")
ml_flow_exp = os.getenv("MLFLOW_EXPERIMENT_NAME")
if not ml_flow_exp:
    raise RuntimeError("MLFLOW_EXPERIMENT_NAME must be set (pass via git secrets).")

# IMPORTANT: strip accidental newlines from secrets (prevents %0A in URLs)
ml_flow_uri = ml_flow_uri.strip()

mlflow.set_tracking_uri(ml_flow_uri)
mlflow.set_experiment(ml_flow_exp)

# -----------------------------
# Features / Target
# -----------------------------
# NOTE:
# - label_what / label_who / label_mitigation are *metadata* fields for analysis/UI.
# - The model target is triage_label (AUTO_APPROVE vs MANUAL_REVIEW).

NUMERIC_FEATURES: List[str] = [
    "po_qty",
    "po_price",
    "asn_qty",
    "inv_qty",
    "inv_price",
    "qty_delta",
    "qty_delta_pct",
    "price_diff_pct",
    "has_po_ref",
    "is_repeat",
    "supplier_exception_rate_30d",
    "days_order_to_ship",
    "days_ship_to_invoice",
    "days_order_to_invoice",
]

CATEGORICAL_FEATURES: List[str] = [
    "supplier_category",
    "supplier_risk_tier",
]

FEATURE_COLUMNS: List[str] = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL = "triage_label"


def _safe_pct_delta(delta: pd.Series, denom: pd.Series) -> pd.Series:
    d = denom.replace(0, np.nan)
    out = (delta / d).replace([np.inf, -np.inf], 0).fillna(0)
    return out


def load_data(data_path: str) -> pd.DataFrame:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"training data not found at {data_path}")
    df = pd.read_csv(data_path)

    # -----------------------------
    # Derived numeric features (always safe to compute)
    # -----------------------------
    if "qty_delta" not in df.columns and {"inv_qty", "po_qty"}.issubset(df.columns):
        df["qty_delta"] = df["inv_qty"] - df["po_qty"]

    if "qty_delta_pct" not in df.columns and {"qty_delta", "po_qty"}.issubset(df.columns):
        df["qty_delta_pct"] = _safe_pct_delta(df["qty_delta"], df["po_qty"])

    if "price_diff_pct" not in df.columns and {"inv_price", "po_price"}.issubset(df.columns):
        denom = df["po_price"].replace(0, np.nan)
        df["price_diff_pct"] = ((df["inv_price"] - df["po_price"]) / denom)
        df["price_diff_pct"] = df["price_diff_pct"].replace([np.inf, -np.inf], 0).fillna(0)

    # -----------------------------
    # Target handling
    # -----------------------------
    # Prefer triage_label. If missing but label_mitigation exists, derive it.
    if TARGET_COL not in df.columns:
        if "label_mitigation" in df.columns:
            # Normalize common strings from generator/expert notes
            lm = df["label_mitigation"].astype(str).str.strip().str.lower()
            df[TARGET_COL] = np.where(lm.isin(["auto-approve", "auto_approve", "auto approve", "autoapprove"]),
                                     "AUTO_APPROVE",
                                     "MANUAL_REVIEW")
        else:
            raise ValueError(
                f"Missing target column '{TARGET_COL}'. Provide '{TARGET_COL}' or 'label_mitigation' to derive it."
            )
    else:
        # normalize strings so we have stable classes
        t = df[TARGET_COL].astype(str).str.strip().str.upper()
        # allow a few variants
        t = t.replace({
            "AUTO-APPROVE": "AUTO_APPROVE",
            "AUTO APPROVE": "AUTO_APPROVE",
            "AUTOAPPROVE": "AUTO_APPROVE",
            "MANUAL-REVIEW": "MANUAL_REVIEW",
            "MANUAL REVIEW": "MANUAL_REVIEW",
            "REVIEW": "MANUAL_REVIEW",
        })
        df[TARGET_COL] = t

    # -----------------------------
    # Ensure required feature columns exist
    # -----------------------------
    # If your generator hasn't added supplier/timing fields yet, we fill sensible defaults
    # so the pipeline still runs, but we ALSO surface which columns were missing.
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]

    # defaults that keep behavior explicit and debuggable
    default_numeric = {
        "supplier_exception_rate_30d": 0.0,
        "days_order_to_ship": -1,
        "days_ship_to_invoice": -1,
        "days_order_to_invoice": -1,
    }
    default_categorical = {
        "supplier_category": "UNKNOWN",
        "supplier_risk_tier": "UNKNOWN",
    }

    for c in missing:
        if c in default_numeric:
            df[c] = default_numeric[c]
        elif c in default_categorical:
            df[c] = default_categorical[c]
        else:
            # For core numeric columns, it is safer to fail hard.
            raise ValueError(
                f"Missing required feature column '{c}'. Update the generator to include it, or add it to the CSV."
            )

    # Cast types
    for c in NUMERIC_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype(str)

    # Keep a note for training logs/debugging
    df.attrs["missing_feature_columns_filled"] = missing

    return df


def train_model(data_path: str, model_output_dir: str) -> dict:
    df = load_data(data_path)

    # Encode target
    le_triage = LabelEncoder()
    y_encoded = le_triage.fit_transform(df[TARGET_COL].astype(str))

    X = df[FEATURE_COLUMNS].copy()

    # Preprocessing: numeric impute + categorical one-hot
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]), NUMERIC_FEATURES),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("ohe", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
        ],
        remainder="drop",
    )

    # Split (stratify on the target)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )

    lgbm_params = {
        "n_estimators": 400,
        "learning_rate": 0.03,
        "num_leaves": 48,
        "class_weight": "balanced",
        "min_data_in_leaf": 120,
        "random_state": 42,
        "verbose": -1,
    }

    clf = lgb.LGBMClassifier(**lgbm_params)

    # Full pipeline
    model = Pipeline(steps=[("preprocess", preprocessor), ("clf", clf)])

    run_name = f"lgbm_triage_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metrics: Dict[str, float] = {}

    run_id = None
    model_path = None

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        missing_filled = df.attrs.get("missing_feature_columns_filled", [])

        mlflow.log_params(lgbm_params)
        mlflow.log_params(
            {
                "data_path": data_path,
                "features_count": len(FEATURE_COLUMNS),
                "numeric_features": len(NUMERIC_FEATURES),
                "categorical_features": len(CATEGORICAL_FEATURES),
                "train_size": len(X_train),
                "test_size": len(X_test),
                "missing_features_filled": ",".join(missing_filled) if missing_filled else "",
                "target_col": TARGET_COL,
                "target_classes": ",".join(list(le_triage.classes_)),
            }
        )

        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        macro_f1 = f1_score(y_test, y_pred, average="macro")
        metrics["TRIAGE_accuracy"] = float(acc)
        metrics["TRIAGE_macro_f1"] = float(macro_f1)

        mlflow.log_metric("TRIAGE_accuracy", float(acc))
        mlflow.log_metric("TRIAGE_macro_f1", float(macro_f1))

        pr, rc, f1s, _ = precision_recall_fscore_support(
            y_test, y_pred, labels=range(len(le_triage.classes_)), zero_division=0
        )
        for idx, cls_name in enumerate(le_triage.classes_):
            clean_name = str(cls_name).replace(" ", "_").replace("&", "and").replace("/", "_")
            mlflow.log_metric(f"test_f1__TRIAGE__{clean_name}", float(f1s[idx]))
            metrics[f"test_f1__TRIAGE__{clean_name}"] = float(f1s[idx])

        # -----------------------------
        # Save model bundle locally
        # -----------------------------
        model_dir = Path(model_output_dir)
        model_dir.mkdir(parents=True, exist_ok=True)

        model_path = model_dir / f"supply_chain_triage_model_{run_name}.joblib"
        joblib.dump(
            {
                "model": model,
                "triage_encoder": le_triage,
                "features": FEATURE_COLUMNS,
                "numeric_features": NUMERIC_FEATURES,
                "categorical_features": CATEGORICAL_FEATURES,
                "target_col": TARGET_COL,
            },
            model_path,
        )
        mlflow.log_artifact(str(model_path), artifact_path="model_artifacts")

        meta = {
            "features": FEATURE_COLUMNS,
            "numeric_features": NUMERIC_FEATURES,
            "categorical_features": CATEGORICAL_FEATURES,
            "target": TARGET_COL,
            "classes": list(le_triage.classes_),
        }
        meta_path = model_dir / f"model_meta_{run_name}.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        mlflow.log_artifact(str(meta_path), artifact_path="model_artifacts")

    artifact_path = f"model_artifacts/{model_path.name}" if model_path else None
    trained_at = (
        datetime.utcfromtimestamp(run.info.start_time / 1000).isoformat() + "Z"
        if run.info.start_time
        else datetime.utcnow().isoformat() + "Z"
    )

    result = {
        "run_name": run_name,
        "mlflow_run_id": run_id,
        "model_path": str(model_path) if model_path else None,
        "artifact_path": artifact_path,
        "features": FEATURE_COLUMNS,
        "target": TARGET_COL,
        "metrics": metrics,
        "timestamp": trained_at,
    }

    token = os.getenv("MODEL_META_PATH", "latest_model.json")
    meta_json = Path(model_output_dir) / token
    with open(meta_json, "w") as f:
        json.dump(result, f, indent=2)

    return result
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import json
import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

# -----------------------------
# MLflow (DAGsHub) config
# -----------------------------
ml_flow_uri = os.getenv("MLFLOW_TRACKING_URI", "https://dagshub.com/youl1/supplylens_ml.mlflow")
ml_flow_exp = os.getenv("MLFLOW_EXPERIMENT_NAME")
if not ml_flow_exp:
    raise RuntimeError("MLFLOW_EXPERIMENT_NAME must be set (pass via git secrets).")

# IMPORTANT: strip accidental newlines from secrets (prevents %0A in URLs)
ml_flow_uri = ml_flow_uri.strip()

mlflow.set_tracking_uri(ml_flow_uri)
mlflow.set_experiment(ml_flow_exp)

# -----------------------------
# Features / Target
# -----------------------------
# NOTE:
# - label_what / label_who / label_mitigation are *metadata* fields for analysis/UI.
# - The model target is triage_label (AUTO_APPROVE vs MANUAL_REVIEW).

NUMERIC_FEATURES: List[str] = [
    "po_qty",
    "po_price",
    "asn_qty",
    "inv_qty",
    "inv_price",
    "qty_delta",
    "qty_delta_pct",
    "price_diff_pct",
    "has_po_ref",
    "is_repeat",
    "supplier_exception_rate_30d",
    "days_order_to_ship",
    "days_ship_to_invoice",
    "days_order_to_invoice",
]

CATEGORICAL_FEATURES: List[str] = [
    "supplier_category",
    "supplier_risk_tier",
]

FEATURE_COLUMNS: List[str] = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COL = "triage_label"


def _safe_pct_delta(delta: pd.Series, denom: pd.Series) -> pd.Series:
    d = denom.replace(0, np.nan)
    out = (delta / d).replace([np.inf, -np.inf], 0).fillna(0)
    return out


def load_data(data_path: str) -> pd.DataFrame:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"training data not found at {data_path}")
    df = pd.read_csv(data_path)

    # -----------------------------
    # Derived numeric features (always safe to compute)
    # -----------------------------
    if "qty_delta" not in df.columns and {"inv_qty", "po_qty"}.issubset(df.columns):
        df["qty_delta"] = df["inv_qty"] - df["po_qty"]

    if "qty_delta_pct" not in df.columns and {"qty_delta", "po_qty"}.issubset(df.columns):
        df["qty_delta_pct"] = _safe_pct_delta(df["qty_delta"], df["po_qty"])

    if "price_diff_pct" not in df.columns and {"inv_price", "po_price"}.issubset(df.columns):
        denom = df["po_price"].replace(0, np.nan)
        df["price_diff_pct"] = ((df["inv_price"] - df["po_price"]) / denom)
        df["price_diff_pct"] = df["price_diff_pct"].replace([np.inf, -np.inf], 0).fillna(0)

    # -----------------------------
    # Target handling
    # -----------------------------
    # Prefer triage_label. If missing but label_mitigation exists, derive it.
    if TARGET_COL not in df.columns:
        if "label_mitigation" in df.columns:
            # Normalize common strings from generator/expert notes
            lm = df["label_mitigation"].astype(str).str.strip().str.lower()
            df[TARGET_COL] = np.where(lm.isin(["auto-approve", "auto_approve", "auto approve", "autoapprove"]),
                                     "AUTO_APPROVE",
                                     "MANUAL_REVIEW")
        else:
            raise ValueError(
                f"Missing target column '{TARGET_COL}'. Provide '{TARGET_COL}' or 'label_mitigation' to derive it."
            )
    else:
        # normalize strings so we have stable classes
        t = df[TARGET_COL].astype(str).str.strip().str.upper()
        # allow a few variants
        t = t.replace({
            "AUTO-APPROVE": "AUTO_APPROVE",
            "AUTO APPROVE": "AUTO_APPROVE",
            "AUTOAPPROVE": "AUTO_APPROVE",
            "MANUAL-REVIEW": "MANUAL_REVIEW",
            "MANUAL REVIEW": "MANUAL_REVIEW",
            "REVIEW": "MANUAL_REVIEW",
        })
        df[TARGET_COL] = t

    # -----------------------------
    # Ensure required feature columns exist
    # -----------------------------
    # If your generator hasn't added supplier/timing fields yet, we fill sensible defaults
    # so the pipeline still runs, but we ALSO surface which columns were missing.
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]

    # defaults that keep behavior explicit and debuggable
    default_numeric = {
        "supplier_exception_rate_30d": 0.0,
        "days_order_to_ship": -1,
        "days_ship_to_invoice": -1,
        "days_order_to_invoice": -1,
    }
    default_categorical = {
        "supplier_category": "UNKNOWN",
        "supplier_risk_tier": "UNKNOWN",
    }

    for c in missing:
        if c in default_numeric:
            df[c] = default_numeric[c]
        elif c in default_categorical:
            df[c] = default_categorical[c]
        else:
            # For core numeric columns, it is safer to fail hard.
            raise ValueError(
                f"Missing required feature column '{c}'. Update the generator to include it, or add it to the CSV."
            )

    # Cast types
    for c in NUMERIC_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype(str)

    # Keep a note for training logs/debugging
    df.attrs["missing_feature_columns_filled"] = missing

    return df


def train_model(data_path: str, model_output_dir: str) -> dict:
    df = load_data(data_path)

    # Encode target
    le_y = LabelEncoder()
    y = le_y.fit_transform(df[TARGET_COL].astype(str))

    X = df[FEATURE_COLUMNS].copy()

    # Preprocessing: numeric impute + categorical one-hot
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]), NUMERIC_FEATURES),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("ohe", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                CATEGORICAL_FEATURES,
            ),
        ],
        remainder="drop",
    )

    # Split (stratify on the target)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    lgbm_params = {
        "n_estimators": 400,
        "learning_rate": 0.03,
        "num_leaves": 48,
        "class_weight": "balanced",
        "min_data_in_leaf": 120,
        "random_state": 42,
        "verbose": -1,
    }

    clf = lgb.LGBMClassifier(**lgbm_params)

    # Full pipeline
    model = Pipeline(steps=[("preprocess", preprocessor), ("clf", clf)])

    run_name = f"lgbm_triage_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metrics: Dict[str, float] = {}

    run_id = None
    model_path = None

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        missing_filled = df.attrs.get("missing_feature_columns_filled", [])

        mlflow.log_params(lgbm_params)
        mlflow.log_params(
            {
                "data_path": data_path,
                "features_count": len(FEATURE_COLUMNS),
                "numeric_features": len(NUMERIC_FEATURES),
                "categorical_features": len(CATEGORICAL_FEATURES),
                "train_size": len(X_train),
                "test_size": len(X_test),
                "missing_features_filled": ",".join(missing_filled) if missing_filled else "",
                "target_col": TARGET_COL,
                "target_classes": ",".join(list(le_y.classes_)),
            }
        )

        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        macro_f1 = f1_score(y_test, y_pred, average="macro")
        metrics["TRIAGE_accuracy"] = float(acc)
        metrics["TRIAGE_macro_f1"] = float(macro_f1)

        mlflow.log_metric("TRIAGE_accuracy", float(acc))
        mlflow.log_metric("TRIAGE_macro_f1", float(macro_f1))

        pr, rc, f1s, _ = precision_recall_fscore_support(
            y_test, y_pred, labels=range(len(le_y.classes_)), zero_division=0
        )
        for idx, cls_name in enumerate(le_y.classes_):
            clean_name = str(cls_name).replace(" ", "_").replace("&", "and").replace("/", "_")
            mlflow.log_metric(f"test_f1__TRIAGE__{clean_name}", float(f1s[idx]))
            metrics[f"test_f1__TRIAGE__{clean_name}"] = float(f1s[idx])

        # -----------------------------
        # Save model bundle locally
        # -----------------------------
        model_dir = Path(model_output_dir)
        model_dir.mkdir(parents=True, exist_ok=True)

        model_path = model_dir / f"supply_chain_triage_model_{run_name}.joblib"
        joblib.dump(
            {
                "model": model,
                "target_encoder": le_y,
                "features": FEATURE_COLUMNS,
                "numeric_features": NUMERIC_FEATURES,
                "categorical_features": CATEGORICAL_FEATURES,
                "target_col": TARGET_COL,
            },
            model_path,
        )
        mlflow.log_artifact(str(model_path), artifact_path="model_artifacts")

        meta = {
            "features": FEATURE_COLUMNS,
            "numeric_features": NUMERIC_FEATURES,
            "categorical_features": CATEGORICAL_FEATURES,
            "target": TARGET_COL,
            "classes": list(le_y.classes_),
        }
        meta_path = model_dir / f"model_meta_{run_name}.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        mlflow.log_artifact(str(meta_path), artifact_path="model_artifacts")

    artifact_path = f"model_artifacts/{model_path.name}" if model_path else None
    trained_at = (
        datetime.utcfromtimestamp(run.info.start_time / 1000).isoformat() + "Z"
        if run.info.start_time
        else datetime.utcnow().isoformat() + "Z"
    )

    result = {
        "run_name": run_name,
        "mlflow_run_id": run_id,
        "model_path": str(model_path) if model_path else None,
        "artifact_path": artifact_path,
        "features": FEATURE_COLUMNS,
        "target": TARGET_COL,
        "metrics": metrics,
        "timestamp": trained_at,
    }

    token = os.getenv("MODEL_META_PATH", "latest_model.json")
    meta_json = Path(model_output_dir) / token
    with open(meta_json, "w") as f:
        json.dump(result, f, indent=2)

    return result