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
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split, GroupShuffleSplit, GroupKFold, RandomizedSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

# -----------------------------
# MLflow (DAGsHub) config
# -----------------------------
ml_flow_uri = os.getenv("MLFLOW_TRACKING_URI", "https://dagshub.com/youl1/supplylens_ml.mlflow")
ml_flow_exp = os.getenv("MLFLOW_EXPERIMENT_NAME", "supply_chain_lightgbm_three_way")
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

GROUP_KEY_COL = "po_number"  # group by PO so we evaluate on unseen POs

# Optional engineered features (if present in CSV). Safe: we only include columns that exist.
OPTIONAL_ENGINEERED_FEATURES: List[str] = [
    "inv_qty_over_po_qty",
    "inv_price_over_po_price",
    "abs_qty_delta",
    "abs_qty_delta_pct",
    "abs_price_diff_pct",
    "qty_delta_pct_cap",
    "price_diff_pct_cap",
    "log_abs_qty_delta",
    "log_abs_qty_delta_pct",
    "log_abs_price_diff_pct",
    "joint_anomaly_score",
    "risk_exception_score",
    "high_chronic_exception",
    "timing_outlier_long",
    "timing_outlier_short",
]


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
    # Optional engineered features (compute if derivable)
    # -----------------------------
    if {"inv_qty", "po_qty"}.issubset(df.columns):
        denom = df["po_qty"].replace(0, np.nan)
        if "inv_qty_over_po_qty" not in df.columns:
            df["inv_qty_over_po_qty"] = (df["inv_qty"] / denom).replace([np.inf, -np.inf], 0).fillna(0)

    if {"inv_price", "po_price"}.issubset(df.columns):
        denom_p = df["po_price"].replace(0, np.nan)
        if "inv_price_over_po_price" not in df.columns:
            df["inv_price_over_po_price"] = (df["inv_price"] / denom_p).replace([np.inf, -np.inf], 0).fillna(0)

    if "qty_delta" in df.columns and "abs_qty_delta" not in df.columns:
        df["abs_qty_delta"] = df["qty_delta"].abs()

    if "qty_delta_pct" in df.columns:
        if "abs_qty_delta_pct" not in df.columns:
            df["abs_qty_delta_pct"] = df["qty_delta_pct"].abs()
        if "qty_delta_pct_cap" not in df.columns:
            df["qty_delta_pct_cap"] = df["qty_delta_pct"].clip(-0.30, 0.30)

    if "price_diff_pct" in df.columns:
        if "abs_price_diff_pct" not in df.columns:
            df["abs_price_diff_pct"] = df["price_diff_pct"].abs()
        if "price_diff_pct_cap" not in df.columns:
            df["price_diff_pct_cap"] = df["price_diff_pct"].clip(-0.30, 0.30)

    # log-scaled magnitudes (avoid log(0))
    if "abs_qty_delta" in df.columns and "log_abs_qty_delta" not in df.columns:
        df["log_abs_qty_delta"] = np.log1p(df["abs_qty_delta"].clip(lower=0))
    if "abs_qty_delta_pct" in df.columns and "log_abs_qty_delta_pct" not in df.columns:
        df["log_abs_qty_delta_pct"] = np.log1p(df["abs_qty_delta_pct"].clip(lower=0))
    if "abs_price_diff_pct" in df.columns and "log_abs_price_diff_pct" not in df.columns:
        df["log_abs_price_diff_pct"] = np.log1p(df["abs_price_diff_pct"].clip(lower=0))

    # simple interactions if inputs exist
    if {"abs_qty_delta_pct", "abs_price_diff_pct"}.issubset(df.columns) and "joint_anomaly_score" not in df.columns:
        df["joint_anomaly_score"] = (df["abs_qty_delta_pct"] * df["abs_price_diff_pct"]).clip(lower=0)

    if {"supplier_exception_rate_30d", "supplier_risk_tier"}.issubset(df.columns) and "risk_exception_score" not in df.columns:
        tier_w = df["supplier_risk_tier"].astype(str).str.upper().map({"LOW": 0.8, "MEDIUM": 1.0, "HIGH": 1.2}).fillna(1.0)
        df["risk_exception_score"] = (tier_w * pd.to_numeric(df["supplier_exception_rate_30d"], errors="coerce").fillna(0)).clip(0, 2.0)

    if "supplier_exception_rate_30d" in df.columns and "high_chronic_exception" not in df.columns:
        df["high_chronic_exception"] = (pd.to_numeric(df["supplier_exception_rate_30d"], errors="coerce").fillna(0) > 0.30).astype(int)

    if "days_order_to_invoice" in df.columns:
        doi = pd.to_numeric(df["days_order_to_invoice"], errors="coerce").fillna(-1)
        if "timing_outlier_long" not in df.columns:
            df["timing_outlier_long"] = (doi >= 35).astype(int)
        if "timing_outlier_short" not in df.columns:
            df["timing_outlier_short"] = ((doi >= 0) & (doi <= 2)).astype(int)

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

    # If optional engineered columns exist, include them as numeric features.
    present_opt = [c for c in OPTIONAL_ENGINEERED_FEATURES if c in df.columns]
    if present_opt:
        # extend numeric + feature columns (module-level lists)
        for c in present_opt:
            if c not in NUMERIC_FEATURES:
                NUMERIC_FEATURES.append(c)
        # refresh global feature columns
        global FEATURE_COLUMNS
        FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES

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

    # Group-aware split if group key exists, else stratified
    group_key_present = GROUP_KEY_COL in df.columns
    if group_key_present:
        gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        groups = df[GROUP_KEY_COL]
        train_idx, test_idx = next(gss.split(X, y_encoded, groups=groups))
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y_encoded[train_idx], y_encoded[test_idx]
        groups_train = groups.iloc[train_idx]
        groups_test = groups.iloc[test_idx]
        cv_type = "groupkfold"
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
        )
        groups_train = None
        cv_type = "kfold"

    # Param search
    base_params = {
        "objective": "binary",
        "n_estimators": 450,
        "learning_rate": 0.04,
        "random_state": 42,
        "verbose": -1,
    }
    param_distributions = {
        "clf__num_leaves": [31, 48, 64],
        "clf__max_depth": [-1, 5, 7, 9],
        "clf__min_data_in_leaf": [60, 120, 200],
        "clf__feature_fraction": [0.7, 0.8, 0.9],
        "clf__bagging_fraction": [0.7, 0.8, 0.9],
        "clf__bagging_freq": [1, 3, 5],
        "clf__lambda_l1": [0.0, 0.5, 1.0],
        "clf__lambda_l2": [0.0, 1.0, 3.0],
        "clf__min_gain_to_split": [0.0, 0.05, 0.1],
        "clf__class_weight": [None, "balanced"],
    }

    clf = lgb.LGBMClassifier(**base_params)
    model = Pipeline(steps=[("preprocess", preprocessor), ("clf", clf)])

    # CV object
    if group_key_present:
        cv = GroupKFold(n_splits=5)
    else:
        cv = 5

    run_name = f"lgbm_triage_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    metrics: Dict[str, float] = {}

    run_id = None
    model_path = None

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        missing_filled = df.attrs.get("missing_feature_columns_filled", [])

        # Log base params and meta
        mlflow.log_params(base_params)
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
                "cv_type": cv_type,
                "group_key_present": int(group_key_present),
            }
        )

        # RandomizedSearchCV (with group-aware CV if possible)
        search = RandomizedSearchCV(
            estimator=model,
            param_distributions=param_distributions,
            n_iter=20,
            scoring="f1_macro",
            cv=cv,
            random_state=42,
            n_jobs=-1,
            verbose=0,
        )

        if group_key_present:
            search.fit(X_train, y_train, groups=groups_train)
        else:
            search.fit(X_train, y_train)
        best_model = search.best_estimator_

        # Log best params and CV macro F1
        mlflow.log_params({f"search__{k}": v for k, v in search.best_params_.items()})
        mlflow.log_metric("best_cv_macro_f1", float(search.best_score_))

        # Predict proba on test set
        proba = best_model.predict_proba(X_test)
        # Find positive class index for "MANUAL_REVIEW"
        try:
            pos_label = list(le_triage.classes_).index("MANUAL_REVIEW")
        except Exception:
            pos_label = 1  # fallback
        proba_pos = proba[:, pos_label]

        # Sweep thresholds to maximize positive-class F1
        thresholds = np.arange(0.05, 0.95, 0.01)
        best_f1 = -1
        best_threshold = 0.5
        for t in thresholds:
            y_pred_thresh = (proba_pos >= t).astype(int)
            # Map back to full class labels for F1 (since label order may differ)
            y_pred_full = np.full_like(y_test, fill_value=0)
            y_pred_full[y_pred_thresh == 1] = pos_label
            y_pred_full[y_pred_thresh == 0] = 1 - pos_label if len(le_triage.classes_) == 2 else 0
            f1 = f1_score(y_test, y_pred_full, average=None, labels=[pos_label])[0]
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = t
        # Final prediction at best threshold
        y_pred_thresh = (proba_pos >= best_threshold).astype(int)
        y_pred_final = np.full_like(y_test, fill_value=0)
        y_pred_final[y_pred_thresh == 1] = pos_label
        y_pred_final[y_pred_thresh == 0] = 1 - pos_label if len(le_triage.classes_) == 2 else 0

        # Cost-aware metric: FN = predict AUTO_APPROVE when true is MANUAL_REVIEW
        # FP = predict MANUAL_REVIEW when true is AUTO_APPROVE (less costly)
        # y_test and y_pred_final are integer-encoded; pos_label = MANUAL_REVIEW
        y_true = y_test
        y_pred = y_pred_final
        fn = int(np.sum((y_true == pos_label) & (y_pred != pos_label)))
        fp = int(np.sum((y_true != pos_label) & (y_pred == pos_label)))
        cost_fn5_fp1 = 5 * fn + 1 * fp

        # Compute metrics
        acc = accuracy_score(y_true, y_pred)
        macro_f1 = f1_score(y_true, y_pred, average="macro")
        metrics["TRIAGE_accuracy"] = float(acc)
        metrics["TRIAGE_macro_f1"] = float(macro_f1)
        metrics["best_pr_f1"] = float(best_f1)
        metrics["cost_fn5_fp1"] = float(cost_fn5_fp1)

        mlflow.log_metric("TRIAGE_accuracy", float(acc))
        mlflow.log_metric("TRIAGE_macro_f1", float(macro_f1))
        mlflow.log_metric("best_pr_f1", float(best_f1))
        mlflow.log_metric("cost_fn5_fp1", float(cost_fn5_fp1))
        mlflow.log_param("best_threshold", float(best_threshold))

        # Per-class F1s
        pr, rc, f1s, _ = precision_recall_fscore_support(
            y_true, y_pred, labels=range(len(le_triage.classes_)), zero_division=0
        )
        for idx, cls_name in enumerate(le_triage.classes_):
            clean_name = str(cls_name).replace(" ", "_").replace("&", "and").replace("/", "_")
            mlflow.log_metric(f"test_f1__TRIAGE__{clean_name}", float(f1s[idx]))
            metrics[f"test_f1__TRIAGE__{clean_name}"] = float(f1s[idx])

        # Log confusion matrix and classification report as text
        cm = confusion_matrix(y_true, y_pred)
        cr = classification_report(y_true, y_pred, target_names=[str(x) for x in le_triage.classes_])
        # Save to file for artifact
        model_dir = Path(model_output_dir)
        model_dir.mkdir(parents=True, exist_ok=True)
        cm_path = model_dir / f"confusion_matrix_{run_name}.txt"
        with open(cm_path, "w") as f:
            f.write("Confusion Matrix:\n")
            f.write(np.array2string(cm))
            f.write("\n\nClassification Report:\n")
            f.write(cr)
        mlflow.log_artifact(str(cm_path), artifact_path="model_artifacts")

        # -----------------------------
        # Save model bundle locally
        # -----------------------------
        model_path = model_dir / f"supply_chain_triage_model_{run_name}.joblib"
        joblib.dump(
            {
                "model": best_model,
                "triage_encoder": le_triage,
                "features": FEATURE_COLUMNS,
                "numeric_features": NUMERIC_FEATURES,
                "categorical_features": CATEGORICAL_FEATURES,
                "target_col": TARGET_COL,
                "decision_threshold": float(best_threshold),
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