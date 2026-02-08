import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
import joblib

# 1. Load Data
df = pd.read_csv("../data_gen/supply_chain_ml_data.csv")

# 2. Preprocessing
for col in ['label_what', 'label_who', 'label_mitigation']:
    df[col] = df[col].astype(str)

le_what, le_who, le_mit = LabelEncoder(), LabelEncoder(), LabelEncoder()
df['target_what'] = le_what.fit_transform(df['label_what'])
df['target_who'] = le_who.fit_transform(df['label_who'])
df['target_mit'] = le_mit.fit_transform(df['label_mitigation'])

features = ['po_qty', 'po_price', 'asn_qty', 'inv_qty', 'inv_price', 
            'has_po_ref', 'is_repeat', 'qty_delta', 'price_diff_pct']

X, y = df[features], df[['target_what', 'target_who', 'target_mit']]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y['target_what'])

# 3. Optimized Model Configuration
# We use custom weights to avoid the 'Match Collapse'
custom_weights = {
    le_what.transform(['MATCH'])[0]: 1,
    le_what.transform(['QTY_SHORT'])[0]: 2,
    le_what.transform(['PRICE_ERROR'])[0]: 2,
    le_what.transform(['TAX_MISMATCH'])[0]: 3,
    le_what.transform(['UOM_DISCREPANCY'])[0]: 3,
    le_what.transform(['DUPLICATE_INVOICE'])[0]: 2,
    le_what.transform(['UNAUTHORIZED_SHIP'])[0]: 2,
}

lgbm_clf = lgb.LGBMClassifier(
    n_estimators=300, 
    learning_rate=0.02, 
    num_leaves=40,
    class_weight=custom_weights, # Strategic weighting
    min_data_in_leaf=100,         # Prevents learning single noisy rows
    verbose=-1
)

model = MultiOutputClassifier(lgbm_clf, n_jobs=-1)

print("Training Smoothed Supply Chain Model...")
model.fit(X_train, y_train)

# 4. Evaluation
predictions = model.predict(X_test)
print("\n--- Realistic Performance Report ---")
for i, name in enumerate(['WHAT', 'WHO', 'MITIGATION']):
    enc = [le_what, le_who, le_mit][i]
    print(f"\nTarget: {name}\n", classification_report(y_test.iloc[:, i], predictions[:, i], 
                                                        target_names=enc.classes_, zero_division=0))

# 5. Save bundle
joblib.dump({'model': model, 'encoders': {'what': le_what, 'who': le_who, 'mit': le_mit}, 'features': features}, 'supply_chain_model_final.pkl')