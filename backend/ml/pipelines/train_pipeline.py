import importlib.util
from pathlib import Path
import os

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "training"
    / "train_lightgbm_basic.py"
)
spec = importlib.util.spec_from_file_location("train_lightgbm_basic", MODULE_PATH)
train_lightgbm_basic = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(train_lightgbm_basic)
train_model = train_lightgbm_basic.train_model

def main():
    data_path = os.environ.get(
        "TRAINING_DATA_PATH", "../data_gen/supply_chain_ml_data.csv"
    )
    model_output_dir = os.environ.get("MODEL_OUTPUT_DIR", "../artifacts")
    result = train_model(data_path, model_output_dir)
    print(f"Training complete. Run {result['run_name']} logged to MLflow.")
    print(f"Model artifact saved to {result['model_path']}")


if __name__ == "__main__":
    main()

