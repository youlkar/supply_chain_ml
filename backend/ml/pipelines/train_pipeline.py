from ml.train_lightgbm_basic import train_model
import os

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

