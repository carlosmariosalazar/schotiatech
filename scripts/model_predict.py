from pathlib import Path
import pandas as pd
import joblib
from sklearn import set_config
set_config(transform_output= "pandas")

# Paths and directories
root_dir = Path()
data_path = root_dir / "data" / "test_data.csv"
pipeline_path = root_dir / "models" / "pipeline.pkl"

# Read data
df = pd.read_csv(data_path, index_col= 0, parse_dates= ["period"])

# Load pipeline
pipeline = joblib.load(pipeline_path)

# Make predictions
preds = pipeline.predict(df)

# Join predictions
df["status"] = preds
df["department_code"] = [41,41,19,47,25]

if __name__ == "__main__":
    print(df.head())
    
