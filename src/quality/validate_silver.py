
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def validate_silver():

    files = list(SILVER_DIR.glob("*.parquet"))

    print(f"Found {len(files)} Silver datasets")
    print("=" * 55)

    for file in files:
        df = pd.read_parquet(file)

        print(f"Dataset: {file.name}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"Readable: True")
        print("-" * 55)

    print("Silver validation completed.")


if __name__ == "__main__":
    validate_silver()
