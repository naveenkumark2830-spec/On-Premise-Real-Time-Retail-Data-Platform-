from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "OLIST_Raw"


def load_csv(file_name: str):
    """
    Generic CSV loader.
    """

    file_path = RAW_DATA_DIR / file_name

    df = pd.read_csv(file_path)

    print("=" * 60)
    print(f"{file_name} Loaded Successfully")
    print("=" * 60)
    print(f"Rows    : {len(df)}")
    print(f"Columns : {len(df.columns)}")

    return df


if __name__ == "__main__":

    customers = load_csv("olist_customers_dataset.csv")