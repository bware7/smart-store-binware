import pathlib
import sys
import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from utils.logger import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Saved to {file_path}")

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    df = df.drop_duplicates(subset=['CustomerID'], keep='first')
    logger.info(f"Removed {initial_count - len(df)} duplicates")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df['LoyaltyPoints'] = df['LoyaltyPoints'].fillna(df['LoyaltyPoints'].median())
    df['CustomerSegment'] = df['CustomerSegment'].fillna('Unknown')
    logger.info(f"Handled missing values: {df.isna().sum().sum()} remaining")
    return df

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    df = df[(df['LoyaltyPoints'] >= 0) & (df['LoyaltyPoints'] <= 2000)]
    logger.info(f"Removed {initial_count - len(df)} outliers in LoyaltyPoints")
    return df

def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    df['CustomerSegment'] = df['CustomerSegment'].str.title().str.strip()
    df['JoinDate'] = pd.to_datetime(df['JoinDate'], errors='coerce')
    logger.info("Standardized formats")
    return df

def main() -> None:
    logger.info("Starting prepare_customers_data.py")
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    df = read_raw_data("customers_data.csv")
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = remove_outliers(df)
    df = standardize_formats(df)
    
    save_prepared_data(df, "customers_data_prepared.csv")
    logger.info("Finished prepare_customers_data.py")

if __name__ == "__main__":
    main()