"""
Module 3: Data Preparation Script for ETL
File: scripts/data_prep.py
"""

import pathlib
import sys
import pandas as pd

# Add project root to sys.path for local imports
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import local modules
from utils.logger import logger
from scripts.data_scrubber import DataScrubber

# Constants
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
CLEAN_DATA_DIR = DATA_DIR / "clean"

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path = RAW_DATA_DIR / file_name
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def save_clean_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    clean_file_path = CLEAN_DATA_DIR / file_name
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(clean_file_path, index=False)
    logger.info(f"Saved cleaned data to {clean_file_path}.")

def process_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean customers data."""
    scrubber = DataScrubber(df)
    
    # Inspect initial data
    info_str, _ = scrubber.inspect_data()
    logger.info(f"Initial customers data info:\n{info_str}")
    
    # Clean the data
    scrubber.remove_duplicate_records()  # Remove exact duplicates
    scrubber.handle_missing_data(fill_value="Unknown")  # Fill missing values
    scrubber.format_column_strings_to_upper_and_trim("Name")  # Standardize Name
    scrubber.format_column_strings_to_upper_and_trim("Region")  # Standardize Region
    scrubber.format_column_strings_to_upper_and_trim("CustomerSegment")  # Standardize CustomerSegment
    scrubber.parse_dates_to_add_standard_datetime("JoinDate")  # Parse JoinDate and add StandardDateTime
    scrubber.convert_column_to_new_data_type("LoyaltyPoints", int)  # Ensure LoyaltyPoints is int
    
    # Final consistency check
    consistency = scrubber.check_data_consistency_after_cleaning()
    logger.info(f"Post-cleaning customers consistency: {consistency}")
    
    return scrubber.df

def process_products_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean products data."""
    scrubber = DataScrubber(df)
    
    # Inspect initial data
    info_str, _ = scrubber.inspect_data()
    logger.info(f"Initial products data info:\n{info_str}")
    
    # Clean the data
    scrubber.remove_duplicate_records()  # Remove exact duplicates
    scrubber.handle_missing_data(fill_value="Unknown")  # Fill missing values
    scrubber.format_column_strings_to_lower_and_trim("ProductName")  # Standardize ProductName
    scrubber.format_column_strings_to_lower_and_trim("Category")  # Standardize Category
    scrubber.format_column_strings_to_lower_and_trim("Subcategory")  # Standardize Subcategory
    scrubber.convert_column_to_new_data_type("UnitPrice", float)  # Ensure UnitPrice is float
    scrubber.convert_column_to_new_data_type("StockQuantity", int)  # Ensure StockQuantity is int
    
    # Final consistency check
    consistency = scrubber.check_data_consistency_after_cleaning()
    logger.info(f"Post-cleaning products consistency: {consistency}")
    
    return scrubber.df

def process_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean sales data."""
    scrubber = DataScrubber(df)
    
    # Inspect initial data
    info_str, _ = scrubber.inspect_data()
    logger.info(f"Initial sales data info:\n{info_str}")
    
    # Clean the data
    scrubber.remove_duplicate_records()  # Remove exact duplicates (e.g., TransactionID 612, 613)
    scrubber.handle_missing_data(fill_value=0)  # Fill missing numeric values
    scrubber.format_column_strings_to_upper_and_trim("PaymentType")  # Standardize PaymentType
    scrubber.parse_dates_to_add_standard_datetime("SaleDate")  # Parse SaleDate and add StandardDateTime
    scrubber.convert_column_to_new_data_type("SaleAmount", float)  # Ensure SaleAmount is float
    scrubber.convert_column_to_new_data_type("DiscountPercent", int)  # Ensure DiscountPercent is int
    scrubber.filter_column_outliers("SaleAmount", 0, 10000)  # Filter extreme SaleAmount values
    
    # Final consistency check
    consistency = scrubber.check_data_consistency_after_cleaning()
    logger.info(f"Post-cleaning sales consistency: {consistency}")
    
    return scrubber.df

def process_data(file_name: str, process_func, output_file_name: str) -> None:
    """Generic function to process and save data."""
    df = read_raw_data(file_name)
    if not df.empty:
        cleaned_df = process_func(df)
        save_clean_data(cleaned_df, output_file_name)

def main() -> None:
    """Main function for processing customer, product, and sales data."""
    logger.info("Starting data preparation...")
    
    process_data("customers_data.csv", process_customers_data, "clean_customers_data.csv")
    process_data("products_data.csv", process_products_data, "clean_products_data.csv")
    process_data("sales_data.csv", process_sales_data, "clean_sales_data.csv")
    
    logger.info("Data preparation complete.")

if __name__ == "__main__":
    main()