import pandas as pd
import sqlite3
import pathlib
import sys

# Add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = PROJECT_ROOT / "data" / "dw"
DB_PATH = DW_DIR / "smart_sales.db"
PREPARED_DATA_DIR = PROJECT_ROOT / "data" / "clean"

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            customer_segment TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            sale_amount REAL,
            sale_date TEXT,
            payment_type TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
    """)

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the tables."""
    cursor.execute("DELETE FROM sale")
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    customers_df[['CustomerID', 'Name', 'Region', 'StandardDateTime', 'CustomerSegment']] \
        .rename(columns={'CustomerID': 'customer_id', 'Name': 'name', 'Region': 'region', 
                         'StandardDateTime': 'join_date', 'CustomerSegment': 'customer_segment'}) \
        .to_sql("customer", cursor.connection, if_exists="append", index=False)

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    products_df[['ProductID', 'ProductName', 'Category']] \
        .rename(columns={'ProductID': 'product_id', 'ProductName': 'product_name', 'Category': 'category'}) \
        .to_sql("product", cursor.connection, if_exists="append", index=False)

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    sales_df[['TransactionID', 'CustomerID', 'ProductID', 'SaleAmount', 'StandardDateTime', 'PaymentType']] \
        .rename(columns={'TransactionID': 'sale_id', 'CustomerID': 'customer_id', 'ProductID': 'product_id', 
                         'SaleAmount': 'sale_amount', 'StandardDateTime': 'sale_date', 'PaymentType': 'payment_type'}) \
        .to_sql("sale", cursor.connection, if_exists="append", index=False)

def load_data_to_db() -> None:
    try:
        # Ensure DW directory exists
        DW_DIR.mkdir(parents=True, exist_ok=True)
        
        # Connect to SQLite
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data (updated to match your filenames)
        customers_df = pd.read_csv(PREPARED_DATA_DIR / "clean_customers_data.csv")
        products_df = pd.read_csv(PREPARED_DATA_DIR / "clean_products_data.csv")
        sales_df = pd.read_csv(PREPARED_DATA_DIR / "clean_sales_data.csv")

        # Insert data
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()