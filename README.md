# Smart Store Project - Bin Ware

A business intelligence project to analyze smart store sales data using Python

## Structure 

smart-store-binware/

├── data/raw/       
├── scripts/        
├── utils/          
├── .gitignore      
├── README.md       
└── requirements.txt 


## Setup

1. Clone: `git clone https://github.com/bware7/smart-store-binware.git`
2. Virtual Env: `python -m venv .venv` & `.venv\Scripts\activate` (Windows)
3. Install: `pip install -r requirements.txt`

## Workflow

## P2: BI Python w/External Packages

### Overview
Initialized a professional Python project with external packages, focusing on Git integration and virtual environments. Added utility and script files to process raw data, executed the initial script, and versioned changes on GitHub.

### Notes
- Verified project folder in VS Code with `README.md`, `.gitignore`, `requirements.txt`, and `data/raw/`.
- Created `utils/logger.py` and `scripts/data_prep.py` from starter files (https://github.com/denisecase/smart-sales-starter-files/).
- Ran initial script: `py scripts\data_prep.py`

## P3 Data Preparation

### Overview
In P3, raw sales data was cleaned and standardized using a reusable DataScrubber class in Python with pandas, verified by unit tests, to prepare it for the ETL process into a central data warehouse

### Notes
- Ran tests: `py tests\test_data_scrubber.py`
- Ran data prep: `py scripts\data_prep.py`
- All 13 tests passed, confirming `DataScrubber` functionality.
- Processed `customers_data.csv`, `products_data.csv`, `sales_data.csv`:
  - Customers: Standardized strings to uppercase, parsed `JoinDate`, no duplicates found.
  - Products: Standardized strings to lowercase, ensured numeric types, no duplicates.
  - Sales: Standardized `PaymentType`, parsed `SaleDate`, filtered `SaleAmount` (0–10000), removed duplicate TransactionID 612/613.
- Cleaned files saved to `data/clean/`.

## P4: Create and Populate Data Warehouse

### Overview
Designed and implemented a star schema data warehouse in SQLite to centralize cleaned sales data for efficient querying and business intelligence. Created an ETL script to populate the DW and validated the structure.

### Design Choices
- **Schema**: Star schema with one fact table (`sale`) and two dimension tables (`customer`, `product`).
- **Fact Table (sale)**: Tracks transactions (`sale_id`, `customer_id`, `product_id`, `sale_amount`, `sale_date`, `payment_type`).
- **Dimension Tables**:
  - `customer`: `customer_id`, `name`, `region`, `join_date`, `customer_segment`.
  - `product`: `product_id`, `product_name`, `category`.
- **Reasoning**: Simplifies queries for sales analysis (e.g., by region, segment, or category); supports scalability and business insights like maximizing profits or enhancing customer experience.

### Notes
- Created `scripts/etl_to_dw.py` to define the schema, load data from `data/clean/` (`clean_customers_data.csv`, `clean_products_data.csv`, `clean_sales_data.csv`), and populate `data/dw/smart_sales.db`.
- Ran script: `python scripts/etl_to_dw.py`.
- Validated tables in SQLite Viewer.

Git Commands:
```bash
git pull origin main
git add .
git commit -m "brief message"
git push -u origin main
```