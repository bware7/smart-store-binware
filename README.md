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
  
Git Commands:
```bash
git pull origin main
git add .
git commit -m "brief message"
git push -u origin main
```