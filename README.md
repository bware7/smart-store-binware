# Smart Store Project - Bin Ware

A business intelligence project to analyze smart store sales data using Python

## Structure 

smart-store-binware/

├── data/
│   ├── clean/
│   ├── dw/
│   └── raw/  
├── reports/   
├── scripts/        
├── utils/
├── screenshots/          
├── .gitignore      
├── README.md       
└── requirements.txt 


## Setup

1. Clone: `git clone https://github.com/bware7/smart-store-binware.git`
2. Virtual Env: `python -m venv .venv` & `.venv\Scripts\activate` (Windows)
3. Install: `pip install -r requirements.txt`
4. Power BI (for P5):
   - Install Power BI Desktop: [https://powerbi.microsoft.com/downloads/](https://powerbi.microsoft.com/downloads/)
   - Install SQLite ODBC Driver: [http://www.ch-werner.de/sqliteodbc/](http://www.ch-werner.de/sqliteodbc/) (use `sqliteodbc_w64.exe` for 64-bit)
   - Configure ODBC DSN (`SmartSalesDSN`) to point to `data/dw/smart_sales.db` (or `.venv/data/dw/smart_sales.db`)

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

### P5: Cross-Platform Reporting with Power BI

#### Overview
Used Power BI to connect to `smart_sales.db`, query sales data, and create an interactive dashboard with visuals to analyze sales trends.

#### Implementation
- Connected Power BI to `smart_sales.db` via `SmartSalesDSN`.
- Queried total sales per customer.
- Created visuals: column chart (drilldown by date), matrix (category vs. region), bar chart (top customers), line chart (sales trends).
- Added slicers for date and category filtering.

#### SQL Query
```sql
SELECT c.name, SUM(s.sale_amount) AS total_spent
FROM sale s
JOIN customer c ON s.customer_id = c.customer_id
GROUP BY c.name
ORDER BY total_spent DESC;
```

#### Visuals
Column Chart: Sales by year → quarter → month.
Matrix: Sales by category (e.g., electronics) and region (e.g., East).
Bar Chart: Top customers (e.g., WILLIAM WHITE).
Line Chart: Monthly sales trends.
Slicers: Date range (e.g., Jan-Jul 2024), product category.

#### Notes
Ran python scripts/etl_to_dw.py to populate database.
Saved report as smart_sales_report.pbix.
Screenshots in screenshots/.

### P6: BI Insights and Storytelling

#### Business Goal
Analyzed which customer segments (PREMIUM, REGULAR, NEW) show the highest spending on product subcategories across different regions using OLAP techniques.

#### Data Source
Used cleaned CSV files containing sales transactions, customer data, and product information from the `data/clean/` directory.

#### Tools
Python with pandas for data manipulation, matplotlib/seaborn for visualization, implementing multidimensional OLAP analysis.

#### Workflow & Logic
Implemented three key OLAP operations:
- **Slicing**: Filtered data by customer segment and region
- **Dicing**: Broke down data by customer segment and product subcategory
- **Drill-down**: Explored from segment-subcategory to segment-subcategory-region detail

#### Key Results
- **NEW customers**: Strongest in SOUTH ($12,422) with computers (74.7%) and outerwear (12.0%)
- **PREMIUM customers**: Highest sales in EAST ($23,752) with heavy focus on computers (85.7%)
- **REGULAR customers**: Best performance in EAST ($31,573) and WEST ($13,619)

#### Business Actions
1. **NEW Customers**: Target South region with computer-outerwear bundles
2. **PREMIUM Customers**: Create East region VIP program, focus on computer accessories
3. **REGULAR Customers**: Optimize inventory in East/West regions, improve North engagement

#### Implementation
Python script `scripts/olap_segment_analysis.py` performs the analysis, saving results as CSVs and visualizations to `data/results/`.


Git Commands:
```bash
git pull origin main
git add .
git commit -m "brief message"
git push -u origin main
```