"""
P6: BI Insights and Storytelling - OLAP Analysis Implementation
Goal: Identify which customer segments show the highest spending on specific 
product subcategories, and how this varies by region

Author: Bin Ware
Date: 4/18/2025
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib
from datetime import datetime

# Constants - File Paths
DATA_DIR = pathlib.Path("data")
CLEAN_DATA_DIR = DATA_DIR.joinpath("clean")
RESULTS_OUTPUT_DIR = DATA_DIR.joinpath("results")

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_data_from_csv():
    """Load the necessary data from CSV files in the clean directory."""
    try:
        # Define file paths
        customers_file = CLEAN_DATA_DIR.joinpath("clean_customers_data.csv")
        products_file = CLEAN_DATA_DIR.joinpath("clean_products_data.csv")
        sales_file = CLEAN_DATA_DIR.joinpath("clean_sales_data.csv")
        
        # Load customer data
        customer_df = pd.read_csv(customers_file)
        print(f"Customer data loaded from {customers_file} with {len(customer_df)} records")
        
        # Load product data
        product_df = pd.read_csv(products_file)
        print(f"Product data loaded from {products_file} with {len(product_df)} records")
        
        # Load sales data
        sales_df = pd.read_csv(sales_file)
        print(f"Sales data loaded from {sales_file} with {len(sales_df)} records")
        
        # Rename columns to standardized format
        customer_df = customer_df.rename(columns={
            'CustomerID': 'customer_id',
            'Name': 'name',
            'Region': 'region',
            'JoinDate': 'join_date',
            'CustomerSegment': 'customer_segment'
        })
        
        product_df = product_df.rename(columns={
            'ProductID': 'product_id',
            'ProductName': 'product_name',
            'Category': 'category',
            'UnitPrice': 'unit_price_usd',
            'Subcategory': 'subcategory'
        })
        
        sales_df = sales_df.rename(columns={
            'TransactionID': 'sale_id',
            'CustomerID': 'customer_id',
            'ProductID': 'product_id',
            'SaleDate': 'sale_date',
            'SaleAmount': 'sale_amount_usd',
            'PaymentType': 'payment_type'
        })
        
        return sales_df, customer_df, product_df
        
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the required files: {e}")
        # Fall back to sample data (removed for brevity)
        print("Please ensure your CSV files exist in the data/clean directory.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    except Exception as e:
        print(f"Unexpected error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def create_fact_dimension_model(sales_df, customer_df, product_df):
    """Create an integrated dataset by joining fact and dimension tables."""
    try:
        # Check if we have data to work with
        if sales_df.empty or customer_df.empty or product_df.empty:
            print("Warning: One or more input dataframes are empty.")
            return pd.DataFrame()
            
        # Convert sale_date to datetime
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
        
        # Add time dimensions
        sales_df['month'] = sales_df['sale_date'].dt.month
        sales_df['quarter'] = sales_df['sale_date'].dt.quarter
        sales_df['year'] = sales_df['sale_date'].dt.year
        sales_df['day_of_week'] = sales_df['sale_date'].dt.day_name()
        
        # Join with customer dimension
        enriched_df = pd.merge(
            sales_df,
            customer_df[['customer_id', 'region', 'customer_segment']],
            on='customer_id',
            how='left'
        )
        
        # Join with product dimension
        enriched_df = pd.merge(
            enriched_df,
            product_df[['product_id', 'category', 'subcategory']],
            on='product_id',
            how='left'
        )
        
        # Convert columns to correct types
        if 'sale_amount_usd' in enriched_df.columns:
            enriched_df['sale_amount_usd'] = pd.to_numeric(enriched_df['sale_amount_usd'], errors='coerce')
        
        # Check if we have data to analyze
        if enriched_df.empty:
            print("Warning: No data available after joining tables.")
        else:
            print(f"Fact-dimension model created successfully with {len(enriched_df)} rows.")
            
        return enriched_df
        
    except Exception as e:
        print(f"Error creating fact-dimension model: {e}")
        return pd.DataFrame()

def analyze_segment_by_region(enriched_df):
    """SLICE: Analyze sales performance by customer segment and region."""
    try:
        # Check if we have data to analyze
        if enriched_df.empty:
            print("Warning: No data available for segment by region analysis.")
            return pd.DataFrame()
            
        # Slice: Aggregate by customer segment and region
        segment_region_performance = enriched_df.groupby(['customer_segment', 'region']).agg({
            'sale_amount_usd': 'sum',
            'sale_id': 'count',
            'customer_id': 'nunique'
        }).reset_index()
        
        # Rename aggregated columns
        segment_region_performance.rename(columns={
            'sale_amount_usd': 'total_sales',
            'sale_id': 'transaction_count',
            'customer_id': 'customer_count'
        }, inplace=True)
        
        # Calculate average order value
        segment_region_performance['avg_order_value'] = (
            segment_region_performance['total_sales'] / 
            segment_region_performance['transaction_count']
        )
        
        # Calculate sales per customer
        segment_region_performance['sales_per_customer'] = (
            segment_region_performance['total_sales'] / 
            segment_region_performance['customer_count']
        )
        
        # Sort by total sales
        segment_region_performance.sort_values(['customer_segment', 'total_sales'], 
                                              ascending=[True, False], inplace=True)
        
        print("Customer segment by region analysis completed.")
        return segment_region_performance
        
    except Exception as e:
        print(f"Error analyzing segment by region: {e}")
        return pd.DataFrame()

def analyze_subcategory_by_segment(enriched_df):
    """DICE: Analyze product subcategory performance by customer segment."""
    try:
        # Check if we have data to analyze
        if enriched_df.empty:
            print("Warning: No data available for subcategory by segment analysis.")
            return pd.DataFrame()
            
        # Dice: Breakdown by customer segment and product subcategory
        subcategory_segment_performance = enriched_df.groupby(['customer_segment', 'subcategory']).agg({
            'sale_amount_usd': 'sum',
            'sale_id': 'count',
            'customer_id': pd.Series.nunique
        }).reset_index()
        
        # Rename aggregated columns
        subcategory_segment_performance.rename(columns={
            'sale_amount_usd': 'total_sales',
            'sale_id': 'transaction_count',
            'customer_id': 'customer_count'
        }, inplace=True)
        
        # Calculate average purchase value by subcategory and segment
        subcategory_segment_performance['avg_purchase_value'] = (
            subcategory_segment_performance['total_sales'] / 
            subcategory_segment_performance['transaction_count']
        )
        
        # Calculate percentage of total sales for each segment
        segment_totals = subcategory_segment_performance.groupby('customer_segment')['total_sales'].sum().reset_index()
        segment_totals.rename(columns={'total_sales': 'segment_total_sales'}, inplace=True)
        
        # Merge the totals back
        subcategory_segment_performance = pd.merge(
            subcategory_segment_performance,
            segment_totals,
            on='customer_segment',
            how='left'
        )
        
        # Calculate percentage
        subcategory_segment_performance['pct_of_segment_sales'] = (
            subcategory_segment_performance['total_sales'] / 
            subcategory_segment_performance['segment_total_sales'] * 100
        )
        
        # Sort by customer segment and total sales
        subcategory_segment_performance.sort_values(
            ['customer_segment', 'total_sales'], 
            ascending=[True, False], 
            inplace=True
        )
        
        print("Subcategory by customer segment analysis completed.")
        return subcategory_segment_performance
        
    except Exception as e:
        print(f"Error analyzing subcategory by segment: {e}")
        return pd.DataFrame()

def analyze_subcategory_by_segment_and_region(enriched_df):
    """DRILL-DOWN: Analyze product subcategory by customer segment with region breakdown."""
    try:
        # Check if we have data to analyze
        if enriched_df.empty:
            print("Warning: No data available for drill-down analysis.")
            return pd.DataFrame()
            
        # Drill-down: From segment-subcategory to segment-subcategory-region
        segment_subcategory_region = enriched_df.groupby(
            ['customer_segment', 'subcategory', 'region']
        ).agg({
            'sale_amount_usd': 'sum',
            'sale_id': 'count',
            'customer_id': pd.Series.nunique
        }).reset_index()
        
        # Rename aggregated columns
        segment_subcategory_region.rename(columns={
            'sale_amount_usd': 'total_sales',
            'sale_id': 'transaction_count',
            'customer_id': 'customer_count'
        }, inplace=True)
        
        # Calculate metrics
        segment_subcategory_region['avg_purchase_value'] = (
            segment_subcategory_region['total_sales'] / 
            segment_subcategory_region['transaction_count']
        )
        
        # Sort by customer segment, subcategory, and total sales
        segment_subcategory_region.sort_values(
            ['customer_segment', 'subcategory', 'total_sales'], 
            ascending=[True, True, False], 
            inplace=True
        )
        
        print("Drill-down analysis completed.")
        return segment_subcategory_region
        
    except Exception as e:
        print(f"Error performing drill-down analysis: {e}")
        return pd.DataFrame()

def visualize_segment_by_region(segment_region_performance):
    """Visualize customer segment performance across regions."""
    try:
        # Check if we have data
        if segment_region_performance.empty:
            print("Warning: No data available for segment by region visualization.")
            return
            
        # Create a pivot table for the visualization
        pivot_data = segment_region_performance.pivot_table(
            index='customer_segment',
            columns='region',
            values='total_sales',
            aggfunc='sum',
            fill_value=0
        )
        
        # Check if pivot data is empty
        if pivot_data.empty:
            print("Warning: No data available after pivot for visualization.")
            return
            
        # Plot the stacked bar chart
        ax = pivot_data.plot(
            kind='bar',
            stacked=True,
            figsize=(12, 8),
            colormap='viridis'
        )
        
        # Add data labels to each segment
        for c in ax.containers:
            # Add labels
            labels = [f'${int(v):,}' if v > 0 else '' for v in c.datavalues]
            ax.bar_label(c, labels=labels, label_type='center')
        
        plt.title('Total Sales by Customer Segment and Region', fontsize=16)
        plt.xlabel('Customer Segment', fontsize=12)
        plt.ylabel('Total Sales (USD)', fontsize=12)
        plt.xticks(rotation=0)
        plt.legend(title='Region')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("segment_region_sales.png")
        plt.savefig(output_path)
        print(f"Customer segment by region visualization saved to {output_path}.")
        plt.close()
        
    except Exception as e:
        print(f"Error visualizing segment by region: {e}")

def visualize_subcategory_by_segment(subcategory_segment_data):
    """Visualize product subcategory performance across customer segments."""
    try:
        # Check if we have data
        if subcategory_segment_data.empty:
            print("Warning: No data available for subcategory by segment visualization.")
            return
            
        # Create a pivot table for the heatmap
        pivot_data = subcategory_segment_data.pivot_table(
            index='subcategory',
            columns='customer_segment',
            values='total_sales',
            aggfunc='sum',
            fill_value=0
        )
        
        # Check if pivot data is empty
        if pivot_data.empty:
            print("Warning: No data available after pivot for visualization.")
            return
            
        plt.figure(figsize=(12, 10))
        sns.heatmap(pivot_data, annot=True, fmt=",.0f", cmap="YlGnBu", linewidths=.5)
        plt.title('Sales by Product Subcategory and Customer Segment', fontsize=16)
        plt.ylabel('Product Subcategory', fontsize=12)
        plt.xlabel('Customer Segment', fontsize=12)
        plt.tight_layout()
        
        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("subcategory_segment_heatmap.png")
        plt.savefig(output_path)
        print(f"Subcategory by segment heatmap saved to {output_path}.")
        plt.close()
        
    except Exception as e:
        print(f"Error visualizing subcategory by segment: {e}")

def visualize_drilldown_analysis(segment_subcategory_region):
    """Visualize the drill-down analysis results."""
    try:
        # Check if we have data
        if segment_subcategory_region.empty:
            print("Warning: No data available for drill-down visualization.")
            return
            
        # Check if we have enough unique segments
        if len(segment_subcategory_region['customer_segment'].unique()) == 0:
            print("Warning: No customer segments available for drill-down visualization.")
            return
            
        # Focus on the top subcategory for each segment to avoid overcrowding
        top_combos = []
        for segment in segment_subcategory_region['customer_segment'].unique():
            segment_data = segment_subcategory_region[segment_subcategory_region['customer_segment'] == segment]
            
            # Skip if not enough data
            if len(segment_data['subcategory'].unique()) == 0:
                continue
                
            # Get the top subcategory for this segment
            top_subcategory = segment_data.groupby('subcategory')['total_sales'].sum().nlargest(1).index
            
            # Skip if no top subcategory found
            if len(top_subcategory) == 0:
                continue
                
            top_subcategory = top_subcategory[0]
            
            # Filter for that segment-subcategory combination
            top_combo = segment_data[segment_data['subcategory'] == top_subcategory]
            
            # Add to list if not empty
            if not top_combo.empty:
                top_combos.append(top_combo)
        
        # Skip if no top combos found
        if len(top_combos) == 0:
            print("Warning: No top subcategories found for drill-down visualization.")
            return
            
        # Combine the results
        plot_data = pd.concat(top_combos)
        
        # Create a grouped bar chart
        plt.figure(figsize=(14, 8))
        
        # Plot the data
        ax = sns.barplot(
            x='customer_segment',
            y='total_sales',
            hue='region',
            data=plot_data,
            palette='Set2'
        )
        
        # Add subcategory information to the labels
        current_labels = ax.get_xticklabels()
        new_labels = []
        for idx, segment in enumerate(plot_data['customer_segment'].unique()):
            subcategory = plot_data[plot_data['customer_segment'] == segment]['subcategory'].iloc[0]
            new_labels.append(f"{segment}\n({subcategory})")
        
        ax.set_xticklabels(new_labels)
        
        plt.title('Top Subcategory Sales by Customer Segment and Region', fontsize=16)
        plt.xlabel('Customer Segment (Top Subcategory)', fontsize=12)
        plt.ylabel('Total Sales (USD)', fontsize=12)
        plt.legend(title='Region')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("segment_subcategory_region_drilldown.png")
        plt.savefig(output_path)
        print(f"Drill-down visualization saved to {output_path}.")
        plt.close()
        
    except Exception as e:
        print(f"Error visualizing drill-down analysis: {e}")

def visualize_segment_purchase_patterns(enriched_df):
    """Visualize purchase patterns by customer segment."""
    try:
        # Check if we have data
        if enriched_df.empty or 'customer_segment' not in enriched_df.columns:
            print("Warning: Not enough data for segment purchase pattern visualization.")
            return
            
        # Group by segment and product category
        segment_category = enriched_df.groupby(['customer_segment', 'category']).agg({
            'sale_amount_usd': 'sum'
        }).reset_index()
        
        # Get unique segments
        segments = segment_category['customer_segment'].unique()
        
        # Skip if no segments found
        if len(segments) == 0:
            print("Warning: No customer segments found for purchase pattern visualization.")
            return
            
        # Create a pie chart for each segment
        fig, axes = plt.subplots(1, len(segments), figsize=(18, 6))
        
        # If only one segment, convert axes to a list for consistent indexing
        if len(segments) == 1:
            axes = [axes]
        
        for i, segment in enumerate(segments):
            # Filter data for this segment
            segment_data = segment_category[segment_category['customer_segment'] == segment]
            
            # Skip if no data
            if segment_data.empty:
                continue
                
            # Create the pie chart
            axes[i].pie(
                segment_data['sale_amount_usd'],
                labels=segment_data['category'],
                autopct='%1.1f%%',
                startangle=90,
                shadow=True
            )
            axes[i].set_title(f'{segment} Segment')
        
        plt.suptitle('Category Distribution by Customer Segment', fontsize=16)
        plt.tight_layout()
        
        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("segment_category_distribution.png")
        plt.savefig(output_path)
        print(f"Segment purchase pattern visualization saved to {output_path}.")
        plt.close()
        
    except Exception as e:
        print(f"Error visualizing segment purchase patterns: {e}")

def main():
    """Main function for analyzing customer segment and product subcategory relationships."""
    print("Starting Customer Segment & Product Subcategory Analysis...")
    
    # Step 1: Load the data from CSV files
    sales_df, customer_df, product_df = load_data_from_csv()
    
    # Step 2: Create fact-dimension model
    enriched_df = create_fact_dimension_model(sales_df, customer_df, product_df)
    
    print("\nSample of integrated data model:")
    print(enriched_df.head())
    
    # Step 3: Slice - Analyze sales by customer segment and region
    segment_region_performance = analyze_segment_by_region(enriched_df)
    print("\nCustomer Segment by Region Performance Summary:")
    print(segment_region_performance)
    
    # Step 4: Dice - Analyze product subcategory by customer segment
    subcategory_segment_performance = analyze_subcategory_by_segment(enriched_df)
    print("\nSubcategory by Customer Segment Performance Summary:")
    print(subcategory_segment_performance.head(10))  # Show first 10 rows as example
    
    # Step 5: Drill-down - Analyze subcategory performance by segment and region
    segment_subcategory_region = analyze_subcategory_by_segment_and_region(enriched_df)
    print("\nDrill-down Analysis Summary (Top 10 rows):")
    print(segment_subcategory_region.head(10))
    
    # Step 6: Visualize the results
    try:
        # Only create visualizations if we have data
        if not segment_region_performance.empty:
            visualize_segment_by_region(segment_region_performance)
            
        if not subcategory_segment_performance.empty:
            visualize_subcategory_by_segment(subcategory_segment_performance)
            
        if not segment_subcategory_region.empty:
            visualize_drilldown_analysis(segment_subcategory_region)
            
        if not enriched_df.empty:
            visualize_segment_purchase_patterns(enriched_df)
            
        # Step 7: Save results to CSV for further reference
        segment_region_performance.to_csv(RESULTS_OUTPUT_DIR.joinpath("segment_region_performance.csv"), index=False)
        subcategory_segment_performance.to_csv(RESULTS_OUTPUT_DIR.joinpath("subcategory_segment_performance.csv"), index=False)
        segment_subcategory_region.to_csv(RESULTS_OUTPUT_DIR.joinpath("segment_subcategory_region.csv"), index=False)
        
        print("\nAnalysis and visualization completed successfully.")
        print(f"Results saved to {RESULTS_OUTPUT_DIR}")
    except Exception as e:
        print(f"Error during visualization or saving results: {e}")
        print("Analysis completed but visualization or saving results encountered errors.")

if __name__ == "__main__":
    main()