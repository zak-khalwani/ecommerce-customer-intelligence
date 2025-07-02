import os
import pandas as pd
from sqlalchemy import create_engine

# Import custom cleaning functions from 'data_cleaning.py'
from data_cleaning import (
    clean_customers,
    clean_orders,
    clean_order_items,
    clean_products
)

def main():
    """
    Main function to orchestrate the entire ETL pipeline.
    """
    print(">>> Step 1. ETL Pipeline Started...")

    # --- 1. EXTRACT: Load Raw Data ---
    data_path = 'data'
    try:
        customers_raw = pd.read_csv(os.path.join(data_path, 'customers.csv'))
        orders_raw = pd.read_csv(os.path.join(data_path, 'orders.csv'))
        order_items_raw = pd.read_csv(os.path.join(data_path, 'order_items.csv'))
        products_raw = pd.read_csv(os.path.join(data_path, 'products.csv'))
        product_category_raw = pd.read_csv(os.path.join(data_path, 'product_category.csv'))
        print("Raw data extracted successfully.")
    except FileNotFoundError as e:
        print(f"Error: Raw data file not found. {e}")
        return

    # --- 2. TRANSFORM: Clean the Data ---
    print("\n>>> Step 2.Starting data transformation process...")
    customers_clean = clean_customers(customers_raw.copy())
    orders_clean = clean_orders(orders_raw.copy())
    order_items_clean = clean_order_items(order_items_raw.copy())

    # The products cleaning function handles its own merge with the translation table
    products_clean = clean_products(products_raw.copy(), product_category_raw.copy())
    print("Data transformation complete.")

    # --- 3. LOAD: Ingest Clean Data into PostgreSQL ---
    print("\n>>> Step 3. Connecting to PostgreSQL and loading data...")
    try:
        # Get database credentials from environment variables set in docker-compose
        user = os.environ['DB_USER']
        password = os.environ['DB_PASSWORD']
        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        dbname = os.environ['DB_NAME']
        
        # Create the database connection URL
        db_url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
        engine = create_engine(db_url)

        # Ingest each cleaned dataframe into the database
        customers_clean.to_sql('customers', engine, if_exists='replace', index=False)
        orders_clean.to_sql('orders', engine, if_exists='replace', index=False)
        order_items_clean.to_sql('order_items', engine, if_exists='replace', index=False)
        products_clean.to_sql('products', engine, if_exists='replace', index=False)
        
        print("All clean data successfully loaded into PostgreSQL.")

    except KeyError as e:
        print(f"Error: Environment variable {e} not set. Cannot connect to the database.")
    except Exception as e:
        print(f"An error occurred during database ingestion: {e}")
        
    print("ETL Pipeline Finished Successfully!")


if __name__ == '__main__':
    main()