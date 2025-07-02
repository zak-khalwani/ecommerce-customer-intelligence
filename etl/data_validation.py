import pandas as pd
import os
from tabulate import tabulate

# This dictionary defines the schema we expect our final dataframes to have.
COLUMN_REQUIREMENTS = {
    'customers': {
        'customer_id': 'object',
        'customer_unique_id': 'object',
        'customer_zip_code_prefix': 'object',
        'customer_city': 'object',
        'customer_state': 'object'
    },
    'orders': {
        'order_id': 'object',
        'customer_id': 'object',
        'order_status': 'object',
        'order_purchase_timestamp': 'datetime64[ns]',
        'order_delivered_carrier_date': 'datetime64[ns]',
        'order_delivered_customer_date': 'datetime64[ns]',
        'order_estimated_delivery_date': 'datetime64[ns]'
    },
    'order_items': {
        'order_id': 'object',
        'order_item_id': 'int64',
        'product_id': 'object',
        'seller_id': 'object',
        'shipping_limit_date': 'datetime64[ns]',
        'price': 'float64',
        'freight_value': 'float64'
    },
    'products': {
        'product_id': 'object',
        'product_category_name': 'object',
        'product_name_lenght': 'float64',
        'product_description_lenght': 'float64',
        'product_photos_qty': 'float64',
        'product_weight_g': 'float64',
        'product_length_cm': 'float64',
        'product_height_cm': 'float64',
        'product_width_cm': 'float64'
    }
}


def load_data_from_csv(table_mapping, data_path='data'):
    """
    Loads raw data from CSV files into a dictionary of pandas DataFrames.
    
    Args:
        table_mapping (dict): A dictionary mapping CSV filenames to desired table names.
        data_path (str): The relative path to the directory containing the CSV files.

    Returns:
        dict: A dictionary where keys are table names and values are the loaded DataFrames.
    """
    data = {}
    print("\n--- Step 1. Starting Data Loading ---")
    for csv_file, table_name in table_mapping.items():
        try:
            full_path = os.path.join(data_path, csv_file)
            df = pd.read_csv(full_path)
            data[table_name] = df
            print(f"Successfully loaded '{csv_file}' into '{table_name}' DataFrame.")
        except FileNotFoundError:
            print(f"Warning: {csv_file} not found at {full_path}. Skipping.")
        except Exception as e:
            print(f"An error occurred with {csv_file}: {e}")
    print("--- Data Loading Complete ---\n")
    return data


def merge_product_translations(data):
    """
    Merges English product category names into the products table.
    
    Args:
        data (dict): The dictionary of DataFrames.

    Returns:
        dict: The modified dictionary of DataFrames with a transformed products table.
    """
    print("\n--- Step 2. Merging Product Category Translations ---")
    if 'products' in data and 'product_category' in data:
        data['products'] = data['products'].merge(data['product_category'], how="left", on="product_category_name")
        data['products'].drop(columns='product_category_name', inplace=True)
        data['products'].rename(columns={'product_category_name_english': 'product_category_name'}, inplace=True)
        del data['product_category']
        print("Successfully merged translations and cleaned up products table.")
    else:
        print("Warning: 'products' or 'product_category' table not found. Skipping merge.")
    print("--- Merge Complete ---\n")
    return data


def check_data_types(data, requirements):
    """
    Validates the data types of each column against a requirements dictionary.
    This function uses your provided logic to report mismatches.
    
    Args:
        data (dict): The dictionary of DataFrames.
        requirements (dict): The dictionary specifying required data types for each table.
    """
    print("\n--- Step 3. Validating Data Types ---")
    
    mismatched_dtype = []

    for table, requirement in requirements.items():
        if table not in data:
            continue # Skip if table doesn't exist in data
        
        for column, expected_dtype in requirement.items():
            if column in data[table].columns:
                actual_dtype = str(data[table][column].dtype) # Convert dtype to string for comparison
                if expected_dtype != actual_dtype:
                    mismatched_dtype.append({
                        "table_name": table,
                        "column_name": column,
                        "actual_data_type": actual_dtype,
                        "expected_data_type": expected_dtype
                    })

    if mismatched_dtype:
        print("Found columns that don't match the data type requirement:")
        print(tabulate(mismatched_dtype, headers="keys", tablefmt="grid"))
    else:
        print("All columns matched data type requirement.")
        
    print("--- Data Type Validation Complete ---\n")


def check_duplicates(data):
    """
    Checks for fully duplicated rows in each DataFrame.
    
    Args:
        data (dict): The dictionary of DataFrames.
    """
    print("\n--- Step 4. Checking for Duplicates ---")
    duplicates = []
    for table, df in data.items():
        duplicate_counts = df.duplicated(keep=False).sum()
        if duplicate_counts > 0:
            duplicates.append({
                "table_name": table,
                "duplicate_rows": duplicate_counts
            })
    
    if duplicates:
        print("Found duplicate rows in the following tables:")
        print(tabulate(duplicates, headers="keys", tablefmt="grid"))
    else:
        print("No duplicate rows found in any table.")
    print("--- Duplicate Check Complete ---\n")


def check_missing_values(data):
    """
    Checks for missing values in each DataFrame and reports the counts.
    
    Args:
        data (dict): The dictionary of DataFrames.
    """
    print("\n--- Step 5. Checking for Missing Values ---")
    missing_values = []
    for table, df in data.items():
        for column, missing_count in df.isnull().sum().items():
            if missing_count > 0:
                missing_values.append({
                    "table_name": table,
                    "column_name": column,
                    "missing_count": missing_count
                })

    if missing_values:
        print("Found missing values in the following columns:")
        print(tabulate(missing_values, headers="keys", tablefmt="grid"))
    else:
        print("No missing values found in any table.")
    print("--- Missing Value Check Complete ---\n")


def check_data_sanity(data):
    """
    Performs specific data sanity checks, such as negative prices.
    
    Args:
        data (dict): The dictionary of DataFrames.
    """
    print("\n--- Step 6. Performing Data Sanity Checks ---")
    sanity_issues = []
    
    # Check for negative price or freight value in order_items
    if 'order_items' in data:
        order_items_df = data['order_items']
        negative_price_count = (order_items_df['price'] < 0).sum()
        negative_freight_count = (order_items_df['freight_value'] < 0).sum()

        if negative_price_count > 0:
            sanity_issues.append({'check': 'Negative Price', 'table': 'order_items', 'count': negative_price_count})
        if negative_freight_count > 0:
            sanity_issues.append({'check': 'Negative Freight Value', 'table': 'order_items', 'count': negative_freight_count})
            
    if sanity_issues:
        print("Found data sanity issues:")
        print(tabulate(sanity_issues, headers="keys", tablefmt="grid"))
    else:
        print("No data sanity issues found.")
    print("--- Sanity Check Complete ---\n")


def run_validation_pipeline():
    """
    Main function to orchestrate the entire data validation pipeline.
    """
    # Define the mapping of CSV files to table names
    table_mapping = {
        'customers.csv': 'customers',
        'orders.csv': 'orders', 
        'order_items.csv': 'order_items',
        'products.csv': 'products',
        'product_category.csv': 'product_category'
    }

    # 1. Load data
    data_dict = load_data_from_csv(table_mapping)

    # 2. Merge translations
    data_dict = merge_product_translations(data_dict)

    # 3. Run validation checks
    check_data_types(data_dict, COLUMN_REQUIREMENTS)
    check_duplicates(data_dict)
    check_missing_values(data_dict)
    check_data_sanity(data_dict)


if __name__ == '__main__':
    run_validation_pipeline()