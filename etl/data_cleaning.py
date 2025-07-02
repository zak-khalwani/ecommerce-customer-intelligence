import pandas as pd

def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """A helper function to standardize all column names to snake_case."""
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the customers table.
    - Standardizes column names to snake_case.
    - Fixes data type of 'customer_zip_code_prefix'.
    """
    df = _standardize_column_names(df)
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str)
    print("Cleaned 'customers' table.")
    return df

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the orders table.
    - Standardizes column names to snake_case.
    - Converts timestamp columns to datetime.
    """
    df = _standardize_column_names(df)
    for column in df.columns:
        if "timestamp" in column or "date" in column or "_at" in column:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    print("Cleaned 'orders' table.")
    return df

def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the order_items table.
    - Standardizes column names to snake_case.
    - Converts 'shipping_limit_date' to datetime.
    """
    df = _standardize_column_names(df)
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    print("Cleaned 'order_items' table.")
    return df

def clean_products(df: pd.DataFrame, translation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the products table *after* it has been merged.
    - Standardizes column names to snake_case.
    - Renames columns.
    - Fills missing category names.
    - Reorders columns.
    """
    
    # Standardize columns on both dataframes before the merge
    df = _standardize_column_names(df)
    translation_df = _standardize_column_names(translation_df)
    
    # Perform the merge
    df = pd.merge(df, translation_df, on='product_category_name', how='left')
    df.drop('product_category_name', axis=1, inplace=True)

    # rename columns
    df.rename(columns = {
        'product_category_name_english' : 'product_category_name', 
        'product_name_lenght' : 'product_name_length',
        'product_description_lenght' : 'product_description_length'
    }, inplace=True)
    
    # Fill missing category names (this column only exists after the merge)
    if 'product_category_name' in df.columns:
        df['product_category_name'].fillna('unknown', inplace=True)
    
    # reorder columns
    df = pd.DataFrame(df[['product_id', 'product_category_name', 'product_name_length', 'product_description_length', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']])

    print("Cleaned 'products' table.")
    return df