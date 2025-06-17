# Main ETL Pipeline for BookHaven ETL (STUDENT VERSION)
"""
Main entry point for the BookHaven ETL pipeline.

Instructions:
- Implement the ETL pipeline by calling each step in order: extract, clean, validate, transform, load, and report.
- Use the modular functions you implemented in the other ETL modules.
- Add logging, error handling, and SLA/performance tracking as described in 'E2E Pipeline Testing with Health Monitoring'.
- Reference the milestone checklist and rubric in the README.
- Document your approach and any assumptions.
"""
from etl import extractors, cleaning, data_quality, transformers, loaders
from config import DATABASE_CONFIG
import hashlib

def main():
    """Run the ETL pipeline (students must implement each step).
    Hint: Follow the ETL workflow from the lessons. Use try/except for error handling and log/report each step's results.
    """
    # 1. Extract data from all sources (see extractors.py)
    # 2. Clean and validate data (see cleaning.py, data_quality.py)
    # 3. Transform data for star schema (see transformers.py)
    # 4. Load data into SQL Server (see loaders.py)
    # 5. Output health/trend report (see README and lessons on monitoring)

    books_df = extractors.extract_csv_book_catalog("data/csv/book_catalog.csv")
    authors_df = extractors.extract_json_author_profiles("data/json/author_profiles.json")
    customers_df = extractors.extract_sqlserver_table("customers")

    desired = ["author", "isbn"]
    try:
        profiler_df = books_df[desired]
    except KeyError:
        profiler_df = books_df.loc[:, books_df.columns.isin(desired)]

    mongo_customers_df = extractors.extract_mongodb_customers(
        DATABASE_CONFIG['mongodb']['connection_string'],
        DATABASE_CONFIG['mongodb']['database'],
        'customers'
    )

    mongo_customers_df['customer_id'] = mongo_customers_df['email'].apply(hashEmail)

    orders_df = extractors.extract_sqlserver_table('orders', 'sql_server_source')
    books_df = transformers.transform_books(books_df)
    authors_df = transformers.transform_authors(authors_df)
    
    customers_df = transformers.transform_customers(customers_df)
    mongo_customers_df = transformers.transform_customers(mongo_customers_df)

    orders_df = transformers.transform_orders(orders_df)
    
    book_rules = {'title': {'required': True}}
    author_rules = {'name': {'required': True}}
    customer_rules = {'name': {'required': True}}
    order_rules = {'order_id': {'required': True}}

    validation_results = []
    validation_results.extend(data_quality.validate_field_level(books_df, book_rules))
    validation_results.extend(data_quality.validate_field_level(authors_df, author_rules))
    validation_results.extend(data_quality.validate_field_level(customers_df, customer_rules))
    validation_results.extend(data_quality.validate_field_level(orders_df, order_rules))
    for result in validation_results:
        print(result)
    
    loaders.load_dimension_table(books_df, 'dim_book', DATABASE_CONFIG['sql_server_dw'])
    loaders.load_dimension_table(authors_df, 'dim_author', DATABASE_CONFIG['sql_server_dw'])
    loaders.load_dimension_table(customers_df, 'dim_customer', DATABASE_CONFIG['sql_server_dw'])
    loaders.load_dimension_table(mongo_customers_df, 'dim_customer', DATABASE_CONFIG['sql_server_dw'])
    
    # create_dim_dates only needs to run once, uncomment when you want to submit
    #loaders.create_dim_dates(start_date='1950-01-01', end_date='2025-01-01', sql_conn_str=DATABASE_CONFIG['sql_server_dw'])
    required = ["book_key", "author_key", "customer_key", "date_key", "quantity", "price"]
    orders_df = loaders.transform_to_load(profiler_df, orders_df, required, sql_conn_str=DATABASE_CONFIG['sql_server_dw'])
    loaders.load_fact_table(orders_df, 'fact_book_sales', DATABASE_CONFIG['sql_server_dw'])

def hashEmail(email):
    hash_object = hashlib.md5(email.encode())
    hash_hex = hash_object.hexdigest()
    hash_int = int(hash_hex, 16) % 100_000
    return hash_int

if __name__ == "__main__":
    main() 