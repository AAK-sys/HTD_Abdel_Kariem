"""
Loaders for BookHaven ETL Assessment
"""

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
import json
from datetime import datetime, timedelta


def get_connection_url(dcf):
    if isinstance(dcf, str):
        return dcf
    url = (
        f"mssql+pyodbc://{dcf['username']}:"
        f"{dcf['password']}@localhost:1433/"
        f"{dcf['database']}"
        # "?driver=ODBC+Driver+18+for+SQL+Server"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )
    return url


def create_dim_dates(
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    sql_conn_str: str = None,
):

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    dim_dates = pd.DataFrame(
        {
            "date": date_range,
            "year": date_range.year,
            "month": date_range.month,
            "day": date_range.day,
        }
    )

    dim_dates["date_key"] = dim_dates["date"].dt.strftime("%Y%m%d").astype(int)

    dim_dates = dim_dates[["date_key", "date", "year", "month", "day"]]

    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        dim_dates.to_sql("dim_date", con=conn, if_exists="append", index=False)

    return dim_dates


# --- Load Dimension Table ---


def get_table_columns(engine, table_name):
    insp = inspect(engine)
    return [col["name"] for col in insp.get_columns(table_name)]


def load_dimension_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="append", index=False)
    print(len(df), "records loaded")


def load_fact_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="append", index=False)
    print(len(df), "records loaded")


def transform_to_load(profile_df, orders_df, required, sql_conn_str):

    url = get_connection_url(sql_conn_str)
    engine = create_engine(url)

    authors_df = pd.read_sql(
        "SELECT author_key, name AS author FROM dim_author", engine
    )
    books_df = pd.read_sql("SELECT book_key, isbn FROM dim_book", engine)
    customers_df = pd.read_sql(
        "SELECT customer_key, customer_id FROM dim_customer", engine
    )

    orders_with_cust = orders_df.merge(customers_df, on="customer_id", how="left")

    author_book_keys = profile_df.merge(
        authors_df, left_on="author", right_on="author", how="left"
    ).merge(books_df, left_on="isbn", right_on="isbn", how="left")[
        ["isbn", "author_key", "book_key"]
    ]

    final = orders_with_cust.merge(
        author_book_keys, left_on="book_isbn", right_on="isbn", how="left"
    )

    final["order_date"] = pd.to_datetime(final["order_date"], errors="coerce")
    final = final.dropna()
    final["date_key"] = final["order_date"].dt.strftime("%Y%m%d").astype(int)

    result = final[
        ["book_key", "author_key", "customer_key", "date_key", "quantity", "price"]
    ]

    if required:
        result = result[required]

    return result
