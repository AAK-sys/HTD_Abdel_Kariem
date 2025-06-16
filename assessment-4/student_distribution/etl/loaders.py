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
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )
    return url

def create_dim_dates(start_date: str = '2020-01-01', end_date: str = '2024-12-31', sql_conn_str: str = None):

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_dates = pd.DataFrame({
        'date': date_range,
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day
    })
    
    dim_dates['date_key'] = dim_dates['date'].dt.strftime('%Y%m%d').astype(int)
    
    dim_dates = dim_dates[['date_key', 'date', 'year', 'month', 'day']]

    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        dim_dates.to_sql('dim_date', con=conn, if_exists='replace', index=False)
    
    return dim_dates

# --- Load Dimension Table ---
def load_dimension_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")
    
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
    print(len(df), "records loaded")


def load_fact_table(df: pd.DataFrame, table_name: str, sql_conn_str: str):
    url = get_connection_url(sql_conn_str)
    try:
        engine = create_engine(url)
    except Exception as e:
        print(e)
        engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists='append', index=False)
    print(len(df), "records loaded")
