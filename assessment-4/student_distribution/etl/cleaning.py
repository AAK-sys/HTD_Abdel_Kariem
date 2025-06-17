"""
Data cleaning module for BookHaven ETL Assessment (Student Version)

Instructions:
- Implement each function to clean and standardize the specified field in the DataFrame.
- Use pandas string/date methods and regular expressions as needed (see 'Data Quality & Cleaning with Pandas').
- Handle missing, invalid, or inconsistent data as described in the lessons.
- Document your approach and any edge cases handled.
"""
import pandas as pd
import re
import numpy as np
import warnings

# --- Clean Dates ---
def clean_dates(df, field):
    """Clean and standardize date fields to YYYY-MM-DD format.
    Hint: Use pandas.to_datetime with error handling. See 'Data Quality & Cleaning with Pandas'.
    """
    df[field] = pd.to_datetime(df[field], errors='coerce')
    df[field] = df[field].dt.strftime('%Y-%m-%d')
    return df

# --- Clean Emails ---
def clean_emails(df, field):
    """Clean and validate email fields (set invalid emails to None or NaN).
    Hint: Use regular expressions and pandas apply. See 'Data Quality & Cleaning with Pandas' and 'Unit Testing for Data Transformations'.
    """
    regexp = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df[field] = df[field].str.strip().apply(lambda x: x if isinstance(x, str) and re.fullmatch(regexp, x) else None)
    return df

# --- Clean Phone Numbers ---
def clean_phone_numbers(df, field):
    """Standardize phone numbers (remove non-digits, set invalid to None).
    Hint: Use regular expressions and pandas string methods. See 'Data Quality & Cleaning with Pandas'.
    """
    df[field] = df[field].str.replace(r'\D', '', regex=True)
    df[field] = df[field].apply(lambda x: f"{str(x)}" if isinstance(x, str) and len(x) == 10 else None)
    return df

# --- Clean Numerics ---
def clean_numerics(df, field):
    """Convert to numeric, set invalid to NaN.
    Hint: Use pandas.to_numeric with error handling. See 'Data Quality & Cleaning with Pandas'.
    """
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        df.loc[:,field] = df[field].astype(str)

        df.loc[:,field] = df[field].str.replace(r'[^0-9.]', '', regex=True)
        
        df.loc[:,field] = pd.to_numeric(df[field], errors='coerce')
        return df

# --- Clean Text ---
def clean_text(df, field):
    """Clean text fields (e.g., strip whitespace, standardize case, remove special characters).
    Hint: Use pandas string methods. See 'Pandas Fundamentals for ETL' and 'Data Quality & Cleaning with Pandas'.
    """
    df[field] = df[field].apply(lambda x: x.strip().title() if isinstance(x, str) else '')
    return df

# --- Remove Duplicates ---
def remove_duplicates(df, subset=None):
    """Remove duplicate rows based on subset of fields.
    Hint: Use pandas.drop_duplicates. See 'Data Quality & Cleaning with Pandas'.
    """
    df = df.drop_duplicates(subset)
    return df

# --- Handle Missing Values ---
def handle_missing_values(df, strategy='drop', fill_value=None):
    """Handle missing values using specified strategy ('drop', 'fill').
    Hint: Use pandas.dropna or pandas.fillna. See 'Data Quality & Cleaning with Pandas'.
    """
    df = df.copy()
    df.replace("", np.nan, inplace=True)
    
    if strategy == 'drop':
        return df.dropna()
    elif strategy == 'fill':
        return df.fillna(fill_value)
    else:
        return df