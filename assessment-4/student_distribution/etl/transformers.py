"""
Transformers for BookHaven ETL Assessment
"""
import pandas as pd
from etl import cleaning
import numpy as np
# --- Book Series Transformer ---
def transform_book_series(df_books):
    """Add a 'series_normalized' column (copy of 'series' for now)."""
    df_books = df_books.copy()
    if 'series' in df_books.columns:
        df_books['series_normalized'] = df_books['series']
    else:
        df_books['series_normalized'] = None
    return df_books

# --- Author Collaborations Transformer ---
def transform_author_collaborations(df_authors):
    """Add a 'collab_count' column (number of collaborations)."""
    df_authors = df_authors.copy()
    if 'collaborations' in df_authors.columns:
        df_authors['collab_count'] = df_authors['collaborations'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_authors['collab_count'] = 0
    return df_authors

# --- Customer Reading History Transformer ---
def transform_reading_history(df_customers):
    """Add a 'reading_count' column (number of books read)."""
    df_customers = df_customers.copy()
    if 'reading_history' in df_customers.columns:
        df_customers['reading_count'] = df_customers['reading_history'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['reading_count'] = 0
    return df_customers

# --- Book Recommendations Transformer ---
def transform_book_recommendations(df_books, df_customers):
    """Return books DataFrame with a dummy 'recommended_score' column."""
    df_books = df_books.copy()
    df_books['recommended_score'] = 1.0  # Placeholder
    return df_books

# --- Genre Preferences Transformer ---
def transform_genre_preferences(df_customers):
    """Add a 'num_genres' column (number of preferred genres)."""
    df_customers = df_customers.copy()
    if 'genre_preferences' in df_customers.columns:
        df_customers['num_genres'] = df_customers['genre_preferences'].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
    else:
        df_customers['num_genres'] = 0
    return df_customers

# Transformation module for BookHaven ETL (STUDENT VERSION)
"""Business logic and star schema transformation functions.

Instructions:
- Implement each function to transform the input DataFrame for loading into the star schema.
- Apply business rules, SCD Type 1 logic, and any required joins or aggregations (see 'ETL Transformations with Pandas').
- Ensure output matches the target schema for each dimension/fact table.
- Document your approach and any edge cases handled.
"""
def transform_books(books_df):
    df = books_df.copy()
    
    if 'title' in df.columns:
        df = cleaning.clean_text(df, 'title')
    if 'genre' in df.columns:
        df = cleaning.clean_text(df, 'genre')
    if 'series' in df.columns:
        df = cleaning.clean_text(df, 'series')
    if 'isbn' in df.columns:
        df = cleaning.clean_text(df, 'isbn')
    if 'recommended' in df.columns:
        df['recommended'] = df['recommended'].apply(lambda x: 'Yes' if x else 'No' if x is not None else '')

    df = transform_book_series(df)
    df = cleaning.handle_missing_values(df, strategy='drop', fill_value='')
    expected = ["book_key", "isbn", "title", "genre", "series", "recommended"]
    df = df[df.columns.intersection(expected)]
    return df

def transform_authors(authors_df):
    df = authors_df.copy()

    if 'name' in df.columns:
        df = cleaning.clean_text(df, 'name')
    if 'email' in df.columns:
        df = cleaning.clean_emails(df, 'email')
    if 'phone' in df.columns:
        df = cleaning.clean_phone_numbers(df, 'phone')
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(lambda x: ','.join(x) if isinstance(x, (list, tuple)) else str(x) if x else '')

    df = transform_author_collaborations(df)
    df = cleaning.handle_missing_values(df, strategy='drop', fill_value='')
    df = cleaning.remove_duplicates(df, subset=['email'])
    expected = ["author_key", "name", "email", "phone", "genres"]
    df = df[df.columns.intersection(expected)]
    
    return df

def transform_customers(customers_df):
    df = customers_df.copy()

    if 'name' in df.columns:
        df = cleaning.clean_text(df, 'name')
    if 'email' in df.columns:
        df = cleaning.clean_emails(df, 'email')
    if 'phone' in df.columns:
        df = cleaning.clean_phone_numbers(df, 'phone')
    if 'genre_preferences' in df.columns:
        df['genre_preferences'] = df['genre_preferences'].apply(lambda x: ','.join(x) if isinstance(x, (list, tuple)) else str(x) if x else '')

    df = transform_reading_history(df)
    df = transform_genre_preferences(df)
    df = cleaning.handle_missing_values(df, strategy='drop', fill_value='')
    
    expected = ["customer_id", "name", "email", "phone", "genre_preferences"]
    df = df[df.columns.intersection(expected)]
    
    
    return df

def transform_orders(orders_df):
    df = orders_df.copy()
    
    if 'quantity' in df.columns:
        df = cleaning.clean_numerics(df, 'quantity')
        df['quantity'] = df['quantity'].astype('Int64')
    if 'price' in df.columns:
        df = cleaning.clean_numerics(df, 'price')
        df['price'] = df['price'].astype(np.float32).round(2)

    df = cleaning.handle_missing_values(df, strategy='fill', fill_value=0)
    df = cleaning.remove_duplicates(df, subset=['order_id'])
    
    return df 