# Data quality module for BookHaven ETL (STUDENT VERSION)
"""Data quality validation and reporting functions.

Instructions:
- Implement each function to validate, check, or report on data quality for a DataFrame.
- Use field/type checks, pattern matching, and summary statistics as described in 'Integration Testing with Quality Metrics for Data Sources'.
- Return results in a format suitable for reporting and testing.
- Document your approach and any assumptions.
"""
import pandas as pd
import re 

ERROR = "placeholder"

def validate_schema(df, required_fields):
    """Validate DataFrame schema against required fields.
    Hint: Check for missing or extra columns. See 'Integration Testing with Quality Metrics for Data Sources'.
    """
    actual = set(df.columns)
    required = set(required_fields)
    missing = required - actual
    extra = actual - required
    return missing, extra

def check_duplicates(df, field):
    """Check for duplicate values in a field and return a summary or list.
    Hint: Use pandas.duplicated and value_counts. See 'Data Quality & Cleaning with Pandas'.
    """
    if df and field and field not in df.columns:
        return False
    duplicated = df[df.duplicated(subset=[field], keep=False)]
    return duplicated[field].value_counts()

def quality_report(df):
    """Generate a data quality report for a DataFrame (missing, invalid, duplicates, etc.).
    Hint: Summarize key quality metrics. See 'Integration Testing with Quality Metrics for Data Sources' and 'E2E Pipeline Testing with Health Monitoring'.
    """
    report = pd.DataFrame(index=df.columns)
    
    report['Missing Values'] = df.isnull().sum()
    report['% Missing'] = (df.isnull().mean() * 100).round(2)
    report['Unique Values'] = df.nunique()
    report['Duplicate Rows'] = df.duplicated().sum()

    
    return report
    

def validate_field_level(df: pd.DataFrame, rules: dict):
    results = []
    if df.empty:
        return results

    for field, spec in rules.items():
        required = spec.get('required', False)
        pattern = spec.get('pattern')
        min_len = spec.get('min_length')
        max_len = spec.get('max_length')

        if field not in df.columns:
            if required:
                results.append((None, field, f"Missing required field '{field}'"))
            continue

        for idx, val in df[field].items():
            row_num = idx + 2
            if required and (pd.isnull(val) or val == ""):
                results.append((row_num, field, "Missing required value"))
                continue
            if isinstance(val, str):
                if min_len is not None and len(val) < min_len:
                    results.append((row_num, field,
                        f"String too short (len {len(val)} < min {min_len})"))
                if max_len is not None and len(val) > max_len:
                    results.append((row_num, field,
                        f"String too long (len {len(val)} > max {max_len})"))
                if pattern and not re.fullmatch(pattern, val):
                    results.append((row_num, field, "Pattern mismatch"))
    return results

def validate_list_length(df: pd.DataFrame, field: str,
                         min_length=None, max_length=None):
    results = []
    if df.empty or field not in df.columns:
        return results

    for idx, item in df[field].items():
        if isinstance(item, list):
            length = len(item)
            row_num = idx + 2
            if min_length is not None and length < min_length:
                results.append(
                    (row_num, field,
                     f"Too short (length {length} < min_length {min_length})")
                )
            if max_length is not None and length > max_length:
                results.append(
                    (row_num, field,
                     f"Too long (length {length} > max_length {max_length})")
                )
    return results

def generate_quality_report(results):
    if not results:
        return "No data quality issues found"

    messages = ["Data Quality Report"]
    for item in results:
        if isinstance(item, tuple) and len(item) == 3:
            row, field, msg = item
            messages.append(f"Row {row}, Field '{field}': {msg}")
        elif isinstance(item, tuple) and len(item) == 5:
            row, field, issue, error, msg = item
            messages.append(
                f"Malformed validation result Row {row}, Field '{field}': [{error}] {issue} - {msg}"
            )
        else:
            messages.append(f"Malformed validation result: {item!r}")

    return " ".join(messages)