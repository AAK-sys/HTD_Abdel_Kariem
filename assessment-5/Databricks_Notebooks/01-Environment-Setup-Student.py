# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Fraud Detection System - Environment Setup
# MAGIC
# MAGIC **Week 6 Assessment - Part 1: Environment Setup**
# MAGIC
# MAGIC This notebook sets up the environment and loads all data files for the fraud detection system.
# MAGIC
# MAGIC ## Objectives:
# MAGIC 1. Import required libraries and test Spark connection
# MAGIC 2. Load the three data files (transactions, customers, fraud rules)
# MAGIC 3. Display basic data info and quality checks
# MAGIC 4. Verify all data loaded correctly
# MAGIC
# MAGIC ## Expected Data Files:
# MAGIC - `banking_transactions.csv` (100 transactions)
# MAGIC - `customer_profiles.csv` (50 customers) 
# MAGIC - `fraud_rules.json` (fraud detection rules)
# MAGIC
# MAGIC ## Success Criteria:
# MAGIC - All files load without errors
# MAGIC - Data displays correctly
# MAGIC - Row counts match expectations
# MAGIC - Basic data validation passes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Import Libraries and Test Spark Connection

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Test Spark connection
print("Testing Spark Connection...")
print(f"Spark Version: {spark.version}")
print(f"Spark Session ID: {spark.sparkContext.applicationId}")

# Create a simple test DataFrame to verify Spark is working
test_data = [(1, "test"), (2, "spark"), (3, "connection")]
test_df = spark.createDataFrame(test_data, ["id", "status"])
test_df.show()

print("✅ Spark connection successful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Banking Transaction Data

# COMMAND ----------

# TODO 1: Load Banking Transaction Data
"""
OBJECTIVE: Load and validate the banking transactions CSV file

COMPLETE THE CODE TEMPLATE BELOW:
Fill in the missing parts marked with # FILL IN:
"""

print("Loading banking transactions data...")
DATA_PATH = "/mnt/coursedata/"

try:
    # Step 1: Load the CSV file
    transactions_df = spark.read.csv(
        f"{DATA_PATH}banking_transactions.csv",  # File path
        header=True,  # First row contains column names
        inferSchema=True,  # Automatically detect data types
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss"  # Date format
    )
    
    # Step 2: Display basic information
    print("✅ Transactions loaded successfully!")
    row_count = transactions_df.count()
    print(f"Total transactions: {row_count}")
    print(f"Number of columns: {len(transactions_df.columns)}")
    print(f"Column names: {transactions_df.columns}")
    
    # Step 3: Show sample data
    print("\n📊 Sample Transaction Data:")
    # FILL IN: Use .show() method to display first 5 rows with truncate=False
    transactions_df.show(5, truncate=False)
    
    # Step 4: Display schema
    print("\n📋 Transaction Data Schema:")
    # FILL IN: Use .printSchema() to show column types
    transactions_df.printSchema()
    
    # Step 5: Check for null values
    print("\n🔍 Null Value Check:")
    # FILL IN: Create null_counts using this pattern:
    null_counts = transactions_df.select([count(when(col(c).isNull(), c)).alias(c) for c in transactions_df.columns])
    # Then use .show() to display results
    null_counts.show()
    
    
except Exception as e:
    print(f"❌ Error loading transactions data: {str(e)}")
    print("Please ensure banking_transactions.csv is uploaded to the correct location")

# END TODO 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Customer Profile Data

# COMMAND ----------

# TODO 2: Load Customer Profile Data
"""
OBJECTIVE: Load customer profiles and perform demographic analysis

COMPLETE THE CODE TEMPLATE BELOW:
"""

print("Loading customer profiles data...")

try:
    # Step 1: Load the CSV file (following the same pattern as transactions)
    customers_df = spark.read.csv(f"{DATA_PATH}customer_profiles.csv", header=True, inferSchema=True)
    
    
    # Step 2: Display basic information
    print("✅ Customer profiles loaded successfully!")
    print(f"Total customers: {customers_df.count()}")
    print(f"Number of columns: {len(customers_df.columns)}")
    print(f"Column names: {customers_df.columns}")
    
    # Step 3: Show sample data
    print("\n📊 Sample Customer Data:")
    # FILL IN: Show first 5 rows
    customers_df.show(5)
    
    # Step 4: Display schema
    print("\n📋 Customer Data Schema:")
    # FILL IN: Print schema
    customers_df.printSchema()
    
    # Step 5: Check for null values (same pattern as transactions)
    print("\n🔍 Null Value Check:")
    # FILL IN: Create and show null_counts
    null_counts = customers_df.select([count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns])
    null_counts.show()
    
    
    # Step 6: Summary statistics for numeric columns
    print("\n📈 Customer Demographics Summary:")
    # FILL IN: Use .select() to choose numeric columns: "age", "credit_score", "annual_income", "account_tenure_months"
    # Then use .describe() and .show()
    customers_df.select(# FILL IN: list of numeric columns
                        age="age",
                        credit_score="credit_score",
                        annual_income="annual_income",
                        account_tenure_months="account_tenure_months"
                       ).describe().show()
    
except Exception as e:
    print(f"❌ Error loading customer data: {str(e)}")
    print("Please ensure customer_profiles.csv is uploaded to the correct location")

# END TODO 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Fraud Rules Configuration

# COMMAND ----------

# TODO 3: Load and Parse Fraud Rules JSON
"""
OBJECTIVE: Load fraud detection rules from JSON file and convert to Spark DataFrame

COMPLETE THE TEMPLATE BELOW - Most complex section with extra scaffolding:
"""

print("Loading fraud detection rules...")

try:
    # Step 1: Load JSON file as text (PROVIDED - This is the tricky part)
    fraud_rules_text = spark.read.text(f"{DATA_PATH}fraud_rules.json").collect()
    
    # Step 2: Parse JSON data line by line
    fraud_rules = []
    for row in fraud_rules_text:
        rule_line = row.value.strip()
        if rule_line:  # Skip empty lines
            try:
                # FILL IN: Use json.loads() to parse rule_line
                rule = json.loads(rule_line)
                fraud_rules.append(rule)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse line: {rule_line}")
    
    # Step 3: Display basic info
    print(f"✅ Fraud rules loaded successfully!")
    print(f"Total rules: {len(fraud_rules)}")
    
    # Step 4: Display rules summary (PROVIDED - Complex formatting)
    print("\n📋 Fraud Rules Summary:")
    for i, rule in enumerate(fraud_rules, 1):
        print(f"Rule {i}: {rule.get('rule_name', 'Unknown')}")
        print(f"  - Priority: {rule.get('priority', 'Unknown')}")
        print(f"  - Risk Score: {rule.get('thresholds', {}).get('risk_score', 'Unknown')}")
        print(f"  - Active: {rule.get('active', 'Unknown')}")
        print()
    
    # Step 5: Create DataFrame from rules
    rules_data = []
    for rule in fraud_rules:
        # FILL IN: Create dictionary with these keys:
        rule_dict = {
            'rule_id': rule.get('rule_id'),
            'rule_name': rule.get('rule_name'), # FILL IN: get rule_name from rule
            'priority': rule.get('priority'), # FILL IN: get priority from rule
            'risk_score': rule.get('thresholds', {}).get('risk_score'), # FILL IN: get risk_score from rule.get('thresholds', {})
            'active': rule.get('is_active') # FILL IN: get active from rule
        }
        rules_data.append(rule_dict)
    
    # Step 6: Convert to Spark DataFrame
    rules_df = spark.createDataFrame(rules_data) # FILL IN: Use spark.createDataFrame(rules_data)
    
    print("📊 Fraud Rules DataFrame:")
    # FILL IN: Show the DataFrame with truncate=False
    rules_df.show(truncate=False)
    
except Exception as e:
    print(f"❌ Error loading fraud rules: {str(e)}")
    print("Please ensure fraud_rules.json is uploaded and properly formatted")
    
    # Backup rules (PROVIDED - Don't change this)
    print("\n⚠️ Creating backup fraud rules...")
    backup_rules = [
        {
            'rule_id': 'HIGH_AMOUNT_SINGLE',
            'rule_name': 'High Amount Single Transaction',
            'priority': 'HIGH',
            'risk_score': 75,
            'active': True
        },
        {
            'rule_id': 'VELOCITY_MULTIPLE_TRANSACTIONS',
            'rule_name': 'Transaction Velocity Check',
            'priority': 'MEDIUM',
            'risk_score': 60,
            'active': True
        }
    ]
    
    rules_df = spark.createDataFrame(backup_rules)
    fraud_rules = backup_rules
    print("✅ Backup fraud rules created successfully!")

# END TODO 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Quality Validation

# COMMAND ----------

# TODO 4: Comprehensive Data Quality Validation
"""
OBJECTIVE: Perform thorough data quality checks on transactions and customers

COMPLETE THE TEMPLATE BELOW:
"""

print("Performing comprehensive data quality validation...")

# TRANSACTION VALIDATION
print("\n🔍 Transaction Data Validation:")
try:
    # Step 1: Check for duplicate transaction IDs
    total_transactions = transactions_df.count()
    unique_transactions = transactions_df.select("transaction_id").distinct().count() # FILL IN: Get count of distinct transaction_id values
    
    print(f"Total transactions: {total_transactions}")
    print(f"Unique transaction IDs: {unique_transactions}")
    
    # Step 2: Check if counts match (no duplicates)
    if total_transactions == unique_transactions:
        print("✅ No duplicate transaction IDs found")
    else:
        print("⚠️ Duplicate transaction IDs detected")
    
    # Step 3: Transaction amount statistics
    # FILL IN: Use .select("amount").describe() to get amount statistics
    amount_stats = transactions_df.select("amount").describe()
    amount_stats.show()
    
    # Step 4: Fraud distribution
    # FILL IN: Use .groupBy("is_fraud").count().orderBy("is_fraud") to see fraud distribution
    fraud_distribution = transactions_df.groupBy("is_fraud").count().orderBy("is_fraud")
    fraud_distribution.show()
    
    # Step 5: Calculate fraud rate
    fraud_transactions = transactions_df.filter(col("is_fraud") == 1).count()
    fraud_rate = fraud_transactions.count() / total_transactions * 100 # FILL IN: Calculate percentage (fraud_transactions / total_transactions * 100)
    print(f"Fraud rate: {fraud_rate:.2f}%")
    
except Exception as e:
    print(f"❌ Error in transaction validation: {str(e)}")

# CUSTOMER VALIDATION
print("\n🔍 Customer Data Validation:")
try:
    # Step 1: Check for duplicate customer IDs
    total_customers = customers_df.count()
    unique_customers = customers_df.select(countDistinct("customer_id")).collect()[0][0] # FILL IN: Get count of distinct customer_id values
    
    print(f"Total customers: {total_customers}")
    print(f"Unique customer IDs: {unique_customers}")
    
    # Step 2: Check if counts match
    if total_customers == unique_customers:
        print("✅ No duplicate customer IDs found")
    else:
        print("⚠️ Duplicate customer IDs detected")
    
    # Step 3: Age distribution
    # FILL IN: Get age statistics using .select("age").describe()
    age_stats = customers_df.select("age").describe()
    age_stats.show()
    
    # Step 4: Credit score distribution
    # FILL IN: Get credit_score statistics using .select("credit_score").describe()
    credit_stats = customers_df.select("credit_score").describe()
    credit_stats.show()
    
except Exception as e:
    print(f"❌ Error in customer validation: {str(e)}")

# END TODO 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Relationship Validation

# COMMAND ----------

# TODO 5: Validate Data Relationships
"""
OBJECTIVE: Check referential integrity between transactions and customers

COMPLETE THE TEMPLATE BELOW - includes join operations:
"""

print("Validating data relationships...")

try:
    # Step 1: Get unique customer IDs from each dataset
    transaction_customers = transactions_df.select("customer_id").distinct() # FILL IN: Select distinct customer_id from transactions_df
    customer_profiles = customers_df.select("customer_id").distinct() # FILL IN: Select distinct customer_id from customers_df
    
    # Step 2: Find customers in transactions that don't have profiles
    # This uses a left_anti join - returns rows from left that have NO match in right
    missing_profiles = transaction_customers.join(
        customer_profiles, 
        ["customer_id"],  # Join on customer_id column
        "left_anti"  # Keep rows from left that don't match right
    )
    missing_count = missing_profiles.count()
    
    # Step 3: Display counts and validation results
    print(f"Customers in transactions: {transaction_customers.count()}")
    print(f"Customers in profiles: {customer_profiles.count()}")
    print(f"Customers missing profiles: {missing_count}")
    
    # Step 4: Check if all customers have profiles
    if missing_count == 0:
        print("✅ All transaction customers have profiles")
    else:
        print("⚠️ Some transaction customers missing profiles")
        print("Missing customer IDs:")
        # FILL IN: Show the missing_profiles DataFrame
        missing_profiles.show()
    
    # Step 5: Check transaction date range
    print("\n📅 Transaction Date Range:")
    # FILL IN: Create date_range using this template:
    # date_range = transactions_df.select(
    #     min("transaction_date").alias("earliest_date"),
    #     max("transaction_date").alias("latest_date")
    # )
    date_range = date_range = transactions_df.select(
        min("transaction_date").alias("earliest_date"),
        max("transaction_date").alias("latest_date"))
    
    
    # FILL IN: Show the date_range
    date_range.show()
    
except Exception as e:
    print(f"❌ Error in relationship validation: {str(e)}")

# END TODO 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Temporary Views for Next Notebooks

# COMMAND ----------

# TODO 6: Create Temporary Views
"""
OBJECTIVE: Create temporary views for use in subsequent notebooks

COMPLETE THE TEMPLATE BELOW:
"""

print("Creating temporary views for next notebooks...")

try:
    # Step 1: Create temporary views for all DataFrames
    # FILL IN: Use .createOrReplaceTempView() for each DataFrame:
    
    # Create view for transactions_df named "transactions_raw"
    transactions_df.createOrReplaceTempView("transactions_raw")
    
    # Create view for customers_df named "customers_raw"
    customers_df.createOrReplaceTempView("customers_raw")
    
    # Create view for rules_df named "fraud_rules_raw"
    rules_df.createOrReplaceTempView("fraud_rules_raw")
    
    # Step 2: Print success message
    print("✅ Temporary views created successfully:")
    print("  - transactions_raw")
    print("  - customers_raw")
    print("  - fraud_rules_raw")
    
    # Step 3: Test the views with SQL queries
    print("\n🧪 Testing temporary views:")
    
    # Test transactions view
    # FILL IN: Use spark.sql() to count rows in transactions_raw
    result = spark.sql("SELECT COUNT(*) as transaction_count FROM transactions_raw")
    result.show()
    
    # Test customers view
    # FILL IN: Use spark.sql() to count rows in customers_raw
    result = spark.sql("SELECT COUNT(*) as customer_count FROM customers_raw")
    result.show()
    
    # Test rules view
    # FILL IN: Use spark.sql() to count rows in fraud_rules_raw
    result = spark.sql("SELECT COUNT(*) as rule_count FROM fraud_rules_raw")
    result.show()
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")

# END TODO 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Environment Setup Summary

# COMMAND ----------

# Final summary and checklist
print("=== ENVIRONMENT SETUP SUMMARY ===")
print()

# Checklist
checklist = [
    ("Spark connection tested", "✅"),
    ("Banking transactions loaded", "✅" if 'transactions_df' in locals() else "❌"),
    ("Customer profiles loaded", "✅" if 'customers_df' in locals() else "❌"),
    ("Fraud rules loaded", "✅" if 'fraud_rules' in locals() else "❌"),
    ("Data quality checks completed", "✅"),
    ("Temporary views created", "✅")
]

for item, status in checklist:
    print(f"{status} {item}")

print()
print("📊 DATA SUMMARY:")
try:
    print(f"  - Total transactions: {transactions_df.count()}")
    print(f"  - Total customers: {customers_df.count()}")
    print(f"  - Total fraud rules: {len(fraud_rules)}")
    
    # Calculate key metrics for next notebook
    fraud_transactions = transactions_df.filter(col("is_fraud") == 1).count()
    fraud_rate = fraud_transactions / transactions_df.count() * 100
    print(f"  - Fraud transactions: {fraud_transactions}")
    print(f"  - Fraud rate: {fraud_rate:.2f}%")
    
    # Merchant categories
    merchant_count = transactions_df.select("merchant_category").distinct().count()
    print(f"  - Merchant categories: {merchant_count}")
    
    # Transaction types
    transaction_types = transactions_df.select("transaction_type").distinct().count()
    print(f"  - Transaction types: {transaction_types}")
    
except Exception as e:
    print(f"  ❌ Error calculating summary: {str(e)}")

print()
print("🎯 NEXT STEPS:")
print("  1. Proceed to notebook 02-Transaction-Data-Processing.ipynb")
print("  2. Use the temporary views created in this notebook")
print("  3. Apply fraud detection rules to transaction data")
print("  4. Calculate risk scores for all transactions")

print()
print("✅ Environment setup completed successfully!")
print("📅 Setup completed at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC
# MAGIC **1. File Upload Issues:**
# MAGIC - Ensure CSV files are uploaded to the correct Databricks workspace location
# MAGIC - Check file names match exactly: `banking_transactions.csv`, `customer_profiles.csv`, `fraud_rules.json`
# MAGIC - Verify file permissions and accessibility
# MAGIC
# MAGIC **2. Schema Issues:**
# MAGIC - If inferSchema fails, manually define schema
# MAGIC - Check for special characters in column names
# MAGIC - Verify date formats in CSV files
# MAGIC
# MAGIC **3. Memory Issues:**
# MAGIC - If working with larger datasets, consider using `.cache()` on DataFrames
# MAGIC - Adjust Spark configuration if needed
# MAGIC
# MAGIC **4. JSON Parsing Issues:**
# MAGIC - Each line in fraud_rules.json should be a valid JSON object
# MAGIC - Check for proper JSON formatting
# MAGIC
# MAGIC **5. Performance Tips:**
# MAGIC - Use `.cache()` on frequently accessed DataFrames
# MAGIC - Consider partitioning large datasets
# MAGIC - Use appropriate Spark cluster size for your data volume
# MAGIC
# MAGIC ### TODO Completion Checklist:
# MAGIC - [ ] TODO 1: Transaction data loaded successfully (100 rows)
# MAGIC - [ ] TODO 2: Customer data loaded successfully (50 rows)  
# MAGIC - [ ] TODO 3: Fraud rules loaded successfully (4 rules)
# MAGIC - [ ] TODO 4: Data quality validation completed
# MAGIC - [ ] TODO 5: Data relationships validated
# MAGIC - [ ] TODO 6: Temporary views created and tested
# MAGIC
# MAGIC **When all TODOs are complete, you should have:**
# MAGIC - All data files successfully loaded with proper error handling
# MAGIC - Comprehensive data quality validation performed
# MAGIC - Data relationships verified between transactions and customers  
# MAGIC - Temporary views ready for next notebook
# MAGIC - Environment fully prepared for fraud detection processing