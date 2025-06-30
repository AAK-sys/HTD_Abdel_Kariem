# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Fraud Detection System - Transaction Data Processing
# MAGIC
# MAGIC **Week 6 Assessment - Part 2: Transaction Data Processing**
# MAGIC
# MAGIC This notebook applies fraud detection rules to transaction data and calculates risk scores.
# MAGIC
# MAGIC ## Objectives:
# MAGIC 1. Load processed data from the first notebook
# MAGIC 2. Apply fraud detection rules from JSON file
# MAGIC 3. Calculate risk scores (0-100 scale) for all transactions
# MAGIC 4. Flag high-risk transactions (score > 70)
# MAGIC 5. Join customer data with transaction data
# MAGIC 6. Create enriched dataset for analysis
# MAGIC
# MAGIC ## Success Criteria:
# MAGIC - All fraud rules applied correctly
# MAGIC - Risk scores calculated for 100 transactions
# MAGIC - High-risk transactions properly flagged
# MAGIC - Customer data successfully joined
# MAGIC - Summary analytics completed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Environment and Load Data

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd
from datetime import datetime, timedelta

print("Starting Transaction Data Processing...")
print(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Set data path
DATA_PATH = "/mnt/coursedata/"

# LOAD DATA FRESH (don't depend on temp views from other notebooks)
print("Loading data for fraud detection processing...")

try:
    # Load transactions
    transactions_df = spark.read.csv(
        f"{DATA_PATH}banking_transactions.csv", 
        header=True, 
        inferSchema=True,
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss"
    )
    
    # Load customers  
    customers_df = spark.read.csv(
        f"{DATA_PATH}customer_profiles.csv", 
        header=True, 
        inferSchema=True
    )
    
    # Load and parse fraud rules
    fraud_rules_text = spark.read.text(f"{DATA_PATH}fraud_rules.json").collect()
    
    fraud_rules = []
    for row in fraud_rules_text:
        rule_line = row.value.strip()
        if rule_line:
            try:
                rule = json.loads(rule_line)
                fraud_rules.append(rule)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse rule: {rule_line}")
    
    print("✅ Data loaded successfully for processing")
    print(f"  - Transactions: {transactions_df.count()} rows")
    print(f"  - Customers: {customers_df.count()} rows") 
    print(f"  - Fraud rules: {len(fraud_rules)} rules")
    
    # NOW we can create temp views for THIS notebook's use
    transactions_df.createOrReplaceTempView("current_transactions")
    customers_df.createOrReplaceTempView("current_customers")
    
except Exception as e:
    print(f"❌ Error loading data: {str(e)}")
    raise

# Continue with fraud detection processing...
print("\nReady to apply fraud detection rules!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load and Parse Fraud Detection Rules

# COMMAND ----------

# Load fraud rules from JSON file for detailed processing
print("Loading detailed fraud rules...")
DATA_PATH = "/mnt/coursedata/"

try:
    # Read the JSON file again for detailed rule processing
    fraud_rules_text = spark.read.text(f"{DATA_PATH}fraud_rules.json").collect()
    
    # Parse JSON rules
    fraud_rules = []
    for row in fraud_rules_text:
        rule_line = row.value.strip()
        if rule_line:  # Skip empty lines
            try:
                rule = json.loads(rule_line)
                fraud_rules.append(rule)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse line: {rule_line}")
    
    print(f"✅ Loaded {len(fraud_rules)} detailed fraud rules")
    
    # Display rule details for processing
    for rule in fraud_rules:
        print(f"\nRule: {rule.get('rule_name')}")
        print(f"  - ID: {rule.get('rule_id')}")
        print(f"  - Priority: {rule.get('priority')}")
        print(f"  - Risk Score: {rule.get('thresholds', {}).get('risk_score')}")
        print(f"  - Active: {rule.get('active')}")
        
except Exception as e:
    print(f"⚠️ Error loading detailed rules: {str(e)}")
    print("Using simplified rules for processing...")
    
    # Create simplified rules if detailed loading fails
    fraud_rules = [
        {
            'rule_id': 'HIGH_AMOUNT_SINGLE',
            'rule_name': 'High Amount Single Transaction',
            'priority': 'HIGH',
            'thresholds': {'risk_score': 75},
            'active': True,
            'conditions': {'amount': {'operator': 'greater_than', 'value': 1000}}
        },
        {
            'rule_id': 'VELOCITY_MULTIPLE_TRANSACTIONS',
            'rule_name': 'Transaction Velocity Check',
            'priority': 'MEDIUM',
            'thresholds': {'risk_score': 60},
            'active': True,
            'conditions': {'transaction_count': {'operator': 'greater_than', 'value': 5}}
        },
        {
            'rule_id': 'MERCHANT_CATEGORY_RISK',
            'rule_name': 'High Risk Merchant Categories',
            'priority': 'MEDIUM',
            'thresholds': {'risk_score': 50},
            'active': True,
            'conditions': {'merchant_category': {'operator': 'in', 'value': ['GAMBLING', 'CRYPTOCURRENCY']}}
        }
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Fraud Detection Rules and Calculate Risk Scores

# COMMAND ----------

# TODO 1: Apply Individual Fraud Detection Rules
"""
OBJECTIVE: Apply each fraud detection rule and calculate individual risk scores

COMPLETE THE TEMPLATE BELOW:
You need to implement the business logic for each fraud detection rule.
"""

print("Applying fraud detection rules...")

# Start with the base transaction data
processed_transactions = transactions_df

# Initialize risk score column
processed_transactions = processed_transactions.withColumn("risk_score", lit(0))
processed_transactions = processed_transactions.withColumn("triggered_rules", array())

print("📊 Applying individual fraud rules...")

# RULE 1: High Amount Single Transaction
print("\n🔍 Applying Rule 1: High Amount Single Transaction")
high_amount_threshold = 1000  # Transactions over $1000

# TODO: Implement high amount rule
# FILL IN: Use .withColumn() and when() to create "high_amount_risk" column
# - If amount > high_amount_threshold, set risk to 75
# - Otherwise, set risk to 0
# Pattern: .withColumn("high_amount_risk", when(condition, value).otherwise(0))

processed_transactions = processed_transactions.withColumn(
    "high_amount_risk",
    when(col("amount") > high_amount_threshold, 75).otherwise(0)
)

# Count and display results
high_amount_count = processed_transactions.filter(col("high_amount_risk") > 0).count()
print(f"  - High amount transactions (>${high_amount_threshold}): {high_amount_count}")

# RULE 2: Transaction Velocity Check
print("\n🔍 Applying Rule 2: Transaction Velocity Check")

# TODO: Calculate transaction frequency per customer
# FILL IN: Use .groupBy("customer_id") and .agg() to calculate:
# - count("*").alias("transaction_count")
# - sum("amount").alias("total_amount")

customer_transaction_counts = processed_transactions.groupBy("customer_id").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount")
)

# TODO: Join transaction counts back to main dataset
# FILL IN: Use .join() with customer_transaction_counts on "customer_id" with "left" join
processed_transactions = processed_transactions.join(
    customer_transaction_counts, ["customer_id"], "left"
)

# TODO: Apply velocity rule
# FILL IN: Create "velocity_risk" column
# - If transaction_count > 3, set risk to 60
# - Otherwise, set risk to 0
processed_transactions = processed_transactions.withColumn(
    "velocity_risk",
    when(col("transaction_count") > 3, 60).otherwise(0)
)

velocity_count = processed_transactions.filter(col("velocity_risk") > 0).distinct().count()
print(f"  - High velocity customers (>3 transactions): {velocity_count}")

# RULE 3: Merchant Category Risk
print("\n🔍 Applying Rule 3: Merchant Category Risk")
high_risk_categories = ["GAMBLING", "CRYPTOCURRENCY", "ADULT_ENTERTAINMENT", "ONLINE"]

# TODO: Apply merchant category rule
# FILL IN: Create "merchant_risk" column
# - If merchant_category is in high_risk_categories, set risk to 50
# - Otherwise, set risk to 0
# Hint: Use .isin(high_risk_categories) to check if value is in list
processed_transactions = processed_transactions.withColumn(
    "merchant_risk",
    when(col("merchant_category").isin(high_risk_categories), 50).otherwise(0)
)

merchant_risk_count = processed_transactions.filter(col("merchant_risk") > 0).count()
print(f"  - High risk merchant transactions: {merchant_risk_count}")

# RULE 4: Geographic/Location Risk
print("\n🔍 Applying Rule 4: Geographic Risk")
high_risk_locations = ["INTERNATIONAL", "UNKNOWN", "ONLINE"]

# TODO: Apply location risk rule
# FILL IN: Create "location_risk" column
# - If location is in high_risk_locations, set risk to 40
# - Otherwise, set risk to 0
processed_transactions = processed_transactions.withColumn(
    "location_risk",
    when(col("location").isin(high_risk_locations), 40).otherwise(0)
)

location_risk_count = processed_transactions.filter(col("location_risk") > 0).count()
print(f"  - High risk location transactions: {location_risk_count}")

# END TODO 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate Final Risk Scores

# COMMAND ----------

# TODO 2: Calculate Composite Risk Score
"""
OBJECTIVE: Combine all individual risk scores into a final weighted risk score

COMPLETE THE TEMPLATE BELOW:
You need to create a weighted average of all risk factors.
"""

print("Calculating composite risk scores...")

# TODO: Calculate weighted composite risk score
# FILL IN: Use .withColumn() to create "risk_score" column
# Combine risk factors with these weights:
# - high_amount_risk * 0.4 (40% weight)
# - velocity_risk * 0.25 (25% weight)  
# - merchant_risk * 0.2 (20% weight)
# - location_risk * 0.15 (15% weight)
# Use least(lit(100), calculation) to cap at 100
# Cast result to integer: .cast("integer")

processed_transactions = processed_transactions.withColumn(
    "risk_score",
    least(lit(100), 
          (col("high_amount_risk") * 0.4 + 
           col("velocity_risk") * 0.25 + 
           col("merchant_risk") * 0.2 + 
           col("location_risk") * 0.15
          ).cast("integer")
    )
)

# TODO: Create risk categories
# FILL IN: Use .withColumn() and when() to create "risk_category" column
# - If risk_score >= 70, category is "HIGH"
# - If risk_score >= 40, category is "MEDIUM"  
# - Otherwise, category is "LOW"
processed_transactions = processed_transactions.withColumn(
    "risk_category",
    when(col("risk_score") >= 70, "HIGH")
    .when(col("risk_score") >= 40, "MEDIUM")
    .otherwise("LOW")
)

# TODO: Flag high-risk transactions
# FILL IN: Create "is_high_risk" column
# - If risk_score > 70, flag as 1
# - Otherwise, flag as 0
processed_transactions = processed_transactions.withColumn(
    "is_high_risk",
    when(col("risk_score") > 70, 1).otherwise(0)
)

# Calculate and display risk distribution
print("\n📊 Risk Score Distribution:")
risk_distribution = processed_transactions.groupBy("risk_category").agg(
    count("*").alias("transaction_count"),
    round(avg("risk_score"), 2).alias("avg_risk_score")
).orderBy(col("transaction_count").desc())

risk_distribution.show()

# Calculate summary statistics
high_risk_count = processed_transactions.filter(col("is_high_risk") == 1).count()
total_transactions = processed_transactions.count()
high_risk_percentage = (high_risk_count / total_transactions) * 100

print(f"✅ Risk scoring completed:")
print(f"  - Total transactions processed: {total_transactions}")
print(f"  - High-risk transactions (score > 70): {high_risk_count}")
print(f"  - High-risk percentage: {high_risk_percentage:.2f}%")

# END TODO 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Join Customer Data for Enrichment

# COMMAND ----------

# TODO 3: Join Customer Profile Data
"""
OBJECTIVE: Enrich transaction data with customer demographics

COMPLETE THE TEMPLATE BELOW:
You need to perform a left join to add customer information to transactions.
"""

print("Enriching transaction data with customer profiles...")

try:
    # TODO: Perform left join to add customer information
    # FILL IN: Use .join() to combine processed_transactions with customers_df
    # - Join on ["customer_id"] 
    # - Use "left" join type to keep all transactions
    enriched_transactions = processed_transactions.join(
        customers_df, ["customer_id"], "left"
    )
    
    print("✅ Customer data joined successfully")
    
    # Verify join results
    enriched_count = enriched_transactions.count()
    original_count = processed_transactions.count()
    
    print(f"  - Original transactions: {original_count}")
    print(f"  - Enriched transactions: {enriched_count}")
    
    # TODO: Check join success
    # FILL IN: Add logic to verify if enriched_count equals original_count
    if enriched_count == original_count:
        print("✅ All transactions have customer data")
    else:
        print("⚠️ Some transactions missing customer data")
    
    # Display sample of enriched data
    print("\n📊 Sample Enriched Transaction Data:")
    enriched_transactions.select(
        "transaction_id", "customer_id", "customer_name", "amount", 
        "risk_score", "risk_category", "age", "credit_score"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Error joining customer data: {str(e)}")
    enriched_transactions = processed_transactions

# END TODO 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Enhanced Risk Analysis with Customer Data

# COMMAND ----------

# TODO 4: Enhanced Risk Analysis with Customer Demographics
"""
OBJECTIVE: Incorporate customer demographics into risk analysis

COMPLETE THE TEMPLATE BELOW:
You need to add customer-based risk factors and recalculate final scores.
"""

print("Performing enhanced risk analysis with customer data...")

# TODO: Add customer-based risk factors
# FILL IN: Create "customer_risk_factor" column using when() conditions:
# - If credit_score < 600, add 20 points
# - If age < 25, add 15 points  
# - If previous_fraud_incidents > 0, add 30 points
# - Otherwise, add 0 points
# You can chain .when() conditions together

enriched_transactions = enriched_transactions.withColumn(
    "customer_risk_factor",
    when(col("credit_score") < 600, 20)
    .when(col("age") < 25, 15)
    .when(col("previous_fraud_incidents") > 0, 30)
    .otherwise(0)
)

# TODO: Recalculate risk score with customer factors
# FILL IN: Create "final_risk_score" column
# Add customer_risk_factor to the original risk_score
# Use least(lit(100), calculation) to cap at 100
enriched_transactions = enriched_transactions.withColumn(
    "final_risk_score",
    least(lit(100), col("risk_score") + col("customer_risk_factor"))
)

# TODO: Update risk category based on final score
# FILL IN: Create "final_risk_category" using same logic as before
# - If final_risk_score >= 70, category is "HIGH"
# - If final_risk_score >= 40, category is "MEDIUM"
# - Otherwise, category is "LOW"
enriched_transactions = enriched_transactions.withColumn(
    "final_risk_category",
    when(col("final_risk_score") >= 70, "HIGH")
    .when(col("final_risk_score") >= 40, "MEDIUM")
    .otherwise("LOW")
)

# TODO: Update high-risk flag
# FILL IN: Create "is_final_high_risk" column
# - If final_risk_score > 70, flag as 1
# - Otherwise, flag as 0
enriched_transactions = enriched_transactions.withColumn(
    "is_final_high_risk",
    when(col("final_risk_score") > 70, 1).otherwise(0)
)

# Analyze final risk distribution
print("\n📊 Final Risk Distribution:")
final_risk_distribution = enriched_transactions.groupBy("final_risk_category").agg(
    count("*").alias("transaction_count"),
    round(avg("final_risk_score"), 2).alias("avg_final_risk_score")
).orderBy(col("transaction_count").desc())

final_risk_distribution.show()

# Calculate final statistics
final_high_risk_count = enriched_transactions.filter(col("is_final_high_risk") == 1).count()
final_high_risk_percentage = (final_high_risk_count / enriched_count) * 100

print(f"✅ Enhanced risk analysis completed:")
print(f"  - Final high-risk transactions: {final_high_risk_count}")
print(f"  - Final high-risk percentage: {final_high_risk_percentage:.2f}%")

# END TODO 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Identify High-Risk Customers and Patterns

# COMMAND ----------

# TODO 5: Analyze High-Risk Patterns
"""
OBJECTIVE: Identify high-risk customers, merchants, and geographic patterns

COMPLETE THE TEMPLATE BELOW:
You need to perform groupBy operations to analyze fraud patterns.
"""

print("Identifying high-risk customers and fraud patterns...")

# TODO: High-risk customers analysis
# FILL IN: Filter for high-risk transactions and group by customer information
# Use .filter() with col("is_final_high_risk") == 1
# Then .groupBy() customer fields and .agg() to calculate metrics
high_risk_customers = enriched_transactions.filter(col("is_final_high_risk") == 1
                                                  ).groupBy(
    # FILL IN: List of columns to group by
    # "customer_id", "customer_name", "age", "credit_score", "account_type"
).agg(
    # FILL IN: Aggregations to calculate
   count("*").alias("high_risk_transaction_count"),
   sum("amount").alias("total_high_risk_amount"),
   round(avg("final_risk_score"), 2).alias("avg_risk_score"),
   max("final_risk_score").alias("max_risk_score")
).orderBy(col("total_high_risk_amount").desc())

print("\n🚨 Top 10 High-Risk Customers:")
high_risk_customers.show(10, truncate=False)

# TODO: High-risk merchant analysis
# FILL IN: Similar pattern for merchant analysis
# Filter high-risk transactions and group by merchant_category
high_risk_merchants = enriched_transactions.filter( col("is_final_high_risk") == 1 
                                                  ).groupBy(
    # FILL IN: "merchant_category"
).agg(
   count("*").alias("high_risk_count"),
   sum("amount").alias("total_amount"),
   round(avg("final_risk_score"), 2).alias("avg_risk_score")
).orderBy(col("high_risk_count").desc())

print("\n🏪 High-Risk Merchant Categories:")
high_risk_merchants.show()

# TODO: Geographic risk analysis
# FILL IN: Similar pattern for location analysis
# Filter high-risk transactions and group by location
geographic_risk = enriched_transactions.filter(col("is_final_high_risk") == 1
                                              ).groupBy(
    # FILL IN: "location"
).agg(
   count("*").alias("high_risk_count"),
   sum("amount").alias("total_amount")
).orderBy(col("high_risk_count").desc())

print("\n🌍 Geographic Risk Distribution:")
geographic_risk.show()

# END TODO 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Summary Reports

# COMMAND ----------

# Create comprehensive summary reports
print("Creating summary reports...")

# Overall fraud detection summary
total_amount = enriched_transactions.agg(sum("amount")).collect()[0][0]
high_risk_amount = enriched_transactions.filter(col("is_final_high_risk") == 1).agg(sum("amount")).collect()[0][0]
actual_fraud_count = enriched_transactions.filter(col("is_fraud") == 1).count()
predicted_high_risk_count = enriched_transactions.filter(col("is_final_high_risk") == 1).count()

# True positive analysis (if we have actual fraud labels)
if actual_fraud_count > 0:
    true_positives = enriched_transactions.filter(
        (col("is_fraud") == 1) & (col("is_final_high_risk") == 1)
    ).count()
    
    false_positives = enriched_transactions.filter(
        (col("is_fraud") == 0) & (col("is_final_high_risk") == 1)
    ).count()
    
    precision = true_positives / predicted_high_risk_count if predicted_high_risk_count > 0 else 0
    recall = true_positives / actual_fraud_count if actual_fraud_count > 0 else 0
    
    print("\n📈 Model Performance Metrics:")
    print(f"  - True Positives: {true_positives}")
    print(f"  - False Positives: {false_positives}")
    print(f"  - Precision: {precision:.3f}")
    print(f"  - Recall: {recall:.3f}")

print("\n📊 FRAUD DETECTION SUMMARY:")
print(f"  - Total Transactions: {enriched_count:,}")
print(f"  - Total Transaction Amount: ${total_amount:,.2f}")
print(f"  - High-Risk Transactions: {final_high_risk_count:,}")
print(f"  - High-Risk Amount: ${high_risk_amount:,.2f}")
print(f"  - High-Risk Percentage: {final_high_risk_percentage:.2f}%")
print(f"  - Actual Fraud Transactions: {actual_fraud_count}")

# Age group risk analysis
age_risk_analysis = enriched_transactions.withColumn(
    "age_group",
    when(col("age") < 25, "18-24")
    .when(col("age") < 35, "25-34")
    .when(col("age") < 45, "35-44")
    .when(col("age") < 55, "45-54")
    .when(col("age") < 65, "55-64")
    .otherwise("65+")
).groupBy("age_group").agg(
    count("*").alias("total_transactions"),
    sum(col("is_final_high_risk")).alias("high_risk_transactions"),
    round(avg("final_risk_score"), 2).alias("avg_risk_score")
).withColumn(
    "risk_percentage",
    round((col("high_risk_transactions") / col("total_transactions") * 100), 2)
).orderBy("age_group")

print("\n👥 Risk Analysis by Age Group:")
age_risk_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Temporary Views for Next Notebook

# COMMAND ----------

# Create temporary views for the next notebook
print("Creating temporary views for analytics notebook...")

try:
    # Create the main enriched transactions view
    enriched_transactions.createOrReplaceTempView("enriched_transactions")
    
    # Create high-risk customers view
    high_risk_customers.createOrReplaceTempView("high_risk_customers")
    
    # Create merchant risk analysis view
    high_risk_merchants.createOrReplaceTempView("high_risk_merchants")
    
    # Create geographic risk view
    geographic_risk.createOrReplaceTempView("geographic_risk")
    
    # Create summary statistics view
    summary_stats = spark.createDataFrame([
        (enriched_count, final_high_risk_count, final_high_risk_percentage, 
         float(total_amount), float(high_risk_amount), actual_fraud_count)
    ], ["total_transactions", "high_risk_transactions", "high_risk_percentage", 
        "total_amount", "high_risk_amount", "actual_fraud_count"])
    
    summary_stats.createOrReplaceTempView("fraud_summary_stats")
    
    print("✅ Temporary views created successfully:")
    print("  - enriched_transactions")
    print("  - high_risk_customers") 
    print("  - high_risk_merchants")
    print("  - geographic_risk")
    print("  - fraud_summary_stats")
    
    # Test the views
    print("\n🧪 Testing temporary views:")
    print("Enriched transactions count:", spark.sql("SELECT COUNT(*) FROM enriched_transactions").collect()[0][0])
    print("High-risk customers count:", spark.sql("SELECT COUNT(*) FROM high_risk_customers").collect()[0][0])
    
except Exception as e:
    print(f"❌ Error creating temporary views: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Data Processing Summary

# COMMAND ----------

# Final processing summary and validation
print("=== TRANSACTION DATA PROCESSING SUMMARY ===")
print()

# Processing checklist
processing_checklist = [
    ("Data loaded from previous notebook", "✅"),
    ("Fraud rules applied successfully", "✅"),
    ("Risk scores calculated (0-100)", "✅"),
    ("High-risk transactions flagged", "✅"),
    ("Customer data joined", "✅" if 'enriched_transactions' in locals() else "❌"),
    ("Enhanced risk analysis completed", "✅"),
    ("Summary reports generated", "✅"),
    ("Temporary views created", "✅")
]

for item, status in processing_checklist:
    print(f"{status} {item}")

print()
print("📊 PROCESSING RESULTS:")
print(f"  - Transactions processed: {enriched_count:,}")
print(f"  - Risk scores calculated: {enriched_count:,}")
print(f"  - High-risk transactions identified: {final_high_risk_count:,}")
print(f"  - High-risk customers identified: {high_risk_customers.count():,}")
print(f"  - Risk categories created: LOW, MEDIUM, HIGH")

print()
print("🔍 KEY FINDINGS:")
print(f"  - Overall fraud detection rate: {final_high_risk_percentage:.2f}%")
print(f"  - Total amount at risk: ${high_risk_amount:,.2f}")
print(f"  - Top risk factors: High amounts, velocity, merchant category")

# Display sample of final processed data
print("\n📋 Sample Final Processed Data:")
enriched_transactions.select(
    "transaction_id", "customer_name", "amount", "merchant_category",
    "final_risk_score", "final_risk_category", "is_final_high_risk"
).orderBy(col("final_risk_score").desc()).show(10, truncate=False)

print()
print("🎯 NEXT STEPS:")
print("  1. Proceed to notebook 03-Risk-Analytics.ipynb")
print("  2. Use the enriched transaction data for detailed analysis")
print("  3. Create executive summary reports")
print("  4. Export data for Power BI dashboard")

print()
print("✅ Transaction data processing completed successfully!")
print("📅 Processing completed at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Quality Checks

# COMMAND ----------

# Perform final validation and quality checks
print("Performing final validation checks...")

# Check 1: Ensure all transactions have risk scores
null_risk_scores = enriched_transactions.filter(col("final_risk_score").isNull()).count()
print(f"Transactions with null risk scores: {null_risk_scores}")

# Check 2: Verify risk score ranges
risk_score_range = enriched_transactions.select(
    min("final_risk_score").alias("min_score"),
    max("final_risk_score").alias("max_score"),
    avg("final_risk_score").alias("avg_score")
)
risk_score_range.show()

# Check 3: Verify risk categories are consistent
category_score_check = enriched_transactions.groupBy("final_risk_category").agg(
    min("final_risk_score").alias("min_score"),
    max("final_risk_score").alias("max_score")
).orderBy("final_risk_category")

print("Risk category score ranges:")
category_score_check.show()

# Check 4: Ensure customer data is properly joined
customers_with_nulls = enriched_transactions.filter(col("customer_name").isNull()).count()
print(f"Transactions with missing customer names: {customers_with_nulls}")

if null_risk_scores == 0 and customers_with_nulls == 0:
    print("✅ All validation checks passed!")
else:
    print("⚠️ Some validation issues found - please review")

print("\n🎊 Transaction processing notebook completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions:
# MAGIC
# MAGIC **1. Risk Score Calculation Issues:**
# MAGIC - Ensure all numeric columns are properly cast
# MAGIC - Check for null values in amount and customer data
# MAGIC - Verify risk score logic produces values between 0-100
# MAGIC
# MAGIC **2. Join Problems:**
# MAGIC - Verify customer_id exists in both datasets
# MAGIC - Check for data type mismatches in join keys
# MAGIC - Use broadcast joins for small customer datasets
# MAGIC
# MAGIC **3. Performance Issues:**
# MAGIC - Cache enriched_transactions DataFrame if running multiple analyses
# MAGIC - Consider partitioning by customer_id for large datasets
# MAGIC - Use appropriate cluster size for data volume
# MAGIC
# MAGIC **4. Rule Application Issues:**
# MAGIC - Verify fraud rules JSON is properly formatted
# MAGIC - Check that rule conditions match actual data values
# MAGIC - Test rules individually before combining
# MAGIC
# MAGIC **5. Memory Issues:**
# MAGIC - Use `.cache()` strategically on intermediate results
# MAGIC - Process data in batches if memory is limited
# MAGIC - Optimize Spark configuration for your cluster
# MAGIC
# MAGIC ### TODO Completion Checklist:
# MAGIC - [ ] TODO 1: Individual fraud rules applied successfully
# MAGIC - [ ] TODO 2: Composite risk scores calculated correctly
# MAGIC - [ ] TODO 3: Customer data joined without data loss
# MAGIC - [ ] TODO 4: Enhanced risk analysis with demographics completed
# MAGIC - [ ] TODO 5: High-risk pattern analysis completed
# MAGIC
# MAGIC **When all TODOs are complete, you should have:**
# MAGIC - All transactions with calculated risk scores (0-100)
# MAGIC - High-risk transactions properly flagged (score > 70)
# MAGIC - Customer demographics integrated into risk analysis
# MAGIC - Comprehensive fraud pattern analysis completed
# MAGIC - Data ready for executive reporting and Power BI dashboard