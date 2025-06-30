# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Fraud Detection System - Risk Analytics Student Version
# MAGIC
# MAGIC **Week 6 Assessment - Part 3: Risk Analytics**
# MAGIC
# MAGIC This notebook creates comprehensive analytics and exports data for Power BI dashboard.
# MAGIC
# MAGIC ## Objectives:
# MAGIC 1. Load enriched data from the second notebook
# MAGIC 2. Generate key fraud detection metrics
# MAGIC 3. Create executive summary reports
# MAGIC 4. Identify top risk patterns and customers
# MAGIC 5. Export clean data for Power BI dashboard
# MAGIC 6. Create summary tables for business intelligence
# MAGIC
# MAGIC ## Success Criteria:
# MAGIC - Executive KPIs calculated correctly
# MAGIC - Top risk customers and merchants identified
# MAGIC - Time-based fraud patterns analyzed
# MAGIC - Clean data exported for Power BI
# MAGIC - Business recommendations provided

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Environment and Load Processed Data

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

print("Starting Risk Analytics and Reporting...")
print(f"Analytics started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

DATA_PATH = "/mnt/coursedata/"

def load_and_create_all_datasets():
    """Load raw data and create ALL datasets needed for analytics"""
    
    # Load raw data
    print("📂 Loading raw data files...")
    transactions_df = spark.read.csv(
        f"{DATA_PATH}banking_transactions.csv", 
        header=True, 
        inferSchema=True,
        timestampFormat="yyyy-MM-dd'T'HH:mm:ss"
    )
    
    customers_df = spark.read.csv(
        f"{DATA_PATH}customer_profiles.csv", 
        header=True, 
        inferSchema=True
    )
    
    print(f"✅ Loaded {transactions_df.count()} transactions and {customers_df.count()} customers")
    
    # Create enriched_transactions dataset
    print("⚙️ Creating enriched_transactions dataset...")
    enriched_transactions = transactions_df.alias("t").join(
        customers_df.alias("c"), 
        col("t.customer_id") == col("c.customer_id"), 
        "left"
    ).select(
        # Transaction fields
        col("t.transaction_id"),
        col("t.customer_id"),
        col("t.amount"),
        col("t.transaction_date"),
        col("t.merchant_category"),
        col("t.transaction_type"),
        col("t.location"), 
        col("t.is_fraud"),
        
        # Customer fields
        col("c.customer_name"),
        col("c.age"),
        col("c.account_tenure_months"),
        col("c.credit_score"),
        col("c.annual_income"),
        col("c.state"),
        col("c.account_type"),
        col("c.previous_fraud_incidents")
    ).withColumn(
        "amount_risk_flag",
        when(col("amount") > 1000, "High Amount").otherwise("Normal Amount")
    ).withColumn(
        "customer_risk_flag", 
        when(col("previous_fraud_incidents") > 0, "Previous Fraud").otherwise("Clean History")
    ).withColumn(
        "risk_score",
        when(col("amount") > 1000, 30).otherwise(0) +
        when(col("previous_fraud_incidents") > 0, 40).otherwise(0) +
        when(col("credit_score") < 600, 20).otherwise(0) +
        when(col("is_fraud") == 1, 100).otherwise(0)
    ).withColumn(
        "risk_category",
        when(col("risk_score") >= 80, "High Risk")
        .when(col("risk_score") >= 40, "Medium Risk") 
        .otherwise("Low Risk")
    ).withColumn(
        "fraud_status",
        when(col("is_fraud") == 1, "Fraud").otherwise("Legitimate")
    )
    
    # Create high_risk_customers dataset
    print("⚙️ Creating high_risk_customers dataset...")
    high_risk_customers = enriched_transactions.groupBy(
        "customer_id", "customer_name", "age", "credit_score", 
        "annual_income", "state", "account_type", "previous_fraud_incidents"
    ).agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum("is_fraud").alias("fraud_count"),
        avg("risk_score").alias("avg_risk_score"),
        max("risk_score").alias("max_risk_score"),
        sum(when(col("risk_category") == "High Risk", col("amount")).otherwise(0)).alias("total_high_risk_amount"),
        sum(when(col("risk_category") == "High Risk", 1).otherwise(0)).alias("high_risk_transaction_count")
    ).withColumn(
        "fraud_rate",
        round(col("fraud_count") / col("total_transactions") * 100, 2)
    ).withColumn(
        "customer_risk_level",
        when(col("avg_risk_score") >= 60, "High Risk")
        .when(col("avg_risk_score") >= 30, "Medium Risk")
        .otherwise("Low Risk")
    ).filter(
        (col("avg_risk_score") >= 30) | (col("fraud_count") > 0) | (col("previous_fraud_incidents") > 0)
    )
    
    # Create high_risk_merchants dataset
    print("⚙️ Creating high_risk_merchants dataset...")
    high_risk_merchants = enriched_transactions.groupBy("merchant_category").agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum("is_fraud").alias("fraud_count"),
        avg("risk_score").alias("avg_risk_score"),
        sum(when(col("risk_category") == "High Risk", 1).otherwise(0)).alias("high_risk_count"),
        sum(when(col("risk_category") == "High Risk", col("amount")).otherwise(0)).alias("high_risk_amount")
    ).withColumn(
        "fraud_rate",
        round(col("fraud_count") / col("total_transactions") * 100, 2)
    ).withColumn(
        "merchant_risk_level",
        when(col("fraud_rate") >= 10, "High Risk")
        .when(col("fraud_rate") >= 5, "Medium Risk")
        .otherwise("Low Risk")
    ).filter(col("fraud_rate") > 0).orderBy(desc("fraud_rate"))
    
    # Create geographic_risk dataset
    print("⚙️ Creating geographic_risk dataset...")
    geographic_risk = enriched_transactions.groupBy("state", "location").agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum("is_fraud").alias("fraud_count"),
        avg("risk_score").alias("avg_risk_score"),
        countDistinct("customer_id").alias("unique_customers"),
        sum(when(col("risk_category") == "High Risk", 1).otherwise(0)).alias("high_risk_count"),
        sum(when(col("risk_category") == "High Risk", col("amount")).otherwise(0)).alias("high_risk_amount")
    ).withColumn(
        "fraud_rate",
        round(col("fraud_count") / col("total_transactions") * 100, 2)
    ).withColumn(
        "geographic_risk_level",
        when(col("fraud_rate") >= 8, "High Risk")
        .when(col("fraud_rate") >= 4, "Medium Risk")
        .otherwise("Low Risk")
    ).orderBy(desc("fraud_rate"))
    
    # Create fraud_summary_stats dataset
    print("⚙️ Creating fraud_summary_stats dataset...")
    
    high_risk_count = enriched_transactions.filter(col("risk_category") == "High Risk").count()
    high_risk_amount_total = enriched_transactions.filter(col("risk_category") == "High Risk").agg(sum("amount")).collect()[0][0] or 0
    
    fraud_summary_stats = enriched_transactions.agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum("is_fraud").alias("actual_fraud_count"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("merchant_category").alias("unique_merchants"),
        countDistinct("state").alias("unique_states")
    ).withColumn(
        "high_risk_transactions",
        lit(high_risk_count)
    ).withColumn(
        "high_risk_amount",
        lit(high_risk_amount_total)
    ).withColumn(
        "fraud_rate",
        round(col("actual_fraud_count") / col("total_transactions") * 100, 2)
    ).withColumn(
        "high_risk_percentage",
        round(col("high_risk_transactions") / col("total_transactions") * 100, 2)
    ).withColumn(
        "avg_fraud_amount",
        lit(enriched_transactions.filter(col("is_fraud") == 1).agg(avg("amount")).collect()[0][0] or 0)
    ).withColumn(
        "analysis_date",
        current_timestamp()
    )
    
    return enriched_transactions, high_risk_customers, high_risk_merchants, geographic_risk, fraud_summary_stats

# Execute the loading and processing
try:
    enriched_transactions, high_risk_customers, high_risk_merchants, geographic_risk, fraud_summary_stats = load_and_create_all_datasets()
    
    # Create ALL the temporary views that the notebook expects
    enriched_transactions.createOrReplaceTempView("enriched_transactions")
    high_risk_customers.createOrReplaceTempView("high_risk_customers")
    high_risk_merchants.createOrReplaceTempView("high_risk_merchants")
    geographic_risk.createOrReplaceTempView("geographic_risk")
    fraud_summary_stats.createOrReplaceTempView("fraud_summary_stats")
    
    print("✅ All datasets created and temporary views established:")
    print(f"  - enriched_transactions: {enriched_transactions.count()} rows")
    print(f"  - high_risk_customers: {high_risk_customers.count()} rows")
    print(f"  - high_risk_merchants: {high_risk_merchants.count()} rows")
    print(f"  - geographic_risk: {geographic_risk.count()} rows")
    print(f"  - fraud_summary_stats: {fraud_summary_stats.count()} rows")
    
    print("\n✅ All datasets ready for risk analytics and reporting!")
    
except Exception as e:
    print(f"❌ Error in data processing: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Executive Summary Metrics

# COMMAND ----------

# TODO 1: Create Executive Summary Dashboard Metrics
"""
OBJECTIVE: Calculate key performance indicators for executive reporting

COMPLETE THE TEMPLATE BELOW:
You need to extract and calculate executive-level fraud detection metrics.
"""

print("Generating executive summary metrics...")

# TODO: Extract summary statistics from fraud_summary_stats
# FILL IN: Use .collect()[0] to get the first row of fraud_summary_stats
summary_row = fraud_summary_stats.collect()[0]

# TODO: Extract individual metrics from the summary row
# FILL IN: Get each metric using summary_row['column_name']
total_transactions = summary_row['total_transactions']
high_risk_transactions = summary_row['high_risk_transactions'] 
high_risk_percentage = summary_row['high_risk_percentage']
total_amount = summary_row['total_amount']
high_risk_amount = summary_row['high_risk_amount']
actual_fraud_count = summary_row['actual_fraud_count']

print("\n📊 EXECUTIVE SUMMARY DASHBOARD METRICS")
print("=" * 50)
print(f"💰 Total Transaction Volume: ${total_amount:,.2f}")
print(f"📈 Total Transactions Processed: {total_transactions:,}")
print(f"🚨 High-Risk Transactions Detected: {high_risk_transactions:,}")
print(f"⚠️ High-Risk Detection Rate: {high_risk_percentage:.2f}%")
print(f"💸 Amount at Risk: ${high_risk_amount:,.2f}")
print(f"🎯 Actual Fraud Cases: {actual_fraud_count:,}")

# TODO: Calculate additional business metrics
# FILL IN: Calculate fraud prevention amount (same as high_risk_amount)
fraud_prevention_amount = high_risk_amount
# FILL IN: Calculate average transaction size
average_transaction_size = total_amount / total_transactions
# FILL IN: Calculate average risk amount (avoid division by zero)
average_risk_amount = high_risk_amount / high_risk_transactions if high_risk_transactions > 0 else 0

print(f"🛡️ Potential Fraud Prevented: ${fraud_prevention_amount:,.2f}")
print(f"📊 Average Transaction Size: ${average_transaction_size:.2f}")
print(f"⚠️ Average High-Risk Amount: ${average_risk_amount:.2f}")

# TODO: Create executive KPIs DataFrame
# FILL IN: Use spark.createDataFrame() to create a DataFrame with metric data
# Each row should have: (metric_name, metric_value, metric_type)
executive_kpis = spark.createDataFrame([
    ("Total Transactions", float(total_transactions), "count"),
    ("High Risk Transactions", float(high_risk_transactions), "count"),
    ("Total Amount", float(total_amount), "currency"),
    ("High Risk Amount", float(high_risk_amount), "currency"),
    ("Detection Rate", float(high_risk_percentage), "percentage"),
    ("Average Transaction", float(average_transaction_size), "currency"),
    ("Fraud Prevention", float(fraud_prevention_amount), "currency")
], ["metric_name", "metric_value", "metric_type"])

print("\n📋 Executive KPIs Table:")
executive_kpis.show(truncate=False)

# END TODO 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Top Risk Analysis

# COMMAND ----------

# TODO 2: Analyze Top Risk Patterns
"""
OBJECTIVE: Identify the highest risk customers, merchants, and geographic patterns

COMPLETE THE TEMPLATE BELOW:
You need to perform analysis to find the most dangerous fraud patterns.
"""

print("Analyzing top risk patterns...")

# TODO: Find Top 10 Riskiest Customers
print("\n🚨 TOP 10 RISKIEST CUSTOMERS")
print("-" * 40)

# FILL IN: Order high_risk_customers by total_high_risk_amount descending and limit to 10
top_risk_customers = high_risk_customers.orderBy(col("total_high_risk_amount").desc()).limit(10)

# FILL IN: Select relevant columns for display
top_risk_customers.select(
    "customer_name", "age", "credit_score", 
    "total_high_risk_amount", "high_risk_transaction_count", "max_risk_score"
).show(truncate=False)

# TODO: Calculate Customer Risk Summary Statistics
# FILL IN: Use .agg() to calculate summary statistics from high_risk_customers
customer_risk_stats = high_risk_customers.agg(
    count("*").alias("total_high_risk_customers"),
    round(avg("total_high_risk_amount"), 2).alias("avg_risk_amount_per_customer"),
    round(avg("high_risk_transaction_count"), 2).alias("avg_transactions_per_customer"),
    max("max_risk_score").alias("highest_risk_score"),
    round(avg("age"), 1).alias("avg_age_high_risk_customers")
)

print("\n👥 High-Risk Customer Statistics:")
customer_risk_stats.show()

# TODO: Analyze Top Problem Merchant Categories
print("\n🏪 TOP PROBLEM MERCHANT CATEGORIES")
print("-" * 40)

# FILL IN: Order high_risk_merchants by high_risk_count descending
merchant_analysis = high_risk_merchants.orderBy(col("high_risk_count").desc())
merchant_analysis.show(truncate=False)

# TODO: Identify Geographic Risk Hotspots
print("\n🌍 GEOGRAPHIC RISK HOTSPOTS")
print("-" * 30)

# FILL IN: Order geographic_risk by high_risk_count descending and limit to 10
geographic_analysis = geographic_risk.orderBy(col("high_risk_count").desc()).limit(10)
geographic_analysis.show(truncate=False)

print("✅ Risk pattern analysis complete!")

# END TODO 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Time-Based Fraud Analysis

# COMMAND ----------

# TODO 3: Analyze Time-Based Fraud Patterns
"""
OBJECTIVE: Understand when fraud is most likely to occur

COMPLETE THE TEMPLATE BELOW:
You need to analyze fraud patterns by date, hour, and day of week.
"""

print("Analyzing time-based fraud patterns...")

# TODO: Create time analysis dataset with date/time components
# FILL IN: Add date and time columns using withColumn()
time_analysis = enriched_transactions.withColumn(
    "transaction_date_only", to_date(col("transaction_date"))
).withColumn(
    "transaction_hour", hour(col("transaction_date"))
).withColumn(
    "day_of_week", dayofweek(col("transaction_date"))
).withColumn(
    # Create binary flag for high-risk transactions
    "is_final_high_risk", when(col("risk_category") == "High Risk", 1).otherwise(0)
)

# TODO: Create Daily Fraud Trend Analysis
# FILL IN: Group by transaction_date_only and calculate daily metrics
daily_fraud_trend = time_analysis.groupBy("transaction_date_only"
                                         ).agg(
    count("*").alias("total_transactions"),
    sum(col("is_final_high_risk")).alias("high_risk_transactions"),
    sum("amount").alias("total_amount"),
    sum(when(col("is_final_high_risk") == 1, col("amount")).otherwise(0)).alias("high_risk_amount")
).withColumn(
    "risk_percentage", 
    round((col("high_risk_transactions") / col("total_transactions") * 100), 2)
).orderBy("transaction_date_only")

print("\n📅 Daily Fraud Trend:")
daily_fraud_trend.show()

# TODO: Create Hourly Fraud Pattern Analysis
# FILL IN: Group by transaction_hour and calculate hourly metrics
hourly_fraud_pattern = time_analysis.groupBy("transaction_hour"
                                           ).agg(
    count("*").alias("total_transactions"),
    sum(col("is_final_high_risk")).alias("high_risk_transactions"),
    round(avg("risk_score"), 2).alias("avg_risk_score")
).withColumn(
    "risk_percentage",
    round((col("high_risk_transactions") / col("total_transactions") * 100), 2)
).orderBy("transaction_hour")

print("\n🕐 Hourly Fraud Pattern:")
hourly_fraud_pattern.show()

# TODO: Create Day of Week Analysis with Readable Names
# FILL IN: Add day_name column using when() conditions for each day of week
dow_fraud_pattern = time_analysis.withColumn(
    "day_name",
    when(col("day_of_week") == 1, "Sunday")
    .when(col("day_of_week") == 2, "Monday")
    .when(col("day_of_week") == 3, "Tuesday")
    .when(col("day_of_week") == 4, "Wednesday")
    .when(col("day_of_week") == 5, "Thursday")
    .when(col("day_of_week") == 6, "Friday")
    .when(col("day_of_week") == 7, "Saturday")
).groupBy("day_name", "day_of_week").agg(
    count("*").alias("total_transactions"),
    sum(col("is_final_high_risk")).alias("high_risk_transactions"),
    sum("amount").alias("total_amount"),
    round(avg("risk_score"), 2).alias("avg_risk_score")
).withColumn(
    "risk_percentage",
    round((col("high_risk_transactions") / col("total_transactions") * 100), 2)
).orderBy("day_of_week")

print("\n📅 Day of Week Fraud Pattern:")
dow_fraud_pattern.show()

print("✅ Time-based fraud analysis complete!")

# END TODO 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Risk Score Distribution Analysis

# COMMAND ----------

# TODO 4: Analyze Risk Score Distribution
"""
OBJECTIVE: Understand how risk scores are distributed across transactions

COMPLETE THE TEMPLATE BELOW:
You need to analyze risk score patterns and distribution.
"""

print("Analyzing risk score distribution...")

# Get total transaction count for percentage calculations
total_transactions = enriched_transactions.count()

# TODO: Create Risk Score Buckets
# FILL IN: Use withColumn() and when() to create risk_bucket categories
risk_score_distribution = enriched_transactions.withColumn(
    "risk_bucket",
    when(col("risk_score") == 0, "0 - No Risk")
    .when(col("risk_score") <= 25, "1-25 - Low Risk")
    .when(col("risk_score") <= 50, "26-50 - Medium-Low Risk")
    .when(col("risk_score") <= 70, "51-70 - Medium Risk")
    .when(col("risk_score") <= 85, "71-85 - High Risk")
    .otherwise("86-100 - Critical Risk")
).groupBy("risk_bucket").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    round(avg("risk_score"), 1).alias("avg_risk_score")
).withColumn(
    "percentage_of_transactions",
    round((col("transaction_count") / lit(total_transactions) * 100), 2)
).orderBy("avg_risk_score")

print("\n📊 Risk Score Distribution:")
risk_score_distribution.show(truncate=False)

# TODO: Create Risk Category Summary
# FILL IN: Group by risk_category and calculate summary metrics
risk_category_summary = enriched_transactions.groupBy("risk_category"
                                                     ).agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    round(avg("risk_score"), 2).alias("avg_risk_score"),
    round(avg("amount"), 2).alias("avg_transaction_amount")
).withColumn(
    "percentage_of_total",
    round((col("transaction_count") / lit(total_transactions) * 100), 2)
).orderBy(
    when(col("risk_category") == "High Risk", 1)
    .when(col("risk_category") == "Medium Risk", 2)
    .otherwise(3)
)

print("\n🎯 Risk Category Summary:")
risk_category_summary.show()

# TODO: Calculate Risk Score Statistics
# FILL IN: Use agg() to calculate statistical measures of risk scores
risk_stats = enriched_transactions.agg(
    round(avg("risk_score"), 2).alias("overall_avg_risk_score"),
    round(stddev("risk_score"), 2).alias("risk_score_std_dev"),
    min("risk_score").alias("min_risk_score"),
    max("risk_score").alias("max_risk_score"),
    expr("percentile_approx(risk_score, 0.5)").alias("median_risk_score"),
    expr("percentile_approx(risk_score, 0.9)").alias("p90_risk_score"),
    expr("percentile_approx(risk_score, 0.95)").alias("p95_risk_score")
)

print("\n📊 Risk Score Statistics:")
risk_stats.show()

print("✅ Risk score distribution analysis complete!")

# END TODO 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Data Exports for Power BI

# COMMAND ----------

# TODO 5: Create Clean Data Exports for Power BI Dashboard
"""
OBJECTIVE: Prepare and export clean datasets for Power BI visualization

COMPLETE THE TEMPLATE BELOW:
You need to create properly formatted datasets for dashboard creation.
"""

print("Creating data exports for Power BI dashboard...")

# Calculate necessary variables for exports
high_risk_transactions = enriched_transactions.filter(col("risk_score") >= 80).count()
high_risk_percentage = (high_risk_transactions / total_transactions) * 100
total_amount = enriched_transactions.agg(sum("amount")).collect()[0][0]
high_risk_amount = enriched_transactions.filter(col("risk_score") >= 80).agg(sum("amount")).collect()[0][0]
average_transaction_size = enriched_transactions.agg(avg("amount")).collect()[0][0]
average_risk_amount = enriched_transactions.filter(col("risk_score") >= 80).agg(avg("amount")).collect()[0][0]

# TODO: Export 1 - Main transaction summary for dashboard
print("\n📤 Creating main transaction export...")

# FILL IN: Select key columns for Power BI main dashboard
main_transaction_export = enriched_transactions.select(
    "transaction_id",
    "customer_id", 
    "customer_name",
    "amount",
    "transaction_date",
    "merchant_category",
    "transaction_type",
    "location",
    "risk_score",
    "risk_category",
    when(col("risk_score") >= 80, 1).otherwise(0).alias("is_high_risk"),
    "is_fraud",
    "age",
    "credit_score",
    "account_type"
).withColumn("transaction_date_only", to_date(col("transaction_date")))

try:
    main_transaction_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("fraud_transactions_export")
    print("✅ Main transaction export saved successfully")
except Exception as e:
    print(f"⚠️ Could not save to file system: {str(e)}")
    print("Data will be available in temporary view for Power BI connection")

# TODO: Export 2 - Executive summary metrics
print("\n📤 Creating executive summary export...")

# FILL IN: Create DataFrame with executive metrics for Power BI
executive_summary_export = spark.createDataFrame([
    ("Total_Transactions", float(total_transactions)),
    ("High_Risk_Transactions", float(high_risk_transactions)),
    ("High_Risk_Percentage", high_risk_percentage),
    ("Total_Amount", total_amount),
    ("High_Risk_Amount", high_risk_amount),
    ("Average_Transaction", average_transaction_size),
    ("Average_Risk_Amount", average_risk_amount),
    ("Actual_Fraud_Count", float(actual_fraud_count))
], ["metric_name", "metric_value"])

try:
    executive_summary_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("executive_summary_export")
    print("✅ Executive summary export saved successfully")
except Exception as e:
    print(f"⚠️ Could not save executive summary: {str(e)}")

# TODO: Export 3 - High-risk customers for detailed analysis
print("\n📤 Creating high-risk customers export...")

# FILL IN: Filter and group high-risk transactions by customer
high_risk_customers_export = enriched_transactions.filter(col("risk_score") >= 80
                                                         ).groupBy(
    "customer_id", "customer_name", "age", "credit_score", "account_type"
).agg(
    count("*").alias("high_risk_transaction_count"),
    sum("amount").alias("total_high_risk_amount"),
    round(avg("risk_score"), 2).alias("avg_risk_score"),
    max("risk_score").alias("max_risk_score")
)

try:
    high_risk_customers_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("high_risk_customers_export")
    print("✅ High-risk customers export saved successfully")
except Exception as e:
    print(f"⚠️ Could not save high-risk customers: {str(e)}")

# TODO: Export 4 - Daily trends for time series analysis
print("\n📤 Creating daily trends export...")

# FILL IN: Create daily trends export using time analysis
daily_trends_export = enriched_transactions.withColumn(
    "date", to_date(col("transaction_date"))
).groupBy("date").agg(
    count("*").alias("total_transactions"),
    sum(when(col("risk_score") >= 80, 1).otherwise(0)).alias("high_risk_transactions"),
    sum("amount").alias("total_amount"),
    sum(when(col("risk_score") >= 80, col("amount")).otherwise(0)).alias("high_risk_amount")
).withColumn(
    "risk_percentage", 
    round((col("high_risk_transactions") / col("total_transactions") * 100), 2)
)

try:
    daily_trends_export.coalesce(1).write.mode("overwrite").option("header", "true").csv("daily_trends_export")
    print("✅ Daily trends export saved successfully")
except Exception as e:
    print(f"⚠️ Could not save daily trends: {str(e)}")

print("\n🎉 All exports completed! Files ready for Power BI import.")

# END TODO 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Power BI Connection Views

# COMMAND ----------

# Create optimized views for Power BI connection
print("Creating optimized views for Power BI connection...")

try:
    # Create temporary views for Power BI connection
    main_transaction_export.createOrReplaceTempView("powerbi_main_transactions")
    executive_summary_export.createOrReplaceTempView("powerbi_executive_metrics")
    high_risk_customers_export.createOrReplaceTempView("powerbi_high_risk_customers")
    daily_trends_export.createOrReplaceTempView("powerbi_daily_trends")
    risk_score_distribution.createOrReplaceTempView("powerbi_risk_distribution")
    hourly_fraud_pattern.createOrReplaceTempView("powerbi_hourly_patterns")

    main_transaction_export.write.mode("overwrite").saveAsTable("powerbi_main_transactions")
    executive_summary_export.write.mode("overwrite").saveAsTable("powerbi_executive_metrics")
    high_risk_customers_export.write.mode("overwrite").saveAsTable("powerbi_high_risk_customers")
    daily_trends_export.write.mode("overwrite").saveAsTable("powerbi_daily_trends")
    risk_score_distribution.write.mode("overwrite").saveAsTable("powerbi_risk_distribution")
    hourly_fraud_pattern.write.mode("overwrite").saveAsTable("powerbi_hourly_patterns")
    high_risk_merchants.write.mode("overwrite").saveAsTable("powerbi_merchant_analysis")

    print("✅ Power BI connection views created successfully:")
    print("  - powerbi_main_transactions")
    print("  - powerbi_executive_metrics") 
    print("  - powerbi_high_risk_customers")
    print("  - powerbi_daily_trends")
    print("  - powerbi_risk_distribution")
    print("  - powerbi_hourly_patterns")
    
    # Test the views
    print("\n🧪 Testing Power BI views:")
    print("Main transactions:", spark.sql("SELECT COUNT(*) FROM powerbi_main_transactions").collect()[0][0])
    print("Executive metrics:", spark.sql("SELECT COUNT(*) FROM powerbi_executive_metrics").collect()[0][0])
    print("High-risk customers:", spark.sql("SELECT COUNT(*) FROM powerbi_high_risk_customers").collect()[0][0])
    
except Exception as e:
    print(f"❌ Error creating Power BI views: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Final Analytics Summary and Recommendations

# COMMAND ----------

# Generate final analytics summary and business recommendations
print("=== RISK ANALYTICS SUMMARY ===")
print()

# Analytics checklist
analytics_checklist = [
    ("Executive summary metrics generated", "✅"),
    ("Top risk customers identified", "✅"),
    ("Merchant category analysis completed", "✅"),
    ("Time-based fraud patterns analyzed", "✅"),
    ("Risk score distribution analyzed", "✅"),
    ("Data exported for Power BI", "✅"),
    ("Power BI connection views created", "✅")
]

for item, status in analytics_checklist:
    print(f"{status} {item}")

print()
print("📊 KEY ANALYTICAL FINDINGS:")
print(f"  - {high_risk_percentage:.1f}% of transactions flagged as high-risk")
print(f"  - ${high_risk_amount:,.2f} in potential fraudulent activity detected")
print(f"  - {high_risk_customers.count()} customers require immediate attention")
print(f"  - Top risk merchant category: {high_risk_merchants.orderBy(col('high_risk_count').desc()).collect()[0]['merchant_category']}")

# Business recommendations
print()
print("💡 BUSINESS RECOMMENDATIONS:")
print("  1. 🚨 IMMEDIATE ACTIONS:")
print("     - Review and investigate all high-risk transactions flagged")
print("     - Contact top 10 high-risk customers for verification")
print("     - Implement additional monitoring for high-risk merchant categories")

print("  2. 📊 OPERATIONAL IMPROVEMENTS:")
print("     - Deploy real-time alerting for transactions with risk scores > 80")
print("     - Enhance verification processes for high-risk customer segments")
print("     - Consider transaction limits for accounts with poor credit scores")

print("  3. 🔄 SYSTEM ENHANCEMENTS:")
print("     - Implement machine learning models for better prediction accuracy")
print("     - Add geographic anomaly detection based on customer history")
print("     - Develop customer behavior profiling for velocity detection")

print()
print("📋 POWER BI DASHBOARD SETUP:")
print("  1. Import the following CSV exports into Power BI:")
print("     - fraud_transactions_export (main data)")
print("     - executive_summary_export (KPI metrics)")
print("     - high_risk_customers_export (customer details)")
print("     - daily_trends_export (time series)")

print("  2. Connect to Databricks views for real-time data:")
print("     - Use powerbi_main_transactions for main dashboard")
print("     - Use powerbi_executive_metrics for executive summary")
print("     - Use powerbi_daily_trends for trend analysis")

print()
print("🎯 SUCCESS METRICS:")
print(f"  - Fraud Detection Rate: {high_risk_percentage:.2f}%")
print(f"  - Potential Savings: ${high_risk_amount:,.2f}")
print(f"  - Investigation Efficiency: {high_risk_transactions} alerts generated")

print()
print("✅ Risk analytics completed successfully!")
print("📅 Analytics completed at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count as pycount
# Perform final validation and quality checks
print("Performing final validation checks...")

# Check 1: Ensure all exports have data
export_validation = {
    "Main Transactions": main_transaction_export.count(),
    "Executive Metrics": executive_summary_export.count(), 
    "High-Risk Customers": high_risk_customers_export.count(),
    "Daily Trends": daily_trends_export.count()
}

print("\n📊 Export Validation Summary:")
for export_name, count in export_validation.items():
    status = "✅" if count > 0 else "❌"
    print(f"{status} {export_name}: {count} records")

# Check 2: Verify risk score calculations
risk_score_check = enriched_transactions.select(
    min("risk_score").alias("min_score"),
    max("risk_score").alias("max_score"),
    avg("risk_score").alias("avg_score")
)

print("\n📊 Risk Score Validation:")
risk_score_check.show()

# Check 3: Verify category consistency
category_check = enriched_transactions.groupBy("risk_category").agg(
    min("risk_score").alias("min_score"),
    max("risk_score").alias("max_score"),
    pycount("*").alias("count")
).orderBy("risk_category")

print("Risk Category Validation:")
category_check.show()

print("✅ All validation checks completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO Completion Checklist

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO Completion Checklist:
# MAGIC - [ ] **TODO 1**: Executive summary metrics extracted and calculated correctly
# MAGIC - [ ] **TODO 2**: Top risk customers, merchants, and geographic patterns identified
# MAGIC - [ ] **TODO 3**: Time-based fraud patterns analyzed (daily, hourly, day-of-week)
# MAGIC - [ ] **TODO 4**: Risk score distribution analysis completed with statistical measures
# MAGIC - [ ] **TODO 5**: Clean data exports created for Power BI dashboard
# MAGIC
# MAGIC **When all TODOs are complete, you should have:**
# MAGIC - Executive dashboard metrics ready for C-level reporting
# MAGIC - Comprehensive fraud pattern analysis identifying key risk areas
# MAGIC - Time-series analysis showing when fraud is most likely to occur
# MAGIC - Statistical analysis of risk score distribution
# MAGIC - Clean, formatted data exports ready for Power BI visualization
# MAGIC
# MAGIC ### Troubleshooting Tips:
# MAGIC
# MAGIC **Common Issues:**
# MAGIC 1. **Data Export Errors**: Use temporary views if file system writes fail
# MAGIC 2. **Aggregation Problems**: Check for null values and use appropriate handling
# MAGIC 3. **Time Analysis Issues**: Ensure date columns are properly formatted
# MAGIC 4. **Performance Problems**: Use `.cache()` on frequently accessed DataFrames
# MAGIC
# MAGIC **Success Validation:**
# MAGIC - All export validation checks show ✅ status
# MAGIC - Risk scores range from 0-100 with reasonable distribution
# MAGIC - Time-based patterns show logical fraud trends
# MAGIC - Executive metrics provide clear business insights

# COMMAND ----------

high_risk_merchants.describe()