Create DataBase BankingDW
GO

use BankingDW
GO

/*
DIMENSION TABLE: dim_account_holder
SCD TYPE: Type 2 (Historical tracking of account holders changes)
BUSINESS KEY: account_holder_id (natural identifier from source systems)
TRACKED ATTRIBUTES: 
  - account_holder_name: Changes tracked for name updates
  - address: Changes tracked for shipping and demographic analysis
  - relationship_tier: Changes tracked for loyalty program evolution
PURPOSE: 
  - Stores account holder master data with full historical tracking
  - Enables account holder tiering and lifetime value analysis
  - Supports personalization and targeted marketing campaigns
*/
CREATE TABLE dim_account_holder (
    account_holder_key INT IDENTITY(1,1) PRIMARY KEY,
    account_holder_id VARCHAR(20) NOT NULL,
    account_holder_name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    relationship_tier VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL
);

/*
DIMENSION TABLE: dim_banking_product
PURPOSE: 
  - Stores bank product catalog information for bank product analysis
  - Enables bank product frequency tracking and analysis
KEY ATTRIBUTES:
  - product_name: Primary identifier for business users
  - product_category: Enables category-level analysis and reporting
  - is_active: Supports disabling a banking product
*/
CREATE TABLE dim_banking_product (
    banking_product_key INT IDENTITY(1,1) PRIMARY KEY,
    banking_product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE()
);

/*
DIMENSION TABLE: dim_date
PURPOSE: 
  - Provides comprehensive date attributes for temporal analysis
  - Enables time-based reporting and trend analysis
KEY ATTRIBUTES:
  - date_key: Integer key in YYYYMMDD format for performance
  - quarter_number: Enables quarterly business reporting
  - is_weekend/is_business_day: Supports day-of-week analysis
  - Standard date components: year, month, day for flexible grouping
*/
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_business_day BIT NOT NULL
);

-- =====================================================
-- VALIDATION AND CONTROL TABLES
-- =====================================================

/*
CONTROL TABLE: validation_results
PURPOSE: 
  - Tracks data quality validation results across all ETL processes
  - Provides audit trail for data quality monitoring
  - Enables trend analysis of data quality over time
*/

CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    records_checked INT NOT NULL,
    records_failed INT NOT NULL,
    validation_status VARCHAR(20) NOT NULL,
    failure_percentage DECIMAL(5,2) NOT NULL,
    check_date DATETIME NOT NULL DEFAULT GETDATE(),
    error_details NVARCHAR(500) NULL
);

/*
FACT TABLE: fact_transactions
GRAIN: One row per transaction per account_number
MEASURES: 
  - account_number: Determines the account the transaction is involved in
  - transaction_amount: The amount to be withdrawn or deposited to the account
  - transaction_type: withdrawl, or deposite
BUSINESS PURPOSE: 
  - Tracks all trasnactions
  - Enables analysis by account holder/transaction/account type. 
*/
CREATE TABLE fact_transactions (
    transaction_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    account_holder_key INT NOT NULL,
    banking_product_key INT NOT NULL,
    date_key INT NOT NULL,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    account_number VARCHAR(20) NOT NULL,
    transaction_amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    load_date DATETIME NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (account_holder_key) REFERENCES dim_account_holder(account_holder_key),
    FOREIGN KEY (banking_product_key) REFERENCES dim_banking_product(banking_product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);