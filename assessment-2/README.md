# Community Bank Data Warehouse

## Setup Instructions

### Prerequisites
- **SQL Server** (LocalDB, Express, or Developer Edition 2017+)
- **SQL Server Management Studio (SSMS)** or **Azure Data Studio**
- **Database Creation Permissions** on target SQL Server instance
- **CSV Import Capability** for loading sample data

### Step-by-Step Setup
1. **Create Database Environment**
   ```sql
   CREATE DATABASE BankingDW;
   USE BankingDW;
   ```

2. **Execute Schema Creation**
   - Run `schema_ddl.sql` to create all tables and indexes
   - Verify all 5 tables are created successfully
   - Confirm foreign key relationships are established

3. **Load Sample Data**
   - Import the provided Banking CSV data into `stg_transactions_raw` table
   - Verify data loaded correctly with basic row count check
   - Expected: 1000+ transaction records with realistic banking data

4. **Execute ETL Pipeline**
   - Run `etl_pipeline.sql` to process all transformations
   - Monitor console output for processing progress
   - Verify SCD Type 2 creates multiple account holder versions

5. **Run Validation Framework**
   - Execute `validation_checks.sql` to verify data quality
   - Review validation results for any failures
   - Address any critical validation failures before proceeding

6. **Execute Analytics Queries**
   - Run `window_functions.sql` to generate business insights
   - Review moving averages, lifetime values, and purchase rankings
   - Verify meaningful business intelligence results

### Expected Results
- **Staging Table**: 1000 raw transaction records
- **Fact Table**: 1000 processed transaction transactions
- **Account Holder Dimension**: 150 records (including SCD Type 2 versions)
- **Banking Product Dimension**: 16 unique banking products across multiple categories
- **Date Dimension**: 366 dates for complete 2024 calendar year
- **Validation Results**: 10 validation checks with pass/fail status

## Schema Design

### Star Schema Overview
This  data warehouse implements a classic star schema optimized for transactions analysis and customer intelligence:

**Central Fact Table**: `fact_transactions`
- **Grain**: One row per product per transaction line item
- **Measures**: Transaction Amount, Banking Product, Transaction Type
- **Purpose**: Enables comprehensive Transactions analysis, bank product frequency tracking, and Account Holders behavior insights

**Dimension Tables**:
- **`dim_account_holder`**: account holder master data with SCD Type 2 historical tracking
- **`dim_banking_product`**: Bank product catalog with product category, active status, and creation date
- **`dim_date`**: Complete calendar dimension with business day indicators

**Key Design Decisions**:
- Surrogate keys used throughout for performance and SCD Type 2 support
- account holder dimension tracks historical changes for accurate trend analysis
- Fact table captures individual line items for detailed transaction analysis

### Business Rules Applied

**Data Standardization Rules**:
- Account holder names converted to proper case format
- Address formatting standardized with consistent spacing
- Account holders tiers' mapped to exactly 4 standard values (Preferred, Premium, Standard, Basic)

**Business Logic Rules**:
- **Enhanced Relationship tiers**: premium Account Holders in major metropolitan areas (New York, Los Angeles, Chicago) receive 'Premium Metro' designation, Premium Standard for any premium status with other cities.
- **risk_profile Classification**: account holders classified by their tiers, Preferred and premium: low risk, Standard: medium risk, basic: high risk.

**SCD Type 2 Implementation**:
- Tracks changes in account holders name, address, and tiers
- Maintains complete historical record of account holder evolution
- Enables accurate trend analysis and account holder lifetime value calculations


## Key Assumptions

### Data Quality Assumptions
- **Source Data Completeness**: Assumed source system provides core required fields (account_holder_id, account_holder_name, transaction details)
- **Data Format Consistency**: Applied standardization rules to handle common variations in name casing, address spacing, and tiers values
- **Date Data Integrity**: Assumed transaction dates are generally reliable with occasional future date exceptions handled by validation
- **Transaction Amount Range Validity**: Applied business-reasonable ranges (transaction ammount -50000-50000) with validation monitoring

### Business Logic Assumptions  
- **Account Holder Relationship tiers**: Assumed 4-tier structure (Preferred, Premium, Standard, Basic) represents complete tier classification
- **Geographic Enhancement**: Major metropolitan areas warrant enhanced Premium treatment based on market importance
- **Historical Tracking**: Assumed business value in tracking account holders changes over time for retention and growth analysis
- **Product Categorization**: Maintained source system banking product categories as business-meaningful groupings

### Technical Assumptions
- **Surrogate Key Strategy**: Implemented IDENTITY-based surrogate keys for optimal performance and SCD Type 2 support
- **SCD Type 2 Scope**: Limited historical tracking to key account holder attributes that impact business analysis
- **Batch Processing**: Designed for batch-oriented ETL processing with full data refresh capability

## Validation Results

### Summary by Category
**Category 1: Referential Integrity**
- ✅ Account holders Foreign Key Check: All fact records have valid account holders references
- ✅ Bank product Foreign Key Check: All fact records have valid product references  
- ✅ Date Foreign Key Check: All fact records have valid date references
- **Result**: 100% referential integrity maintained

**Category 2: Range Validation**
- ✅ Transaction Amount Range Check: All amounts within business-reasonable range (-50000 to 50000)
- ✅ Future Date Check: No transactions with future dates detected
- ✅ Account Name format Check: All Account names within busniess rules.
- **Result**: All business rules consistently enforced

**Category 3: Date Logic Validation**
- ✅ SCD Date Sequence Check: All effective dates precede expiration dates
- ✅ SCD Current Record Logic: All current records have null expiration dates
- **Result**: SCD Type 2 temporal logic functioning correctly

**Category 4: Calculation Consistency**
- ✅ SCD Account Holder Uniqueness Check: Each Account Holder has exactly one current record
- ✅ SCD Transaction ID Uniqueness Check: Each Transaction ID has exactly one record
- **Result**: Data consistency maintained across all calculations

### Data Quality Score
**Overall Assessment**: EXCELLENT
- **Total Validations**: 10
- **Validations Passed**: 10
- **Overall Pass Rate**: 100%
- **Data Quality Grade**: A+