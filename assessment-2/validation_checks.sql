use BankingDW
GO

PRINT 'Starting comprehensive data quality validation framework...';
PRINT 'Implementing 10 validation checks across 4 categories';

-- Clear previous validation results for this run
DELETE FROM validation_results WHERE CAST(check_date AS DATE) = CAST(GETDATE() AS DATE);

PRINT 'Previous validation results cleared - starting fresh validation run';

-- =====================================================
-- CATEGORY 1: REFERENTIAL INTEGRITY VALIDATION
-- =====================================================

PRINT '=== CATEGORY 1: REFERENTIAL INTEGRITY VALIDATION ===';

-- VALIDATION 1: Account Holder References in Fact Table
PRINT 'Validation 1: Checking account_holders foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Account Holder Foreign Key Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN da.account_holder_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN da.account_holder_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN da.account_holder_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN da.account_holder_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid Account Holder references'
        ELSE 'All Account Holders references are valid'
    END AS error_details
FROM fact_transactions fs
LEFT JOIN dim_account_holder da ON fs.account_holder_key = da.account_holder_key;

-- VALIDATION 2: Bank Product References in Fact Table
PRINT 'Validation 2: Checking bank product foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Bank Product Foreign Key Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dp.banking_product_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dp.banking_product_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dp.banking_product_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dp.banking_product_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid bank product references'
        ELSE 'All bank products references are valid'
    END AS error_details
FROM fact_transactions fs
LEFT JOIN dim_banking_product dp ON fs.banking_product_key = dp.banking_product_key;

-- VALIDATION 3: Date References in Fact Table
PRINT 'Validation 3: Checking date foreign key references...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Date Foreign Key Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) > 0
        THEN 'Found orphaned fact records with invalid date references'
        ELSE 'All date references are valid'
    END AS error_details
FROM fact_transactions fs
LEFT JOIN dim_date dd ON fs.date_key = dd.date_key;

PRINT 'Referential integrity validation completed';


-- =====================================================
-- CATEGORY 2: RANGE VALIDATION
-- =====================================================

PRINT '=== CATEGORY 2: RANGE VALIDATION ===';

-- VALIDATION 4: Transactions amount Range Check
PRINT 'Validation 4: Checking transaction ranges (-50000-50000)...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'transactions amount Range Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN transaction_amount < -50000 OR transaction_amount > 50000 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN transaction_amount < -50000 OR transaction_amount > 50000 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN transaction_amount < -50000 OR transaction_amount > 50000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN transaction_amount < -50000 OR transaction_amount > 50000 THEN 1 ELSE 0 END) > 0
        THEN 'Found Transactions outside valid range (-50000-50000)'
        ELSE 'All Transactions within valid range'
    END AS error_details
FROM fact_transactions;

-- VALIDATION 5: Future Date Check
PRINT 'Validation 6: Checking for future transaction dates...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Future Date Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN dd.full_date > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) > 0
        THEN 'Found transactions with future dates'
        ELSE 'No future-dated transactions found'
    END AS error_details
FROM fact_transactions fs
JOIN dim_date dd ON fs.date_key = dd.date_key;


PRINT 'Range validation completed';

-- VALIDATION 6: Account Holder Number Format Check
PRINT 'Validation 5: Checking Account Holder number Format Check...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'Account Holder number Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE
    WHEN account_number LIKE 'CC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'MTG[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SAV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'INV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SVC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'LON[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'AUTO[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'CHK[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    ELSE 1
END) AS records_failed,
    CASE 
        WHEN SUM(CASE
    WHEN account_number LIKE 'CC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'MTG[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SAV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'INV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SVC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'LON[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'AUTO[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'CHK[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    ELSE 1
END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE
    WHEN account_number LIKE 'CC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'MTG[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SAV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'INV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SVC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'LON[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'AUTO[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'CHK[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    ELSE 1
END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE
    WHEN account_number LIKE 'CC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'MTG[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SAV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'INV[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'SVC[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'LON[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'AUTO[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    WHEN account_number LIKE 'CHK[0-9][0-9][0-9][0-9][0-9][0-9]' THEN 0
    ELSE 1
END) > 0
        THEN 'Found Account Holder Number not following the format CC|MTG|SAV|INV|SVC|LON|AUTO|CHK + [0-9]*6'
        ELSE 'All Account Holder numbers are valid'
    END AS error_details
FROM fact_transactions;

-- =====================================================
-- CATEGORY 3: DATE LOGIC VALIDATION
-- =====================================================

-- VALIDATION 7: SCD Date Sequence Check
PRINT 'Validation 7: Checking SCD date sequence logic...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Date Sequence Check' AS rule_name,
    'dim_account_holder' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE 
        WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
        THEN 1 
        ELSE 0 
    END) AS records_failed,
    CASE 
        WHEN SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE 
            WHEN expiration_date IS NOT NULL AND effective_date > expiration_date 
            THEN 1 
            ELSE 0 
        END) > 0
        THEN 'Found SCD records with effective_date > expiration_date'
        ELSE 'All SCD date sequences are valid'
    END AS error_details
FROM dim_account_holder;


-- VALIDATION 8: SCD Current Record Logic
PRINT 'Validation 8: Checking SCD current record logic...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Current Record Logic Check' AS rule_name,
    'dim_account_holder' AS table_name,
    COUNT(*) AS records_checked,
    SUM(CASE 
        WHEN is_current = 1 AND expiration_date IS NOT NULL 
        THEN 1 
        ELSE 0 
    END) AS records_failed,
    CASE 
        WHEN SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(*) > 0 
        THEN (SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) * 100.0 / COUNT(*))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE 
            WHEN is_current = 1 AND expiration_date IS NOT NULL 
            THEN 1 
            ELSE 0 
        END) > 0
        THEN 'Found current records with non-null expiration dates'
        ELSE 'All current records have null expiration dates'
    END AS error_details
FROM dim_account_holder;

PRINT 'Date logic validation completed';

-- =====================================================
-- CATEGORY 4: CALCULATION CONSISTENCY
-- =====================================================

-- VALIDATION 9: SCD Account Holder Uniqueness Check
PRINT 'Validation 9: Checking SCD Account holder uniqueness...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Account Holder Uniqueness Check' AS rule_name,
    'dim_account_holder' AS table_name,
    COUNT(DISTINCT account_holder_id) AS records_checked,
    SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(DISTINCT account_holder_id) > 0 
        THEN (SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT account_holder_id))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) > 0
        THEN 'Found Account Holder with multiple current records'
        ELSE 'Each Account Holder has exactly one current record'
    END AS error_details
FROM (
    SELECT 
        account_holder_id,
        SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_count
    FROM dim_account_holder
    GROUP BY account_holder_id
) current_record_counts;

PRINT 'Calculation consistency validation completed';

-- VALIDATION 9: SCD Transaction Uniqueness Check
PRINT 'Validation 9: Checking SCD Transaction ID uniqueness...';

INSERT INTO validation_results (
    rule_name, 
    table_name, 
    records_checked, 
    records_failed, 
    validation_status, 
    failure_percentage, 
    error_details
)
SELECT 
    'SCD Transaction Uniqueness Check' AS rule_name,
    'fact_transactions' AS table_name,
    COUNT(DISTINCT transaction_id) AS records_checked,
    SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) AS records_failed,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) = 0 
        THEN 'PASSED' 
        ELSE 'FAILED' 
    END AS validation_status,
    CASE 
        WHEN COUNT(DISTINCT transaction_id) > 0 
        THEN (SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT transaction_id))
        ELSE 0
    END AS failure_percentage,
    CASE 
        WHEN SUM(CASE WHEN current_count > 1 THEN 1 ELSE 0 END) > 0
        THEN 'Found Transaction with multiple current records'
        ELSE 'Each Transaction has exactly one current record'
    END AS error_details
FROM (
    SELECT 
        transaction_id,
        SUM(1) AS current_count
    FROM fact_transactions
    GROUP BY transaction_id
) current_record_counts;

PRINT 'Calculation consistency validation completed';

-- =====================================================
-- Summary Report
-- =====================================================

select * from validation_results order by validation_status asc;
