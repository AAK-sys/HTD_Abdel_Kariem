use BankingDW
GO

-- Final data count verification
SELECT 'stg_transactions_raw' as table_name, COUNT(*) as row_count FROM stg_transactions_raw
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM fact_transactions  
UNION ALL
SELECT 'dim_account_holder', COUNT(*) FROM dim_account_holder
UNION ALL
SELECT 'dim_banking_product', COUNT(*) FROM dim_banking_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'validation_results', COUNT(*) FROM validation_results;