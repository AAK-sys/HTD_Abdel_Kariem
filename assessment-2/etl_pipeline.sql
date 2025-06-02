use BankingDW
GO

-- select * from stg_transactions_raw

WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes
    SELECT DISTINCT
        account_holder_id,
        
        -- Account Holder Name Standardization: Convert to proper case -- REMEBER TO PROPER CASE THE THIRD WORD IF IT EXISTS
        CASE 
            WHEN account_holder_name IS NULL OR TRIM(account_holder_name) = '' THEN NULL
            ELSE 
                upper(left(ltrim(rtrim(account_holder_name)), 1)) +
                lower(substring(ltrim(rtrim(account_holder_name)), 2, charindex(' ', ltrim(rtrim(account_holder_name))) - 1)) +
                upper(substring(ltrim(rtrim(account_holder_name)), charindex(' ', ltrim(rtrim(account_holder_name))) + 1, 1)) +
                lower(substring(ltrim(rtrim(account_holder_name)), charindex(' ', ltrim(rtrim(account_holder_name))) + 2, len(ltrim(rtrim(account_holder_name)))))
        END AS clean_account_holder_name,
        
        -- Address Standardization: Remove Any inconsistant spacing
        TRIM(REPLACE(REPLACE(REPLACE(address, ' ', '<>'), '><', ''), '<>', ' ')) AS CleanedAddress,
        
        -- Account Holder Tier Standardization: Map variations to standard values
        CASE 
            WHEN UPPER(relationship_tier) IN ('PREM', 'PREMIUM') THEN 'Premium'
            WHEN UPPER(relationship_tier) IN ('PREF', 'PREFERRED') THEN 'Preferred'
            WHEN UPPER(relationship_tier) IN ('STD', 'STANDARD') THEN 'Standard'
            WHEN UPPER(relationship_tier) IN ('BASIC') THEN 'Basic'
            ELSE 'Standard'
        END AS clean_relationship_tier

        from stg_transactions_raw
        where account_holder_id is not null
        AND account_holder_name is not null
        AND account_holder_name <> ''
        AND transaction_amount BETWEEN -50000 and 50000
), business_rules as (
    select
        account_holder_id,
        clean_account_holder_name,
        CleanedAddress,
        CASE
            WHEN clean_relationship_tier = 'Premium' AND
                SUBSTRING(
                    CleanedAddress,
                    CHARINDEX(',', CleanedAddress) + 2,
                    LEN(CleanedAddress) - CHARINDEX(',', CleanedAddress) - 1 - LEN(RIGHT(CleanedAddress, CHARINDEX(' ', REVERSE(CleanedAddress)) - 1))
                ) IN ('New York', 'Los Angeles', 'Chicago')
            THEN 'Premium Metro'
            WHEN clean_relationship_tier = 'Premium'
            THEN 'Premium Standard'
            ELSE clean_relationship_tier
        END AS final_relationship_tier,
        CASE
            WHEN clean_relationship_tier IN ('Premium', 'Preferred') THEN 'Low Risk'
            WHEN clean_relationship_tier = 'Standard' THEN 'Medium Risk'
            WHEN clean_relationship_tier = 'Basic' THEN 'High Risk'
            ELSE NULL
        END AS risk_profile
    from data_cleanup), final_staging as (
        -- Stage 3: Prepare for SCD Type 2 loading with change detection
        SELECT 
            br.account_holder_id,
            br.clean_account_holder_name,
            br.CleanedAddress,
            br.final_relationship_tier,
            br.risk_profile,
            
            -- Existing record information
            existing.account_holder_key,
            existing.account_holder_name AS existing_name,
            existing.address AS existing_address,
            existing.relationship_tier AS existing_relationship_tier,
            
            -- Change Detection Logic
            CASE 
                WHEN existing.account_holder_key IS NULL THEN 'NEW'
                WHEN existing.account_holder_name != br.clean_account_holder_name OR
                    existing.address != br.CleanedAddress OR
                    existing.relationship_tier != br.final_relationship_tier
                THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_type,
            
            -- SCD Action Assignment
            CASE 
                WHEN existing.account_holder_key IS NULL THEN 'INSERT_NEW'
                WHEN existing.account_holder_name != br.clean_account_holder_name OR
                    existing.address != br.CleanedAddress OR
                    existing.relationship_tier != br.final_relationship_tier
                THEN 'EXPIRE_AND_INSERT'
                ELSE 'NO_ACTION'
            END AS scd_action
            
        FROM business_rules br
        LEFT JOIN dim_account_holder existing ON br.account_holder_id = existing.account_holder_id 
                                    AND existing.is_current = 1
    )
    
    -- Step 1: Expire Changed Records
    UPDATE dim_account_holder 
    SET expiration_date = CAST(GETDATE() AS DATE),
        is_current = 0,
        updated_date = GETDATE()
    WHERE account_holder_key IN (
        SELECT existing.account_holder_key 
        FROM final_staging fs
        JOIN dim_account_holder existing ON fs.account_holder_id = existing.account_holder_id
        WHERE fs.change_type = 'CHANGED' 
        AND existing.is_current = 1
    )
    PRINT 'Expired ' + CAST(@@ROWCOUNT AS VARCHAR) + ' changed Account Holder records';


WITH data_cleanup AS (
    -- Stage 1: Data standardization and quality fixes
    SELECT DISTINCT
        account_holder_id,
        
        -- Account Holder Name Standardization: Convert to proper case -- REMEBER TO PROPER CASE THE THIRD WORD IF IT EXISTS
        CASE 
            WHEN account_holder_name IS NULL OR TRIM(account_holder_name) = '' THEN NULL
            ELSE 
                upper(left(ltrim(rtrim(account_holder_name)), 1)) +
                lower(substring(ltrim(rtrim(account_holder_name)), 2, charindex(' ', ltrim(rtrim(account_holder_name))) - 1)) +
                upper(substring(ltrim(rtrim(account_holder_name)), charindex(' ', ltrim(rtrim(account_holder_name))) + 1, 1)) +
                lower(substring(ltrim(rtrim(account_holder_name)), charindex(' ', ltrim(rtrim(account_holder_name))) + 2, len(ltrim(rtrim(account_holder_name)))))
        END AS clean_account_holder_name,
        
        -- Address Standardization: Remove Any inconsistant spacing
        TRIM(REPLACE(REPLACE(REPLACE(address, ' ', '<>'), '><', ''), '<>', ' ')) AS CleanedAddress,
        
        -- Account Holder tier Standardization: Map variations to standard values
        CASE 
            WHEN UPPER(relationship_tier) IN ('PREM', 'PREMIUM') THEN 'Premium'
            WHEN UPPER(relationship_tier) IN ('PREF', 'PREFERRED') THEN 'Preferred'
            WHEN UPPER(relationship_tier) IN ('STD', 'STANDARD') THEN 'Standard'
            WHEN UPPER(relationship_tier) IN ('BASIC') THEN 'Basic'
            ELSE 'Standard'
        END AS clean_relationship_tier

        from stg_transactions_raw
        where account_holder_id is not null
        AND account_holder_name is not null
        AND account_holder_name <> ''
        AND transaction_amount BETWEEN -50000 and 50000
), business_rules as (
    select
        account_holder_id,
        clean_account_holder_name,
        CleanedAddress,
        CASE
            WHEN clean_relationship_tier = 'Premium' AND
                SUBSTRING(
                    CleanedAddress,
                    CHARINDEX(',', CleanedAddress) + 2,
                    LEN(CleanedAddress) - CHARINDEX(',', CleanedAddress) - 1 - LEN(RIGHT(CleanedAddress, CHARINDEX(' ', REVERSE(CleanedAddress)) - 1))
                ) IN ('New York', 'Los Angeles', 'Chicago')
            THEN 'Premium Metro'
            WHEN clean_relationship_tier = 'Premium'
            THEN 'Premium Standard'
            ELSE clean_relationship_tier
        END AS final_relationship_tier,
        CASE
            WHEN clean_relationship_tier IN ('Premium', 'Preferred') THEN 'Low Risk'
            WHEN clean_relationship_tier = 'Standard' THEN 'Medium Risk'
            WHEN clean_relationship_tier = 'Basic' THEN 'High Risk'
            ELSE NULL
        END AS risk_profile
    from data_cleanup), final_staging as (
        -- Stage 3: Prepare for SCD Type 2 loading with change detection
        SELECT 
            br.account_holder_id,
            br.clean_account_holder_name,
            br.CleanedAddress,
            br.final_relationship_tier,
            br.risk_profile,
            
            -- Existing record information
            existing.account_holder_key,
            existing.account_holder_name AS existing_name,
            existing.address AS existing_address,
            existing.relationship_tier AS existing_relationship_tier,
            
            -- Change Detection Logic
            CASE 
                WHEN existing.account_holder_key IS NULL THEN 'NEW'
                WHEN existing.account_holder_name != br.clean_account_holder_name OR
                    existing.address != br.CleanedAddress OR
                    existing.relationship_tier != br.final_relationship_tier
                THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_type,
            
            -- SCD Action Assignment
            CASE 
                WHEN existing.account_holder_key IS NULL THEN 'INSERT_NEW'
                WHEN existing.account_holder_name != br.clean_account_holder_name OR
                    existing.address != br.CleanedAddress OR
                    existing.relationship_tier != br.final_relationship_tier
                THEN 'EXPIRE_AND_INSERT'
                ELSE 'NO_ACTION'
            END AS scd_action
            
        FROM business_rules br
        LEFT JOIN dim_account_holder existing ON br.account_holder_id = existing.account_holder_id 
                                    AND existing.is_current = 1
    )

    INSERT INTO dim_account_holder (
    account_holder_id, 
    account_holder_name, 
    address, 
    relationship_tier, 
    effective_date, 
    expiration_date, 
    is_current
)
SELECT 
    account_holder_id,
    clean_account_holder_name,
    CleanedAddress,
    final_relationship_tier,
    CAST(GETDATE() AS DATE),
    NULL,
    1
FROM (
    SELECT 
        account_holder_id,
        clean_account_holder_name,
        CleanedAddress,
        final_relationship_tier,
        change_type,
        ROW_NUMBER() OVER (
            PARTITION BY account_holder_id 
            ORDER BY clean_account_holder_name
        ) as rn
    FROM final_staging
    WHERE change_type IN ('NEW', 'CHANGED')
) deduped_Account_holders
WHERE rn = 1;  -- Take only one record per account_holder_id

-- =====================================================
-- BANK PRODUCT DIMENSION LOADING
-- =====================================================

PRINT 'Loading Bank Product Dimension...';

INSERT INTO dim_banking_product (banking_product_id, product_name, product_category, is_active, created_date)
SELECT 
    ranked_products.banking_product_id,
    ranked_products.product_name,
    ranked_products.product_category,
    ranked_products.is_active,
    ranked_products.created_date
FROM (
    SELECT 
        banking_product_id,
        product_name,
        product_category,
        1 as is_active,
        GETDATE() as created_date,
        ROW_NUMBER() OVER (
            PARTITION BY banking_product_id 
            ORDER BY product_name, product_category
        ) as rn
    FROM stg_transactions_raw
    WHERE banking_product_id IS NOT NULL
      AND product_name IS NOT NULL
) ranked_products
LEFT JOIN dim_banking_product dp ON ranked_products.banking_product_id = dp.banking_product_id
WHERE dp.banking_product_id IS NULL  
  AND ranked_products.rn = 1;

PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR) + ' new Bank products';

-- =====================================================
-- DATE DIMENSION POPULATION
-- =====================================================

PRINT 'Populating Date Dimension for 2024...';

-- Clear existing 2024 dates if rerunning
DELETE FROM dim_date WHERE year_number = 2024;

WITH date_generator AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_generator
    WHERE date_value < '2024-12-31'
)
INSERT INTO dim_date (
    date_key, 
    full_date, 
    year_number, 
    month_number, 
    day_number, 
    day_name, 
    month_name, 
    quarter_number, 
    is_weekend, 
    is_business_day
)
SELECT 
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_key,
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    DATENAME(MONTH, date_value),
    CASE 
        WHEN MONTH(date_value) IN (1,2,3) THEN 1
        WHEN MONTH(date_value) IN (4,5,6) THEN 2
        WHEN MONTH(date_value) IN (7,8,9) THEN 3
        ELSE 4
    END,
    CASE WHEN DATENAME(WEEKDAY, date_value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
    CASE WHEN DATENAME(WEEKDAY, date_value) NOT IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM date_generator
OPTION (MAXRECURSION 400);

PRINT 'Populated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' date records for 2024';

-- =====================================================
-- FACT TABLE LOADING
-- =====================================================

PRINT 'Loading Fact transactions with surrogate keys...';

INSERT INTO fact_transactions (
    account_holder_key,
    banking_product_key,
    date_key,
    transaction_id,
    account_number,
    transaction_amount,
    transaction_type,
    load_date
)
SELECT 
    dc.account_holder_key,
    dp.banking_product_key,
    dd.date_key,
    s.transaction_id,
    s.account_number,
    s.transaction_amount,
    s.transaction_type,
    GETDATE()
FROM stg_transactions_raw s
JOIN dim_account_holder dc ON s.account_holder_id = dc.account_holder_id AND dc.is_current = 1
JOIN dim_banking_product dp ON s.banking_product_id = dp.banking_product_id
JOIN dim_date dd ON CAST(s.transaction_date AS DATE) = dd.full_date
where s.account_holder_id is not null
AND s.account_holder_name is not null
AND s.account_holder_name <> ''
AND s.transaction_amount BETWEEN -50000 and 50000
  AND NOT EXISTS (
      SELECT 1 FROM fact_transactions fo 
      WHERE fo.transaction_id = s.transaction_id 
        AND fo.account_number = s.account_number
  );

PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR) + ' fact records';