use BankingDW
GO

/*
BUSINESS CONTEXT: 3-Transactions Moving Average Analysis
- Calculates the average transaction value over the account holder's last 3 transactions
- Helps relationship managers identify changing account holders behavior patterns
- Rising moving averages indicate increased account holder engagement/value
*/

PRINT 'Analyzing account holder spending patterns with 3-transactions moving averages...';

SELECT 
    fs.account_holder_key,
    dc.account_holder_name,
    dc.relationship_tier,
    dd.full_date AS transaction_date,
    fs.transaction_id,
    fs.transaction_amount,
    
    AVG(fs.transaction_amount) OVER (
        PARTITION BY fs.account_holder_key 
        ORDER BY dd.full_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_3_transaction_avg

FROM fact_transactions fs
JOIN dim_account_holder dc ON fs.account_holder_key = dc.account_holder_key AND dc.is_current = 1
JOIN dim_date dd ON fs.date_key = dd.date_key
WHERE fs.account_holder_key IN (
    SELECT account_holder_key 
    FROM fact_transactions
    GROUP BY account_holder_key 
    HAVING COUNT(*) >= 3
)

ORDER BY fs.account_holder_key, dd.full_date;

PRINT 'Moving average analysis completed for account holders with 3+ transactions';

/*
BUSINESS CONTEXT: Account Holders Lifetime Value Progression
- Calculates cumulative account balance for each account holder over time
- Shows risk averse Account holders through tracking constant negative transactions (debit)
- analyses the amount and frequency of deposites, and withdrawals for cash flow analysis
*/

PRINT 'Calculating account holders lifetime value progression with running totals...';

SELECT 
    fs.account_holder_key,
    dc.account_holder_name,
    dc.relationship_tier,
    dc.address,
    dd.full_date AS transaction_date,
    fs.transaction_id,
    fs.transaction_amount,
    
    SUM(fs.transaction_amount) OVER (
        PARTITION BY fs.account_holder_key 
        ORDER BY dd.full_date
        ROWS UNBOUNDED PRECEDING
    ) AS account_running_balance

FROM fact_transactions fs
JOIN dim_account_holder dc ON fs.account_holder_key = dc.account_holder_key AND dc.is_current = 1
JOIN dim_date dd ON fs.date_key = dd.date_key

ORDER BY fs.account_holder_key, dd.full_date;

PRINT 'account holders lifetime value analysis completed';

/*
BUSINESS CONTEXT: Account Holder's Most Significant transactions
- Ranks each account holder's transaction by value to identify their most significant transaction
- Rank 1 transactions represent the account holder's highest-value transactions
- Helps the relationship managers identify account holder's patterns and preferences
*/

PRINT 'Analyzing account holders purchase rankings to identify significant transactions...';

SELECT 
    fs.account_holder_key,
    dc.account_holder_name,
    dc.relationship_tier,
    dd.full_date AS transaction_date,
    fs.transaction_id,
    fs.transaction_amount,
    dp.product_name,
    dp.product_category,

    DENSE_RANK() OVER (
        PARTITION BY fs.account_holder_key 
        ORDER BY fs.transaction_amount DESC
    ) AS account_holder_transaction_rank

FROM fact_transactions fs
JOIN dim_account_holder dc ON fs.account_holder_key = dc.account_holder_key AND dc.is_current = 1
JOIN dim_banking_product dp ON fs.banking_product_key = dp.banking_product_key
JOIN dim_date dd ON fs.date_key = dd.date_key

ORDER BY fs.account_holder_key, account_holder_transaction_rank;

PRINT 'Account Holders ranking analysis completed';
