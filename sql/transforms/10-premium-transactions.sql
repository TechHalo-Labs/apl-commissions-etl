-- =============================================================================
-- Transform: Premium Transactions (T-SQL)
-- Creates premium transactions from raw_premiums
-- Matches the existing stg_premium_transactions schema:
--   Id, CertificateId, TransactionDate, PremiumAmount, BillingPeriodStart,
--   BillingPeriodEnd, PaymentStatus, SourceSystem, CreatedDate, IsDeleted
-- Usage: sqlcmd -S server -d database -i sql/transforms/10-premium-transactions.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Premium Transactions';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Truncate staging table
-- =============================================================================
PRINT 'Step 1: Truncating stg_premium_transactions...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_transactions];

-- =============================================================================
-- Step 2: Insert premium transactions from raw_premiums
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating premium transactions from raw_premiums...';

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_transactions] (
    Id, CertificateId, TransactionDate, PremiumAmount, 
    BillingPeriodStart, BillingPeriodEnd, PaymentStatus, SourceSystem, 
    CreatedDate, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.Policy, p.DatePost) AS Id,
    TRY_CAST(p.Policy AS BIGINT) AS CertificateId,
    TRY_CAST(p.DatePost AS DATE) AS TransactionDate,
    TRY_CAST(p.Amount AS DECIMAL(18,2)) AS PremiumAmount,
    TRY_CAST(p.DatePaidTo AS DATE) AS BillingPeriodStart,
    DATEADD(MONTH, 1, TRY_CAST(p.DatePaidTo AS DATE)) AS BillingPeriodEnd,
    'Completed' AS PaymentStatus,
    'raw_premiums' AS SourceSystem,
    GETUTCDATE() AS CreatedDate,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[raw_premiums] p
WHERE p.Policy IS NOT NULL 
  AND LTRIM(RTRIM(p.Policy)) <> ''
  AND TRY_CAST(p.Amount AS DECIMAL(18,2)) IS NOT NULL;

DECLARE @prem_count INT = @@ROWCOUNT;
PRINT 'Premium transactions created: ' + CAST(@prem_count AS VARCHAR);

GO

-- =============================================================================
-- Step 3: Also create premium transactions for policies without premiums
-- (using CertPremium from stg_policies)
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating premium transactions for policies without raw_premiums...';

-- Use 10000000 offset to avoid collision with IDs from raw_premiums
-- (raw_premiums has ~138K rows, policies could have IDs up to 2M+)
DECLARE @offset BIGINT = 10000000;
PRINT 'Using offset for policy-based premiums: ' + CAST(@offset AS VARCHAR);

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_transactions] (
    Id, CertificateId, TransactionDate, PremiumAmount, 
    BillingPeriodStart, BillingPeriodEnd, PaymentStatus, SourceSystem, 
    CreatedDate, IsDeleted
)
SELECT
    @offset + ROW_NUMBER() OVER (ORDER BY pol.Id) AS Id,
    pol.Id AS CertificateId,
    pol.EffectiveDate AS TransactionDate,
    pol.Premium AS PremiumAmount,
    pol.EffectiveDate AS BillingPeriodStart,
    DATEADD(MONTH, 1, pol.EffectiveDate) AS BillingPeriodEnd,
    'Completed' AS PaymentStatus,
    'stg_policies' AS SourceSystem,
    GETUTCDATE() AS CreatedDate,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_policies] pol
WHERE NOT EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_transactions] pt 
    WHERE pt.CertificateId = pol.Id
)
  AND pol.Premium > 0;

DECLARE @extra_count INT = @@ROWCOUNT;
PRINT 'Additional premium transactions from policies: ' + CAST(@extra_count AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Total premium transactions' AS metric, COUNT(*) AS cnt 
FROM [$(ETL_SCHEMA)].[stg_premium_transactions];

SELECT 'Premium amount distribution' AS metric,
       SUM(CASE WHEN PremiumAmount <= 0 THEN 1 ELSE 0 END) AS zero_or_negative,
       SUM(CASE WHEN PremiumAmount > 0 AND PremiumAmount < 100 THEN 1 ELSE 0 END) AS under_100,
       SUM(CASE WHEN PremiumAmount >= 100 AND PremiumAmount < 500 THEN 1 ELSE 0 END) AS [100_to_500],
       SUM(CASE WHEN PremiumAmount >= 500 THEN 1 ELSE 0 END) AS over_500
FROM [$(ETL_SCHEMA)].[stg_premium_transactions];

-- Sample transactions
SELECT TOP 10 
    Id, CertificateId, TransactionDate, PremiumAmount
FROM [$(ETL_SCHEMA)].[stg_premium_transactions]
ORDER BY Id;

PRINT '';
PRINT '============================================================';
PRINT 'PREMIUM TRANSACTIONS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

