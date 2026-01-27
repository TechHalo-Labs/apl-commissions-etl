-- =============================================================================
-- Export: Broker Banking Information
-- Exports stg_broker_banking_infos to dbo.BrokerBankingInfos
-- Uses additive INSERT - does not update existing records
-- NOTE: This export is skipped if the staging table doesn't exist
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: Broker Banking Information';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Check if staging table exists
-- =============================================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_broker_banking_infos'
)
BEGIN
    PRINT 'SKIPPED: stg_broker_banking_infos table does not exist';
    PRINT 'Banking info is not currently migrated via ETL';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 2: Check current state
-- =============================================================================
DECLARE @before_count INT;
SELECT @before_count = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos];
PRINT 'BrokerBankingInfos before export: ' + CAST(@before_count AS VARCHAR);

DECLARE @staging_count INT;
SELECT @staging_count = COUNT(*) FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos];
PRINT 'BrokerBankingInfos in staging: ' + CAST(@staging_count AS VARCHAR);

-- =============================================================================
-- Step 2: Export new banking infos
-- Only export for brokers that exist in production AND don't already have banking info
-- =============================================================================
PRINT '';
PRINT 'Step 2: Exporting new broker banking infos...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos] (
    BrokerId,
    PaymentPreference,
    BankName,
    RoutingNumber,
    AccountNumber,
    AccountType,
    AccountHolderName,
    PayeeName,
    PayeeAddressLine1,
    PayeeAddressLine2,
    PayeeCity,
    PayeeState,
    PayeeZipCode,
    Notes,
    CreationTime,
    IsDeleted
)
SELECT
    sbi.BrokerId,
    sbi.PaymentPreference,
    sbi.BankName,
    sbi.RoutingNumber,
    sbi.AccountNumber,
    sbi.AccountType,
    sbi.AccountHolderName,
    sbi.PayeeName,
    sbi.PayeeAddressLine1,
    sbi.PayeeAddressLine2,
    sbi.PayeeCity,
    sbi.PayeeState,
    sbi.PayeeZipCode,
    sbi.Notes,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
WHERE 
    -- Broker must exist in production
    EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[Brokers] b WHERE b.Id = sbi.BrokerId)
    -- Don't create duplicate banking info for same broker
    AND NOT EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos] bbi WHERE bbi.BrokerId = sbi.BrokerId);

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'New BrokerBankingInfos exported: ' + CAST(@exported AS VARCHAR);

-- =============================================================================
-- Step 3: Report skipped records
-- =============================================================================
PRINT '';
PRINT 'Step 3: Reporting skipped records...';

-- Report staging records that couldn't be exported due to missing broker
DECLARE @no_broker_count INT;
SELECT @no_broker_count = COUNT(*)
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
WHERE NOT EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[Brokers] b WHERE b.Id = sbi.BrokerId);

IF @no_broker_count > 0
BEGIN
    PRINT 'WARNING: ' + CAST(@no_broker_count AS VARCHAR) + ' staging records skipped (broker not in production)';
    
    -- Show sample of skipped records
    SELECT TOP 10 
        sbi.BrokerId,
        sbi.AccountHolderName,
        'Broker not in dbo.Brokers' AS Reason
    FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
    WHERE NOT EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[Brokers] b WHERE b.Id = sbi.BrokerId)
    ORDER BY sbi.BrokerId;
END

-- Report staging records that were already in production
DECLARE @already_exists_count INT;
SELECT @already_exists_count = COUNT(*)
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
WHERE EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[Brokers] b WHERE b.Id = sbi.BrokerId)
  AND EXISTS (SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos] bbi WHERE bbi.BrokerId = sbi.BrokerId);

IF @already_exists_count > 0
BEGIN
    PRINT 'INFO: ' + CAST(@already_exists_count AS VARCHAR) + ' staging records skipped (banking info already exists)';
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

DECLARE @after_count INT;
SELECT @after_count = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos];
PRINT 'BrokerBankingInfos after export: ' + CAST(@after_count AS VARCHAR);
PRINT 'Net new records: ' + CAST(@after_count - @before_count AS VARCHAR);

-- Breakdown by account type
SELECT 'Production by Account Type' AS Metric, AccountType, COUNT(*) AS Cnt 
FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos]
GROUP BY AccountType
ORDER BY Cnt DESC;

-- Sample of newly exported records
PRINT '';
PRINT '=== Sample Exported Banking Records ===';
SELECT TOP 5 
    bbi.Id,
    bbi.BrokerId,
    b.Name AS BrokerName,
    bbi.PaymentPreference,
    bbi.BankName,
    bbi.RoutingNumber,
    LEFT(bbi.AccountNumber, 4) + '***' AS AccountNumberMasked,
    bbi.AccountType,
    bbi.AccountHolderName
FROM [$(PRODUCTION_SCHEMA)].[BrokerBankingInfos] bbi
INNER JOIN [$(PRODUCTION_SCHEMA)].[Brokers] b ON b.Id = bbi.BrokerId
ORDER BY bbi.Id DESC;

PRINT '';
PRINT '============================================================';
PRINT 'BROKER BANKING INFO EXPORT COMPLETED';
PRINT '============================================================';

GO
