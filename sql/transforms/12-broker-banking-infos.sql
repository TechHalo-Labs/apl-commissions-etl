-- =============================================================================
-- Transform: Broker Banking Information (T-SQL)
-- Extracts ACH banking details from raw_data.raw_individual and raw_data.raw_org1
-- Creates staging records for brokers who want ACH payment
-- Usage: sqlcmd -S server -d database -i sql/transforms/12-broker-banking-infos.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Broker Banking Information';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Create/truncate staging table
-- =============================================================================
PRINT 'Step 1: Setting up staging table...';

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[$(ETL_SCHEMA)].[stg_broker_banking_infos]') AND type in (N'U'))
BEGIN
    CREATE TABLE [$(ETL_SCHEMA)].[stg_broker_banking_infos] (
        Id BIGINT IDENTITY(1,1) NOT NULL,
        BrokerId BIGINT NOT NULL,
        PaymentPreference INT NOT NULL DEFAULT 0,
        BankName NVARCHAR(255),
        RoutingNumber NVARCHAR(9),
        AccountNumber NVARCHAR(17),
        AccountType NVARCHAR(50),
        AccountHolderName NVARCHAR(255),
        PayeeName NVARCHAR(255),
        PayeeAddressLine1 NVARCHAR(255),
        PayeeAddressLine2 NVARCHAR(255),
        PayeeCity NVARCHAR(100),
        PayeeState NVARCHAR(2),
        PayeeZipCode NVARCHAR(10),
        Notes NVARCHAR(MAX),
        CreationTime DATETIME2 DEFAULT GETUTCDATE(),
        IsDeleted BIT DEFAULT 0,
        CONSTRAINT PK_stg_broker_banking_infos PRIMARY KEY (Id)
    );
    PRINT 'Created [$(ETL_SCHEMA)].[stg_broker_banking_infos]';
END
ELSE
BEGIN
    TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_broker_banking_infos];
    PRINT 'Truncated [$(ETL_SCHEMA)].[stg_broker_banking_infos]';
END

-- =============================================================================
-- Step 2: Extract banking info from Individual brokers
-- Only include records where all 3 banking columns are populated
-- =============================================================================
PRINT '';
PRINT 'Step 2: Extracting banking info from Individual brokers...';

INSERT INTO [$(ETL_SCHEMA)].[stg_broker_banking_infos] (
    BrokerId,
    PaymentPreference,
    BankName,
    RoutingNumber,
    AccountNumber,
    AccountType,
    AccountHolderName,
    PayeeName,
    Notes,
    CreationTime,
    IsDeleted
)
SELECT
    -- BrokerId: Strip 'P' prefix and convert to BIGINT
    TRY_CAST(REPLACE(ri.PartyUniqueId, 'P', '') AS BIGINT) AS BrokerId,
    
    -- PaymentPreference: 1 = ACH (all 3 fields present)
    1 AS PaymentPreference,
    
    -- BankName: 'Unknown' as specified
    'Unknown' AS BankName,
    
    -- RoutingNumber: Left 9 chars (ABA routing numbers are 9 digits)
    LEFT(LTRIM(RTRIM(ri.BankRoutingNumber)), 9) AS RoutingNumber,
    
    -- AccountNumber: Left 17 chars (max length in target)
    LEFT(LTRIM(RTRIM(ri.AccountNumber)), 17) AS AccountNumber,
    
    -- AccountType: 'Checking' if starts with 'C', else 'Savings'
    CASE 
        WHEN LEFT(UPPER(LTRIM(RTRIM(ri.AccountType))), 1) = 'C' THEN 'Checking'
        ELSE 'Savings'
    END AS AccountType,
    
    -- AccountHolderName: FirstName + ' ' + LastName
    LTRIM(RTRIM(
        COALESCE(ri.IndividualFirstName, '') + 
        CASE WHEN ri.IndividualFirstName IS NOT NULL AND ri.IndividualLastName IS NOT NULL THEN ' ' ELSE '' END +
        COALESCE(ri.IndividualLastName, '')
    )) AS AccountHolderName,
    
    -- PayeeName: Same as AccountHolderName for individuals
    LTRIM(RTRIM(
        COALESCE(ri.IndividualFirstName, '') + 
        CASE WHEN ri.IndividualFirstName IS NOT NULL AND ri.IndividualLastName IS NOT NULL THEN ' ' ELSE '' END +
        COALESCE(ri.IndividualLastName, '')
    )) AS PayeeName,
    
    -- Notes: Source tracking
    'Source: raw_data.raw_individual - ETL import' AS Notes,
    
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted

FROM [raw_data].[raw_individual] ri
WHERE 
    -- All 3 banking columns must be populated
    ri.BankRoutingNumber IS NOT NULL AND LTRIM(RTRIM(ri.BankRoutingNumber)) <> ''
    AND ri.AccountNumber IS NOT NULL AND LTRIM(RTRIM(ri.AccountNumber)) <> ''
    AND ri.AccountType IS NOT NULL AND LTRIM(RTRIM(ri.AccountType)) <> ''
    -- Must have valid PartyUniqueId
    AND ri.PartyUniqueId IS NOT NULL AND ri.PartyUniqueId <> ''
    AND TRY_CAST(REPLACE(ri.PartyUniqueId, 'P', '') AS BIGINT) IS NOT NULL;

DECLARE @ind_count INT = @@ROWCOUNT;
PRINT 'Individual broker banking records staged: ' + CAST(@ind_count AS VARCHAR);

-- =============================================================================
-- Step 3: Extract banking info from Organization brokers
-- Only include records where all 3 banking columns are populated
-- =============================================================================
PRINT '';
PRINT 'Step 3: Extracting banking info from Organization brokers...';

INSERT INTO [$(ETL_SCHEMA)].[stg_broker_banking_infos] (
    BrokerId,
    PaymentPreference,
    BankName,
    RoutingNumber,
    AccountNumber,
    AccountType,
    AccountHolderName,
    PayeeName,
    Notes,
    CreationTime,
    IsDeleted
)
SELECT
    -- BrokerId: Strip 'P' prefix and convert to BIGINT
    TRY_CAST(REPLACE(ro.PartyUniqueId, 'P', '') AS BIGINT) AS BrokerId,
    
    -- PaymentPreference: 1 = ACH (all 3 fields present)
    1 AS PaymentPreference,
    
    -- BankName: 'Unknown' as specified
    'Unknown' AS BankName,
    
    -- RoutingNumber: Left 9 chars
    LEFT(LTRIM(RTRIM(ro.BankRoutingNumber)), 9) AS RoutingNumber,
    
    -- AccountNumber: Left 17 chars
    LEFT(LTRIM(RTRIM(ro.AccountNumber)), 17) AS AccountNumber,
    
    -- AccountType: 'Checking' if starts with 'C', else 'Savings'
    CASE 
        WHEN LEFT(UPPER(LTRIM(RTRIM(ro.AccountType))), 1) = 'C' THEN 'Checking'
        ELSE 'Savings'
    END AS AccountType,
    
    -- AccountHolderName: Organization Name
    LEFT(LTRIM(RTRIM(ro.OrganizationName)), 255) AS AccountHolderName,
    
    -- PayeeName: Organization Name
    LEFT(LTRIM(RTRIM(ro.OrganizationName)), 255) AS PayeeName,
    
    -- Notes: Source tracking
    'Source: raw_data.raw_org1 - ETL import' AS Notes,
    
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted

FROM [raw_data].[raw_org1] ro
WHERE 
    -- All 3 banking columns must be populated
    ro.BankRoutingNumber IS NOT NULL AND LTRIM(RTRIM(ro.BankRoutingNumber)) <> ''
    AND ro.AccountNumber IS NOT NULL AND LTRIM(RTRIM(ro.AccountNumber)) <> ''
    AND ro.AccountType IS NOT NULL AND LTRIM(RTRIM(ro.AccountType)) <> ''
    -- Must have valid PartyUniqueId
    AND ro.PartyUniqueId IS NOT NULL AND ro.PartyUniqueId <> ''
    AND TRY_CAST(REPLACE(ro.PartyUniqueId, 'P', '') AS BIGINT) IS NOT NULL
    -- Don't duplicate if already inserted from individuals (rare case)
    AND TRY_CAST(REPLACE(ro.PartyUniqueId, 'P', '') AS BIGINT) NOT IN (
        SELECT BrokerId FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos]
    );

DECLARE @org_count INT = @@ROWCOUNT;
PRINT 'Organization broker banking records staged: ' + CAST(@org_count AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Total Broker Banking Infos' AS Metric, COUNT(*) AS Cnt 
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos];

SELECT 'By Account Type' AS Metric, AccountType, COUNT(*) AS Cnt 
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos]
GROUP BY AccountType
ORDER BY Cnt DESC;

-- Sample records
PRINT '';
PRINT '=== Sample Individual Banking Records ===';
SELECT TOP 5 
    BrokerId, PaymentPreference, BankName, RoutingNumber, 
    LEFT(AccountNumber, 4) + '***' AS AccountNumberMasked,
    AccountType, AccountHolderName
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos]
WHERE Notes LIKE '%individual%'
ORDER BY BrokerId;

PRINT '';
PRINT '=== Sample Organization Banking Records ===';
SELECT TOP 5 
    BrokerId, PaymentPreference, BankName, RoutingNumber, 
    LEFT(AccountNumber, 4) + '***' AS AccountNumberMasked,
    AccountType, AccountHolderName
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos]
WHERE Notes LIKE '%org%'
ORDER BY BrokerId;

-- Check for brokers with banking info that exist in dbo.Brokers
PRINT '';
PRINT '=== Brokers with Banking Info that exist in production ===';
SELECT 'Staging banking infos with matching production broker' AS Metric, COUNT(*) AS Cnt
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
WHERE EXISTS (SELECT 1 FROM [dbo].[Brokers] b WHERE b.Id = sbi.BrokerId);

SELECT 'Staging banking infos WITHOUT matching production broker' AS Metric, COUNT(*) AS Cnt
FROM [$(ETL_SCHEMA)].[stg_broker_banking_infos] sbi
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Brokers] b WHERE b.Id = sbi.BrokerId);

PRINT '';
PRINT '============================================================';
PRINT 'BROKER BANKING INFO TRANSFORM COMPLETED';
PRINT '============================================================';

GO
