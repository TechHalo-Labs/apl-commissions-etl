-- =============================================================================
-- INGEST: Populate Input Tables from Raw Data
-- =============================================================================
-- Populates input_* tables from raw_* tables
-- This is Phase 2 of the data ingest process
-- Input tables are used by transform scripts
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'INGEST: Populate Input Tables';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. input_certificate_info (Primary input for transforms)
-- =============================================================================
PRINT '1. Populating input_certificate_info from raw_certificate_info...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_certificate_info];

INSERT INTO [$(ETL_SCHEMA)].[input_certificate_info] (
    Company,
    ProductMasterCategory,
    ProductCategory,
    GroupId,
    Product,
    PlanCode,
    CertificateId,
    CertEffectiveDate,
    CertIssuedState,
    CertStatus,
    CertPremium,
    CertSplitSeq,
    CertSplitPercent,
    CustomerId,
    RecStatus,
    HierDriver,
    HierVersion,
    CommissionsSchedule,
    CommissionType,
    WritingBrokerID,
    SplitBrokerId,
    SplitBrokerSeq,
    ReassignedType,
    PaidBrokerId
)
SELECT 
    Company,
    ProductMasterCategory,
    ProductCategory,
    GroupId,
    Product,
    PlanCode,
    TRY_CAST(NULLIF(CertificateId, 'NULL') AS BIGINT),
    TRY_CAST(NULLIF(CertEffectiveDate, 'NULL') AS DATE),
    CertIssuedState,
    CertStatus,
    TRY_CAST(NULLIF(CertPremium, 'NULL') AS DECIMAL(18,4)),
    TRY_CAST(NULLIF(CertSplitSeq, 'NULL') AS INT),
    TRY_CAST(NULLIF(CertSplitPercent, 'NULL') AS DECIMAL(18,4)),
    CustomerId,
    RecStatus,
    HierDriver,
    HierVersion,
    CommissionsSchedule,
    CommissionType,
    WritingBrokerID,
    SplitBrokerId,
    COALESCE(TRY_CAST(NULLIF(SplitBrokerSeq, 'NULL') AS INT), 1),
    ReassignedType,
    PaidBrokerId
FROM [$(ETL_SCHEMA)].[raw_certificate_info];

DECLARE @cert_rows INT = @@ROWCOUNT;
PRINT '   ✅ Populated: ' + FORMAT(@cert_rows, 'N0') + ' rows';

-- Verify schedule references
DECLARE @unique_schedules INT;
SELECT @unique_schedules = COUNT(DISTINCT CommissionsSchedule)
FROM [$(ETL_SCHEMA)].[input_certificate_info]
WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule <> '';

PRINT '   📊 Unique schedules referenced: ' + FORMAT(@unique_schedules, 'N0');

-- Verify writing brokers
DECLARE @unique_brokers INT;
SELECT @unique_brokers = COUNT(DISTINCT WritingBrokerID)
FROM [$(ETL_SCHEMA)].[input_certificate_info]
WHERE WritingBrokerID IS NOT NULL AND WritingBrokerID <> '';

PRINT '   📊 Unique writing brokers: ' + FORMAT(@unique_brokers, 'N0');

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION - Input Tables';
PRINT '============================================================';

SELECT 
    'input_certificate_info' AS [table],
    COUNT(*) AS total_rows,
    COUNT(DISTINCT GroupId) AS unique_groups,
    COUNT(DISTINCT CertSplitSeq) AS unique_split_sequences,
    COUNT(DISTINCT CommissionsSchedule) AS unique_schedules
FROM [$(ETL_SCHEMA)].[input_certificate_info];

-- Sample data check
SELECT TOP 5
    CertificateId,
    GroupId,
    CertSplitSeq,
    CommissionsSchedule,
    WritingBrokerID,
    CertEffectiveDate
FROM [$(ETL_SCHEMA)].[input_certificate_info]
WHERE CommissionsSchedule IS NOT NULL
  AND CommissionsSchedule <> ''
ORDER BY CertificateId;

PRINT '';
PRINT '============================================================';
PRINT 'INPUT TABLES POPULATED';
PRINT '============================================================';
PRINT '';
PRINT 'Ready for transform phase (04-schedules.sql will now find schedules!)';
PRINT '';

GO
