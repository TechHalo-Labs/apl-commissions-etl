-- =============================================================================
-- Map new_data schema to etl.input_* tables with correct column mappings
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'MAPPING: new_data to etl.input_* tables';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Map CertificateInfo → input_certificate_info
-- =============================================================================
PRINT 'Step 1: Mapping CertificateInfo...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_certificate_info];

INSERT INTO [$(ETL_SCHEMA)].[input_certificate_info] (
    CertificateId,
    PolicyNumber,
    GroupId,
    CustomerId,
    CustomerName,
    CertificateNumber,
    InsuredName,
    ProductCode,
    ProductName,
    PlanCode,
    [State],
    SitusState,
    CertificateEffectiveDate,
    CertificateTerminationDate,
    PolicyEffectiveDate,
    PolicyTerminationDate,
    Tier,
    Volume,
    Premium,
    AnnualPremium,
    PolicyStatus,
    Occupancy,
    Underwriting,
    CommissionsSchedule,
    SalesClass,
    CaseSize,
    -- Additional fields needed by proposal-builder
    CertStatus,
    RecStatus,
    CertPremium,
    CertSplitSeq,
    CertSplitPercent,
    SplitBrokerSeq,
    SplitBrokerId,
    PaidBrokerId,
    CertEffectiveDate,
    CertIssuedState,
    Product
)
SELECT 
    Id AS CertificateId,
    NULL AS PolicyNumber,  -- Not in source
    LTRIM(RTRIM(GroupId)) AS GroupId,
    CustomerId,
    NULL AS CustomerName,  -- Not in source
    CAST(CertificateId AS NVARCHAR(100)) AS CertificateNumber,
    NULL AS InsuredName,  -- Not in source
    LTRIM(RTRIM(Product)) AS ProductCode,
    ProductCategory AS ProductName,
    LTRIM(RTRIM(ISNULL(PlanCode, ''))) AS PlanCode,
    CertIssuedState AS [State],
    CertIssuedState AS SitusState,
    CertEffectiveDate AS CertificateEffectiveDate,
    NULL AS CertificateTerminationDate,  -- Not in source
    CertEffectiveDate AS PolicyEffectiveDate,
    NULL AS PolicyTerminationDate,  -- Not in source
    NULL AS Tier,  -- Not in source
    NULL AS Volume,  -- Not in source
    CertPremium AS Premium,
    NULL AS AnnualPremium,  -- Not in source
    CertStatus AS PolicyStatus,
    NULL AS Occupancy,  -- Not in source
    NULL AS Underwriting,  -- Not in source
    CommissionsSchedule,
    NULL AS SalesClass,  -- Not in source
    NULL AS CaseSize,  -- Not in source
    -- Additional fields
    CertStatus,
    RecStatus,
    CertPremium,
    CertSplitSeq,
    CertSplitPercent,
    SplitBrokerSeq,
    SplitBrokerId,
    PaidBrokerId,
    CertEffectiveDate,
    CertIssuedState,
    Product
FROM [new_data].[CertificateInfo]
WHERE CertStatus = 'A'
  AND RecStatus IN ('A', 'C')
  AND CertEffectiveDate IS NOT NULL;

DECLARE @cert_count INT = @@ROWCOUNT;
PRINT '  ✅ Mapped: ' + FORMAT(@cert_count, 'N0') + ' certificate records';

-- =============================================================================
-- Step 2: Map CommHierarchy → input_commission_hierarchy
-- =============================================================================
PRINT '';
PRINT 'Step 2: Mapping CommHierarchy...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_commission_hierarchy];

INSERT INTO [$(ETL_SCHEMA)].[input_commission_hierarchy] (
    CertSplitSeq,
    PolicyNumber,
    PolicyEffDate,
    GroupNumber,
    CertEffDate,
    Product,
    Premium,
    SplitPercent,
    SplitBrokerId,
    SplitBrokerName,
    SplitBrokerStatus,
    SplitBrokerNPN,
    [Level],
    LevelName,
    PaidBrokerId,
    PaidBrokerName,
    PaidBrokerStatus,
    PaidBrokerNPN,
    CommissionsSchedule,
    CommPercent
)
SELECT 
    CertificateSplitSequence AS CertSplitSeq,
    PolicyNumber,
    PolicyEffectiveDate AS PolicyEffDate,
    GroupNumber,
    CertificateEffectiveDate AS CertEffDate,
    ProductCode AS Product,
    PremiumAmount AS Premium,
    SplitPercentage AS SplitPercent,
    SplitBrokerId,
    SplitBrokerName,
    NULL AS SplitBrokerStatus,  -- Not in source
    SplitBrokerNPN,
    [Level],
    LevelName,
    PaidBrokerId,
    PaidBrokerName,
    NULL AS PaidBrokerStatus,  -- Not in source
    PaidBrokerNPN,
    CommissionsScheduleName AS CommissionsSchedule,
    CommissionPercentage AS CommPercent
FROM [new_data].[CommHierarchy];

DECLARE @hier_count INT = @@ROWCOUNT;
PRINT '  ✅ Mapped: ' + FORMAT(@hier_count, 'N0') + ' hierarchy records';

-- =============================================================================
-- Step 3: Map PerfGroupModel → input_perf_groups
-- =============================================================================
PRINT '';
PRINT 'Step 3: Mapping PerfGroupModel...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_perf_groups];

INSERT INTO [$(ETL_SCHEMA)].[input_perf_groups] (
    GroupNumber,
    GroupName,
    SitusState,
    GroupSize,
    BrokerUniqueId,
    Status,
    EffectiveDate,
    TerminationDate
)
SELECT 
    GroupNumber,
    GroupName,
    SitusState,
    GroupSize,
    BrokerUniqueId,
    [Status],
    EffectiveDate,
    TerminationDate
FROM [new_data].[PerfGroupModel];

DECLARE @group_count INT = @@ROWCOUNT;
PRINT '  ✅ Mapped: ' + FORMAT(@group_count, 'N0') + ' group records';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

PRINT '';
PRINT 'Input tables populated:';
SELECT 'input_certificate_info' as tbl, COUNT(*) as cnt FROM [$(ETL_SCHEMA)].[input_certificate_info]
UNION ALL SELECT 'input_commission_hierarchy', COUNT(*) FROM [$(ETL_SCHEMA)].[input_commission_hierarchy]
UNION ALL SELECT 'input_perf_groups', COUNT(*) FROM [$(ETL_SCHEMA)].[input_perf_groups]
ORDER BY 1;

PRINT '';
PRINT 'Sample certificate data:';
SELECT TOP 5 
    CertificateId,
    GroupId,
    Product,
    PlanCode,
    CertEffectiveDate,
    CertStatus,
    CertSplitSeq
FROM [$(ETL_SCHEMA)].[input_certificate_info]
ORDER BY GroupId, CertEffectiveDate, Product;

PRINT '';
PRINT '============================================================';
PRINT 'MAPPING COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ Source data mapped to input tables';
PRINT '';
PRINT 'Next: Run TypeScript proposal-builder: npx tsx scripts/proposal-builder.ts';
PRINT '';

GO
