-- =============================================================================
-- INGEST: Copy from new_data to etl (Latest Data)
-- =============================================================================
-- Copies latest data from new_data schema to etl working schema
-- new_data has 1.7M certificates (latest source)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'INGEST: Copy Latest Data (new_data → etl)';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. CertificateInfo → raw_certificate_info
-- =============================================================================
DECLARE @cert_count BIGINT;
SELECT @cert_count = COUNT(*) FROM [new_data].[CertificateInfo];

PRINT '1. CertificateInfo: ' + FORMAT(@cert_count, 'N0') + ' rows';

TRUNCATE TABLE [etl].[raw_certificate_info];

INSERT INTO [etl].[raw_certificate_info] (
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
    CAST(CertificateId AS NVARCHAR(100)),
    CAST(CertEffectiveDate AS NVARCHAR(50)),
    CertIssuedState,
    CertStatus,
    CAST(CertPremium AS NVARCHAR(50)),
    CAST(CertSplitSeq AS NVARCHAR(50)),
    CAST(CertSplitPercent AS NVARCHAR(50)),
    CustomerId,
    RecStatus,
    HierDriver,
    CAST(HierVersion AS NVARCHAR(100)),
    CommissionsSchedule,
    CommissionType,
    WritingBrokerID,
    SplitBrokerId,
    CAST(SplitBrokerSeq AS NVARCHAR(50)),
    ReassignedType,
    PaidBrokerId
FROM [new_data].[CertificateInfo];

PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';

-- Verify schedule references
DECLARE @unique_schedules INT;
SELECT @unique_schedules = COUNT(DISTINCT CommissionsSchedule)
FROM [etl].[raw_certificate_info]
WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule <> '';

PRINT '   📊 Unique schedules referenced: ' + FORMAT(@unique_schedules, 'N0');

-- Verify groups
DECLARE @unique_groups INT;
SELECT @unique_groups = COUNT(DISTINCT GroupId)
FROM [etl].[raw_certificate_info]
WHERE GroupId IS NOT NULL AND GroupId <> '';

PRINT '   📊 Unique groups: ' + FORMAT(@unique_groups, 'N0');

-- =============================================================================
-- 2. PerfGroupModel → raw_perf_groups
-- =============================================================================
PRINT '';
DECLARE @group_count BIGINT;
SELECT @group_count = COUNT(*) FROM [new_data].[PerfGroupModel];

PRINT '2. PerfGroupModel: ' + FORMAT(@group_count, 'N0') + ' rows';

TRUNCATE TABLE [etl].[raw_perf_groups];

INSERT INTO [etl].[raw_perf_groups] (
    GroupNum,
    GroupName,
    StateAbbreviation,
    GroupSize,
    BrokerUniqueId,
    BrokerReportsToUniqueId,
    AgencyName,
    Product,
    EffectiveDate,
    FundingType,
    CommissionTable,
    FeeAgreement
)
SELECT 
    GroupNum,
    GroupName,
    StateAbbreviation,
    GroupSize,
    BrokerUniqueId,
    BrokerReportsToUniqueId,
    AgencyName,
    Product,
    EffectiveDate,
    FUndingType,  -- Note: typo in source table
    CommissionTable,
    FeeAgreement
FROM [new_data].[PerfGroupModel];

PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' rows';

-- =============================================================================
-- 3. IndividualRoster + OrganizationRoster → raw_individual_brokers, raw_org_brokers
-- =============================================================================
PRINT '';
DECLARE @ind_count BIGINT;
SELECT @ind_count = COUNT(*) FROM [new_data].[IndividualRoster];

PRINT '3. IndividualRoster: ' + FORMAT(@ind_count, 'N0') + ' rows';

TRUNCATE TABLE [etl].[raw_individual_brokers];

INSERT INTO [etl].[raw_individual_brokers] (
    PartyUniqueId,
    IndividualLastName,
    IndividualFirstName,
    HireDate,
    EmailAddress,
    CurrentStatus,
    BrokerType
)
SELECT 
    PartyUniqueId,
    IndividualLastName,
    IndividualFirstName,
    HireDate,
    EmailAddress,
    CurrentStatus,
    'Individual' as BrokerType
FROM [new_data].[IndividualRoster];

PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' individual brokers';

PRINT '';
DECLARE @org_count BIGINT;
SELECT @org_count = COUNT(*) FROM [new_data].[OrganizationRoster];

PRINT '4. OrganizationRoster: ' + FORMAT(@org_count, 'N0') + ' rows';

TRUNCATE TABLE [etl].[raw_org_brokers];

INSERT INTO [etl].[raw_org_brokers] (
    PartyUniqueId,
    OrganizationName,
    HireDate,
    EmailAddress,
    CurrentStatus
)
SELECT 
    PartyUniqueId,
    OrganizationName,
    HireDate,
    EmailAddress,
    CurrentStatus
FROM [new_data].[OrganizationRoster];

PRINT '   ✅ Copied: ' + FORMAT(@@ROWCOUNT, 'N0') + ' organization brokers';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION - Raw Tables in ETL Schema';
PRINT '============================================================';

SELECT 'raw_certificate_info' AS [table], COUNT(*) AS row_count FROM [etl].[raw_certificate_info];
SELECT 'raw_schedule_rates' AS [table], COUNT(*) AS row_count FROM [etl].[raw_schedule_rates];
SELECT 'raw_perf_groups' AS [table], COUNT(*) AS row_count FROM [etl].[raw_perf_groups];
SELECT 'raw_individual_brokers' AS [table], COUNT(*) AS row_count FROM [etl].[raw_individual_brokers];
SELECT 'raw_org_brokers' AS [table], COUNT(*) AS row_count FROM [etl].[raw_org_brokers];

PRINT '';
PRINT '============================================================';
PRINT 'RAW DATA COPY COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ Latest data copied from new_data schema (1.7M certificates)';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run: sql/ingest/populate-input-tables.sql';
PRINT '  2. Run: npx tsx scripts/proposal-builder.ts';
PRINT '  3. Run: npx tsx scripts/run-pipeline.ts --skip-ingest';
PRINT '';

GO
