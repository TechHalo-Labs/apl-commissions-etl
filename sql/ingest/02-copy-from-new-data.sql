-- =============================================================================
-- Copy Source Data from new_data schema to etl.input_* tables
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COPY: Source Data from new_data to etl.input_* tables';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Copy IndividualRoster → input_individual_brokers
-- =============================================================================
PRINT 'Step 1: Copying IndividualRoster...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_individual_brokers];

INSERT INTO [$(ETL_SCHEMA)].[input_individual_brokers] (
    UniquePartyId,
    ExternalPartyId,
    PartyUniqueID,
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    EmailAddress,
    PhoneNumber,
    Address1,
    Address2,
    City,
    [State],
    ZipCode,
    County,
    PartyStatus,
    RosterEffectiveDate,
    NPNNumber,
    SSN_TIN,
    BirthDate
)
SELECT 
    UniquePartyId,
    ExternalPartyId,
    PartyUniqueID,
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    EmailAddress,
    PhoneNumber,
    Address1,
    Address2,
    City,
    [State],
    ZipCode,
    County,
    PartyStatus,
    RosterEffectiveDate,
    NPNNumber,
    SSN_TIN,
    BirthDate
FROM [new_data].[IndividualRoster];

DECLARE @ind_count INT = @@ROWCOUNT;
PRINT '  ✅ Copied: ' + FORMAT(@ind_count, 'N0') + ' individual brokers';

-- =============================================================================
-- Step 2: Copy OrganizationRoster → input_organization_brokers
-- =============================================================================
PRINT '';
PRINT 'Step 2: Copying OrganizationRoster...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_organization_brokers];

INSERT INTO [$(ETL_SCHEMA)].[input_organization_brokers] (
    UniquePartyId,
    ExternalPartyId,
    PartyUniqueID,
    OrganizationName,
    EmailAddress,
    PhoneNumber,
    Address1,
    Address2,
    City,
    [State],
    ZipCode,
    County,
    PartyStatus,
    RosterEffectiveDate,
    NPNNumber,
    FEINNumber
)
SELECT 
    UniquePartyId,
    ExternalPartyId,
    PartyUniqueID,
    OrganizationName,
    EmailAddress,
    PhoneNumber,
    Address1,
    Address2,
    City,
    [State],
    ZipCode,
    County,
    PartyStatus,
    RosterEffectiveDate,
    NPNNumber,
    FEINNumber
FROM [new_data].[OrganizationRoster];

DECLARE @org_count INT = @@ROWCOUNT;
PRINT '  ✅ Copied: ' + FORMAT(@org_count, 'N0') + ' organization brokers';

-- =============================================================================
-- Step 3: Copy CertificateInfo → input_certificate_info
-- =============================================================================
PRINT '';
PRINT 'Step 3: Copying CertificateInfo...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_certificate_info];

INSERT INTO [$(ETL_SCHEMA)].[input_certificate_info] (
    CertificateInfoId,
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
    CaseSize
)
SELECT 
    CertificateInfoId,
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
    CaseSize
FROM [new_data].[CertificateInfo];

DECLARE @cert_count INT = @@ROWCOUNT;
PRINT '  ✅ Copied: ' + FORMAT(@cert_count, 'N0') + ' certificate records';

-- =============================================================================
-- Step 4: Copy CommHierarchy → input_commission_hierarchy
-- =============================================================================
PRINT '';
PRINT 'Step 4: Copying CommHierarchy...';

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
FROM [new_data].[CommHierarchy];

DECLARE @hier_count INT = @@ROWCOUNT;
PRINT '  ✅ Copied: ' + FORMAT(@hier_count, 'N0') + ' hierarchy records';

-- =============================================================================
-- Step 5: Copy PerfGroupModel → input_perf_groups
-- =============================================================================
PRINT '';
PRINT 'Step 5: Copying PerfGroupModel...';

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
PRINT '  ✅ Copied: ' + FORMAT(@group_count, 'N0') + ' group records';

-- =============================================================================
-- Step 6: Copy CommissionsDetail → input_commissions_detail
-- =============================================================================
PRINT '';
PRINT 'Step 6: Copying CommissionsDetail...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[input_commissions_detail];

INSERT INTO [$(ETL_SCHEMA)].[input_commissions_detail] (
    PolicyNumber,
    GroupNumber,
    CertificateNumber,
    ProductCode,
    Premium,
    TransactionDate,
    TransactionType,
    BrokerId,
    BrokerName,
    CommissionAmount,
    CommissionPercent,
    [Level],
    LevelName
)
SELECT 
    PolicyNumber,
    GroupNumber,
    CertificateNumber,
    ProductCode,
    Premium,
    TransactionDate,
    TransactionType,
    BrokerId,
    BrokerName,
    CommissionAmount,
    CommissionPercent,
    [Level],
    LevelName
FROM [new_data].[CommissionsDetail];

DECLARE @comm_count INT = @@ROWCOUNT;
PRINT '  ✅ Copied: ' + FORMAT(@comm_count, 'N0') + ' commission detail records';

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

PRINT '';
PRINT 'Input tables populated:';
SELECT 'input_individual_brokers' as tbl, COUNT(*) as cnt FROM [$(ETL_SCHEMA)].[input_individual_brokers]
UNION ALL SELECT 'input_organization_brokers', COUNT(*) FROM [$(ETL_SCHEMA)].[input_organization_brokers]
UNION ALL SELECT 'input_certificate_info', COUNT(*) FROM [$(ETL_SCHEMA)].[input_certificate_info]
UNION ALL SELECT 'input_commission_hierarchy', COUNT(*) FROM [$(ETL_SCHEMA)].[input_commission_hierarchy]
UNION ALL SELECT 'input_perf_groups', COUNT(*) FROM [$(ETL_SCHEMA)].[input_perf_groups]
UNION ALL SELECT 'input_commissions_detail', COUNT(*) FROM [$(ETL_SCHEMA)].[input_commissions_detail]
ORDER BY 1;

PRINT '';
PRINT '============================================================';
PRINT 'DATA COPY COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT '✅ ALL source data copied from new_data schema';
PRINT '';
PRINT 'Next: Run transform pipeline with: npx tsx scripts/run-pipeline.ts --skip-ingest --skip-calc --skip-export';
PRINT '';

GO
