-- =============================================================================
-- Transform: Proposals - Step 2: Non-Conformant (Multiple Configs per Key)
-- 
-- Non-conformant = Same (Group, EffDate, Product, Plan) has 2+ different configs
-- These certificates cannot have a single proposal - each needs PolicyHierarchyAssignment
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 2: Non-Conformant Certificates';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Identify non-conformant keys
-- A key is non-conformant if it has 2+ distinct ConfigJson values
-- =============================================================================
PRINT 'Step 1: Identifying non-conformant keys...';

DROP TABLE IF EXISTS [etl].[non_conformant_keys];

SELECT 
    GroupId,
    EffectiveDate,
    ProductCode,
    PlanCode,
    COUNT(DISTINCT ConfigJson) AS DistinctConfigs,
    COUNT(*) AS CertCount
INTO [etl].[non_conformant_keys]
FROM [etl].[cert_split_configs_remainder]
GROUP BY GroupId, EffectiveDate, ProductCode, PlanCode
HAVING COUNT(DISTINCT ConfigJson) > 1;

DECLARE @nc_keys INT = @@ROWCOUNT;
PRINT 'Non-conformant keys found: ' + CAST(@nc_keys AS VARCHAR);

-- =============================================================================
-- Step 2: Get all certificates that belong to non-conformant keys
-- =============================================================================
PRINT '';
PRINT 'Step 2: Extracting non-conformant certificates...';

DROP TABLE IF EXISTS [etl].[non_conformant_certs];

SELECT csc.*
INTO [etl].[non_conformant_certs]
FROM [etl].[cert_split_configs_remainder] csc
INNER JOIN [etl].[non_conformant_keys] nck
    ON csc.GroupId = nck.GroupId
    AND csc.EffectiveDate = nck.EffectiveDate
    AND csc.ProductCode = nck.ProductCode
    AND csc.PlanCode = nck.PlanCode;

DECLARE @nc_certs INT = @@ROWCOUNT;
PRINT 'Non-conformant certificates: ' + CAST(@nc_certs AS VARCHAR);

-- =============================================================================
-- Step 3: Create PolicyHierarchyAssignment records
-- Each certificate gets ONE assignment record (using first split's writing broker)
-- Schema: Id, PolicyId, CertificateId, HierarchyId, SplitPercent, WritingBrokerId,
--         SplitSequence, IsNonConforming, NonConformantReason, SourceTraceabilityReportId
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating PolicyHierarchyAssignment records...';

TRUNCATE TABLE [etl].[stg_policy_hierarchy_assignments];

-- One record per certificate (get writing broker from first split, level 1)
INSERT INTO [etl].[stg_policy_hierarchy_assignments] (
    Id,
    PolicyId,
    CertificateId,
    HierarchyId,
    SplitPercent,
    WritingBrokerId,
    SplitSequence,
    IsNonConforming,
    NonConformantReason,
    SourceTraceabilityReportId,
    CreationTime,
    IsDeleted
)
SELECT
    CONCAT('PHA-', ncc.CertificateId) AS Id,
    ncc.CertificateId AS PolicyId,
    TRY_CAST(ncc.CertificateId AS BIGINT) AS CertificateId,
    NULL AS HierarchyId,
    100.00 AS SplitPercent,  -- Total split
    TRY_CAST(REPLACE(JSON_VALUE(ncc.ConfigJson, '$[0].brokerId'), 'P', '') AS BIGINT) AS WritingBrokerId,
    1 AS SplitSequence,
    1 AS IsNonConforming,
    'Multiple configs for same (Group, Date, Product, Plan) key' AS NonConformantReason,
    NULL AS SourceTraceabilityReportId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[non_conformant_certs] ncc;

DECLARE @pha_created INT = @@ROWCOUNT;
PRINT 'PolicyHierarchyAssignment records created: ' + CAST(@pha_created AS VARCHAR);

-- =============================================================================
-- Step 4: Create PolicyHierarchyParticipants from the config JSON
-- Schema: Id, PolicyHierarchyAssignmentId, BrokerId, BrokerName, Level, 
--         CommissionRate, ScheduleCode, ScheduleId, ReassignedType, PaidBrokerId
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating PolicyHierarchyParticipants...';

TRUNCATE TABLE [etl].[stg_policy_hierarchy_participants];

INSERT INTO [etl].[stg_policy_hierarchy_participants] (
    Id,
    PolicyHierarchyAssignmentId,
    BrokerId,
    BrokerName,
    [Level],
    CommissionRate,
    ScheduleCode,
    ScheduleId,
    ReassignedType,
    PaidBrokerId,
    CreationTime,
    IsDeleted
)
SELECT
    CONCAT('PHP-', ncc.CertificateId, '-', p.splitSeq, '-', p.[level]) AS Id,
    CONCAT('PHA-', ncc.CertificateId) AS PolicyHierarchyAssignmentId,
    TRY_CAST(REPLACE(p.brokerId, 'P', '') AS BIGINT) AS BrokerId,
    b.Name AS BrokerName,
    TRY_CAST(p.[level] AS INT) AS [Level],
    TRY_CAST(p.[percent] AS DECIMAL(18,2)) AS CommissionRate,
    p.schedule AS ScheduleCode,
    NULL AS ScheduleId,
    NULL AS ReassignedType,
    NULL AS PaidBrokerId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[non_conformant_certs] ncc
CROSS APPLY OPENJSON(ncc.ConfigJson)
WITH (
    splitSeq NVARCHAR(10) '$.splitSeq',
    [level] NVARCHAR(10) '$.level',
    brokerId NVARCHAR(20) '$.brokerId',
    [percent] NVARCHAR(20) '$.percent',
    schedule NVARCHAR(50) '$.schedule'
) p
LEFT JOIN [etl].[stg_brokers] b ON b.Id = TRY_CAST(REPLACE(p.brokerId, 'P', '') AS BIGINT)
WHERE p.brokerId IS NOT NULL;

DECLARE @php_created INT = @@ROWCOUNT;
PRINT 'PolicyHierarchyParticipants created: ' + CAST(@php_created AS VARCHAR);

-- =============================================================================
-- Step 5: Create new remainder table excluding non-conformant
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating conformant remainder table...';

DROP TABLE IF EXISTS [etl].[cert_split_configs_conformant];

SELECT csc.*
INTO [etl].[cert_split_configs_conformant]
FROM [etl].[cert_split_configs_remainder] csc
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[non_conformant_keys] nck
    WHERE csc.GroupId = nck.GroupId
      AND csc.EffectiveDate = nck.EffectiveDate
      AND csc.ProductCode = nck.ProductCode
      AND csc.PlanCode = nck.PlanCode
);

DECLARE @conformant_certs INT = @@ROWCOUNT;
PRINT 'Conformant certificates remaining: ' + CAST(@conformant_certs AS VARCHAR);

DECLARE @conformant_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [etl].[cert_split_configs_conformant]);
PRINT 'Conformant groups remaining: ' + CAST(@conformant_groups AS VARCHAR);

DECLARE @conformant_keys INT = (
    SELECT COUNT(*) FROM (
        SELECT DISTINCT GroupId, EffectiveDate, ProductCode, PlanCode 
        FROM [etl].[cert_split_configs_conformant]
    ) k
);
PRINT 'Conformant keys remaining: ' + CAST(@conformant_keys AS VARCHAR);

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';
PRINT '';
PRINT 'INPUT (from Step 1 remainder):';

DECLARE @input_certs INT = (SELECT COUNT(*) FROM [etl].[cert_split_configs_remainder]);
DECLARE @input_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [etl].[cert_split_configs_remainder]);
PRINT '  Certificates: ' + CAST(@input_certs AS VARCHAR);
PRINT '  Groups: ' + CAST(@input_groups AS VARCHAR);
PRINT '';
PRINT 'NON-CONFORMANT (routed to PolicyHierarchyAssignments):';
PRINT '  Keys with multiple configs: ' + CAST(@nc_keys AS VARCHAR);
PRINT '  Certificates: ' + CAST(@nc_certs AS VARCHAR);
PRINT '  PolicyHierarchyAssignment records: ' + CAST(@pha_created AS VARCHAR);
PRINT '  PolicyHierarchyParticipants: ' + CAST(@php_created AS VARCHAR);
PRINT '';
PRINT 'CONFORMANT (remaining for proposal creation):';
PRINT '  Certificates: ' + CAST(@conformant_certs AS VARCHAR);
PRINT '  Groups: ' + CAST(@conformant_groups AS VARCHAR);
PRINT '  Keys: ' + CAST(@conformant_keys AS VARCHAR);
PRINT '';
PRINT '============================================================';
PRINT 'STEP 2 COMPLETED';
PRINT '============================================================';

GO
