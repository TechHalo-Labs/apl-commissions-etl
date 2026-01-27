-- =============================================================================
-- Transform: Proposals - Step 3: Plan-Differentiated Groups
-- 
-- These are groups where:
--   - Multiple configs when grouped by (Group, Year, Product)
--   - Single config when grouped by (Group, Year, Product, Plan)
-- 
-- For these, we create proposals keyed by (Group, Year, Product, Plan)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 3: Plan-Differentiated Groups';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Find keys that are plan-differentiated
-- =============================================================================
PRINT 'Step 1: Finding plan-differentiated keys...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[plan_differentiated_keys];

WITH ConfigsWithoutPlan AS (
    SELECT 
        GroupId,
        YEAR(EffectiveDate) AS EffYear,
        ProductCode,
        COUNT(DISTINCT ConfigJson) AS ConfigsWithoutPlan
    FROM [$(ETL_SCHEMA)].[cert_split_configs_conformant]
    GROUP BY GroupId, YEAR(EffectiveDate), ProductCode
    HAVING COUNT(DISTINCT ConfigJson) > 1
),
ConfigsWithPlan AS (
    SELECT 
        GroupId,
        YEAR(EffectiveDate) AS EffYear,
        ProductCode,
        PlanCode,
        COUNT(DISTINCT ConfigJson) AS ConfigsWithPlan,
        MAX(ConfigJson) AS ConfigJson,
        COUNT(*) AS CertCount,
        MIN(EffectiveDate) AS MinEffDate,
        MAX(EffectiveDate) AS MaxEffDate
    FROM [$(ETL_SCHEMA)].[cert_split_configs_conformant]
    GROUP BY GroupId, YEAR(EffectiveDate), ProductCode, PlanCode
)
SELECT 
    cwp.GroupId,
    cwp.EffYear,
    cwp.ProductCode,
    cwp.PlanCode,
    cwp.ConfigJson,
    cwp.CertCount,
    cwp.MinEffDate,
    cwp.MaxEffDate
INTO [$(ETL_SCHEMA)].[plan_differentiated_keys]
FROM ConfigsWithPlan cwp
INNER JOIN ConfigsWithoutPlan cwop 
    ON cwop.GroupId = cwp.GroupId 
    AND cwop.EffYear = cwp.EffYear 
    AND cwop.ProductCode = cwp.ProductCode
WHERE cwp.ConfigsWithPlan = 1;  -- Must be single config when Plan included

DECLARE @pd_keys INT = @@ROWCOUNT;
PRINT 'Plan-differentiated keys found: ' + CAST(@pd_keys AS VARCHAR);

DECLARE @pd_certs INT = (SELECT SUM(CertCount) FROM [$(ETL_SCHEMA)].[plan_differentiated_keys]);
PRINT 'Certificates in plan-differentiated keys: ' + CAST(ISNULL(@pd_certs, 0) AS VARCHAR);

-- =============================================================================
-- Step 2: Create proposals for plan-differentiated keys
-- One proposal per (Group, Year, Product, Plan) combination
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating proposals for plan-differentiated keys...';

-- Get max proposal number per group from existing proposals
DROP TABLE IF EXISTS #max_proposal_num;
SELECT 
    GroupId,
    MAX(TRY_CAST(RIGHT(Id, LEN(Id) - CHARINDEX('-', Id, 4)) AS INT)) AS MaxNum
INTO #max_proposal_num
FROM [$(ETL_SCHEMA)].[stg_proposals]
GROUP BY GroupId;

INSERT INTO [$(ETL_SCHEMA)].[stg_proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState,
    BrokerUniquePartyId, BrokerName, GroupId, GroupName, Notes,
    ProductCodes, PlanCodes, SplitConfigHash, DateRangeFrom, DateRangeTo,
    EnableEffectiveDateFiltering, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, PlanCodeConstraints,
    CreationTime, IsDeleted
)
SELECT
    CONCAT('P-G', pdk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY pdk.GroupId ORDER BY pdk.EffYear, pdk.ProductCode, pdk.PlanCode)) AS Id,
    CONCAT('G', pdk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY pdk.GroupId ORDER BY pdk.EffYear, pdk.ProductCode, pdk.PlanCode)) AS ProposalNumber,
    2 AS [Status],
    pdk.MinEffDate AS SubmittedDate,
    pdk.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    g.[State] AS SitusState,
    -- NEW: Use BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b2 
            WHERE b2.ExternalPartyId = REPLACE(REPLACE(JSON_VALUE(pdk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '')
        )
        THEN REPLACE(REPLACE(JSON_VALUE(pdk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '')
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    CONCAT('G', pdk.GroupId) AS GroupId,
    g.Name AS GroupName,
    'Plan-differentiated' AS Notes,
    CONCAT('["', pdk.ProductCode, '"]') AS ProductCodes,
    CONCAT('["', pdk.PlanCode, '"]') AS PlanCodes,
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', pdk.ConfigJson), 2) AS SplitConfigHash,
    pdk.EffYear AS DateRangeFrom,
    pdk.EffYear AS DateRangeTo,
    1 AS EnableEffectiveDateFiltering,
    pdk.MinEffDate AS EffectiveDateFrom,
    CASE WHEN pdk.MaxEffDate <> pdk.MinEffDate THEN pdk.MaxEffDate ELSE NULL END AS EffectiveDateTo,
    1 AS EnablePlanCodeFiltering,
    CONCAT('["', pdk.PlanCode, '"]') AS PlanCodeConstraints,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[plan_differentiated_keys] pdk
LEFT JOIN #max_proposal_num mpn ON mpn.GroupId = CONCAT('G', pdk.GroupId)
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = CONCAT('G', pdk.GroupId)
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = REPLACE(REPLACE(JSON_VALUE(pdk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '');

DECLARE @proposals_created INT = @@ROWCOUNT;
PRINT 'Proposals created: ' + CAST(@proposals_created AS VARCHAR);

-- =============================================================================
-- Step 3: Add key mappings for plan-differentiated keys
-- =============================================================================
PRINT '';
PRINT 'Step 3: Adding key mappings...';

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_key_mapping] (
    GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
)
SELECT DISTINCT
    CONCAT('G', csc.GroupId) AS GroupId,
    YEAR(csc.EffectiveDate) AS EffectiveYear,
    csc.ProductCode,
    csc.PlanCode,
    p.Id AS ProposalId,
    p.SplitConfigHash
FROM [$(ETL_SCHEMA)].[cert_split_configs_conformant] csc
INNER JOIN [$(ETL_SCHEMA)].[plan_differentiated_keys] pdk
    ON csc.GroupId = pdk.GroupId
    AND YEAR(csc.EffectiveDate) = pdk.EffYear
    AND csc.ProductCode = pdk.ProductCode
    AND csc.PlanCode = pdk.PlanCode
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p 
    ON p.GroupId = CONCAT('G', csc.GroupId)
    AND p.ProductCodes = CONCAT('["', csc.ProductCode, '"]')
    AND p.PlanCodes = CONCAT('["', csc.PlanCode, '"]')
    AND p.DateRangeFrom = YEAR(csc.EffectiveDate)
    AND p.Notes = 'Plan-differentiated';

DECLARE @mappings_created INT = @@ROWCOUNT;
PRINT 'Key mappings created: ' + CAST(@mappings_created AS VARCHAR);

-- =============================================================================
-- Step 4: Create PremiumSplitVersions for plan-differentiated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating PremiumSplitVersions for plan-differentiated proposals...';

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_versions] (
    Id, GroupId, GroupName, ProposalId, ProposalNumber,
    VersionNumber, EffectiveFrom, EffectiveTo,
    TotalSplitPercent, [Status], [Source], CreationTime, IsDeleted
)
SELECT
    CONCAT('PSV-', p.Id) AS Id,
    p.GroupId,
    g.Name AS GroupName,
    p.Id AS ProposalId,
    p.ProposalNumber,
    '1.0' AS VersionNumber,
    p.EffectiveDateFrom AS EffectiveFrom,
    p.EffectiveDateTo AS EffectiveTo,
    (
        SELECT SUM(TRY_CAST(j.[percent] AS DECIMAL(5,2)))
        FROM OPENJSON(pdk.ConfigJson)
        WITH ([level] INT '$.level', [percent] DECIMAL(5,2) '$.percent') j
        WHERE j.[level] = 1
    ) AS TotalSplitPercent,
    1 AS [Status],
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[plan_differentiated_keys] pdk
    ON p.GroupId = CONCAT('G', pdk.GroupId)
    AND p.ProductCodes = CONCAT('["', pdk.ProductCode, '"]')
    AND p.PlanCodes = CONCAT('["', pdk.PlanCode, '"]')
    AND p.DateRangeFrom = pdk.EffYear
    AND p.Notes = 'Plan-differentiated'
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = p.GroupId;

DECLARE @pd_split_versions INT = @@ROWCOUNT;
PRINT 'Split versions created: ' + CAST(@pd_split_versions AS VARCHAR);

-- =============================================================================
-- Step 5: Create PremiumSplitParticipants for plan-differentiated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating PremiumSplitParticipants for plan-differentiated proposals...';

-- Note: HierarchyId will be set later in 07-hierarchies.sql via stg_splitseq_hierarchy_map
INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, HierarchyName, Sequence, WritingBrokerId, GroupId,
    EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    (SELECT COALESCE(MAX(TRY_CAST(Id AS INT)), 0) + 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_participants]) + 
        ROW_NUMBER() OVER (ORDER BY p.Id, j.splitSeq) - 1 AS Id,
    CONCAT('PSV-', p.Id) AS VersionId,
    -- BrokerId (required, deprecated but still needed)
    COALESCE(b.Id, TRY_CAST(REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '') AS BIGINT), 0) AS BrokerId,
    -- NEW: Use BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b2 
            WHERE b2.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
        )
        THEN REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    TRY_CAST(j.[percent] AS DECIMAL(5,2)) AS SplitPercent,
    1 AS IsWritingAgent,
    NULL AS HierarchyId,  -- Will be linked in 07-hierarchies.sql
    NULL AS HierarchyName,
    j.splitSeq AS Sequence,
    TRY_CAST(REPLACE(j.brokerId, 'P', '') AS BIGINT) AS WritingBrokerId,  -- Keep for hierarchy lookup
    p.GroupId AS GroupId,  -- Store for later hierarchy linking
    p.EffectiveDateFrom AS EffectiveFrom,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[plan_differentiated_keys] pdk
    ON p.GroupId = CONCAT('G', pdk.GroupId)
    AND p.ProductCodes = CONCAT('["', pdk.ProductCode, '"]')
    AND p.PlanCodes = CONCAT('["', pdk.PlanCode, '"]')
    AND p.DateRangeFrom = pdk.EffYear
    AND p.Notes = 'Plan-differentiated'
CROSS APPLY OPENJSON(pdk.ConfigJson)
    WITH (
        splitSeq INT '$.splitSeq',
        [level] INT '$.level',
        brokerId NVARCHAR(50) '$.brokerId',
        [percent] DECIMAL(5,2) '$.percent',
        schedule NVARCHAR(100) '$.schedule'
    ) j
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b 
    ON b.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
WHERE j.[level] = 1;

DECLARE @pd_split_participants INT = @@ROWCOUNT;
PRINT 'Split participants created: ' + CAST(@pd_split_participants AS VARCHAR);

-- =============================================================================
-- Step 6: Create remainder table excluding plan-differentiated
-- =============================================================================
PRINT '';
PRINT 'Step 6: Creating remainder table...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[cert_split_configs_remainder2];

SELECT csc.*
INTO [$(ETL_SCHEMA)].[cert_split_configs_remainder2]
FROM [$(ETL_SCHEMA)].[cert_split_configs_conformant] csc
WHERE NOT EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[plan_differentiated_keys] pdk
    WHERE csc.GroupId = pdk.GroupId
      AND YEAR(csc.EffectiveDate) = pdk.EffYear
      AND csc.ProductCode = pdk.ProductCode
      AND csc.PlanCode = pdk.PlanCode
);

DECLARE @remainder_certs INT = @@ROWCOUNT;
PRINT 'Certificates remaining: ' + CAST(@remainder_certs AS VARCHAR);

DECLARE @remainder_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [$(ETL_SCHEMA)].[cert_split_configs_remainder2]);
PRINT 'Groups remaining: ' + CAST(@remainder_groups AS VARCHAR);

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';
PRINT '';
PRINT 'PLAN-DIFFERENTIATED:';
PRINT '  Keys: ' + CAST(@pd_keys AS VARCHAR);
PRINT '  Certificates: ' + CAST(ISNULL(@pd_certs, 0) AS VARCHAR);
PRINT '  Proposals created: ' + CAST(@proposals_created AS VARCHAR);
PRINT '  Key mappings: ' + CAST(@mappings_created AS VARCHAR);
PRINT '  Split versions: ' + CAST(@pd_split_versions AS VARCHAR);
PRINT '  Split participants: ' + CAST(@pd_split_participants AS VARCHAR);
PRINT '';
PRINT 'REMAINDER:';
PRINT '  Certificates: ' + CAST(@remainder_certs AS VARCHAR);
PRINT '  Groups: ' + CAST(@remainder_groups AS VARCHAR);
PRINT '';
DECLARE @total_proposals INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]);
PRINT 'TOTAL PROPOSALS SO FAR: ' + CAST(@total_proposals AS VARCHAR);
PRINT '';
PRINT '============================================================';
PRINT 'STEP 3 COMPLETED';
PRINT '============================================================';

GO
