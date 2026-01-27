-- =============================================================================
-- Transform: Proposals - Step 4: Year-Differentiated Groups
-- 
-- These are groups where:
--   - Multiple configs when grouped by (Group, Product, Plan) across all years
--   - Single config when grouped by (Group, Year, Product, Plan)
-- 
-- For these, we create proposals keyed by (Group, Year, Product, Plan)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 4: Year-Differentiated Groups';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Find keys that are year-differentiated
-- =============================================================================
PRINT 'Step 1: Finding year-differentiated keys...';

DROP TABLE IF EXISTS [etl].[year_differentiated_keys];

WITH ConfigsWithoutYear AS (
    SELECT 
        GroupId,
        ProductCode,
        PlanCode,
        COUNT(DISTINCT ConfigJson) AS ConfigsWithoutYear
    FROM [etl].[cert_split_configs_remainder2]
    GROUP BY GroupId, ProductCode, PlanCode
    HAVING COUNT(DISTINCT ConfigJson) > 1
),
ConfigsWithYear AS (
    SELECT 
        GroupId,
        YEAR(EffectiveDate) AS EffYear,
        ProductCode,
        PlanCode,
        COUNT(DISTINCT ConfigJson) AS ConfigsWithYear,
        MAX(ConfigJson) AS ConfigJson,
        COUNT(*) AS CertCount,
        MIN(EffectiveDate) AS MinEffDate,
        MAX(EffectiveDate) AS MaxEffDate
    FROM [etl].[cert_split_configs_remainder2]
    GROUP BY GroupId, YEAR(EffectiveDate), ProductCode, PlanCode
)
SELECT 
    cwy.GroupId,
    cwy.EffYear,
    cwy.ProductCode,
    cwy.PlanCode,
    cwy.ConfigJson,
    cwy.CertCount,
    cwy.MinEffDate,
    cwy.MaxEffDate
INTO [etl].[year_differentiated_keys]
FROM ConfigsWithYear cwy
INNER JOIN ConfigsWithoutYear cwoy 
    ON cwoy.GroupId = cwy.GroupId 
    AND cwoy.ProductCode = cwy.ProductCode
    AND cwoy.PlanCode = cwy.PlanCode
WHERE cwy.ConfigsWithYear = 1;  -- Must be single config when Year included

DECLARE @yd_keys INT = @@ROWCOUNT;
PRINT 'Year-differentiated keys found: ' + CAST(@yd_keys AS VARCHAR);

DECLARE @yd_certs INT = (SELECT SUM(CertCount) FROM [etl].[year_differentiated_keys]);
PRINT 'Certificates in year-differentiated keys: ' + CAST(ISNULL(@yd_certs, 0) AS VARCHAR);

-- =============================================================================
-- Step 2: Create proposals for year-differentiated keys
-- One proposal per (Group, Year, Product, Plan) combination
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating proposals for year-differentiated keys...';

-- Get max proposal number per group from existing proposals
DROP TABLE IF EXISTS #max_proposal_num;
SELECT 
    GroupId,
    MAX(TRY_CAST(SUBSTRING(Id, CHARINDEX('-', Id, 4) + 1, 10) AS INT)) AS MaxNum
INTO #max_proposal_num
FROM [etl].[stg_proposals]
WHERE Id LIKE 'P-G%'
GROUP BY GroupId;

INSERT INTO [etl].[stg_proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState,
    BrokerUniquePartyId, BrokerName, GroupId, GroupName, Notes,
    ProductCodes, PlanCodes, SplitConfigHash, DateRangeFrom, DateRangeTo,
    EnableEffectiveDateFiltering, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, PlanCodeConstraints,
    CreationTime, IsDeleted
)
SELECT
    CONCAT('P-G', ydk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY ydk.GroupId ORDER BY ydk.EffYear, ydk.ProductCode, ydk.PlanCode)) AS Id,
    CONCAT('G', ydk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY ydk.GroupId ORDER BY ydk.EffYear, ydk.ProductCode, ydk.PlanCode)) AS ProposalNumber,
    2 AS [Status],
    ydk.MinEffDate AS SubmittedDate,
    ydk.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    g.[State] AS SitusState,
    -- NEW: Use BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [etl].[stg_brokers] b2 
            WHERE b2.ExternalPartyId = REPLACE(REPLACE(JSON_VALUE(ydk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '')
        )
        THEN REPLACE(REPLACE(JSON_VALUE(ydk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '')
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    CONCAT('G', ydk.GroupId) AS GroupId,
    g.Name AS GroupName,
    'Year-differentiated' AS Notes,
    CONCAT('["', ydk.ProductCode, '"]') AS ProductCodes,
    CASE WHEN ydk.PlanCode = '*' THEN '*' ELSE CONCAT('["', ydk.PlanCode, '"]') END AS PlanCodes,
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', ydk.ConfigJson), 2) AS SplitConfigHash,
    ydk.EffYear AS DateRangeFrom,
    ydk.EffYear AS DateRangeTo,
    1 AS EnableEffectiveDateFiltering,
    ydk.MinEffDate AS EffectiveDateFrom,
    CASE WHEN ydk.MaxEffDate <> ydk.MinEffDate THEN ydk.MaxEffDate ELSE NULL END AS EffectiveDateTo,
    CASE WHEN ydk.PlanCode = '*' THEN 0 ELSE 1 END AS EnablePlanCodeFiltering,
    CASE WHEN ydk.PlanCode = '*' THEN NULL ELSE CONCAT('["', ydk.PlanCode, '"]') END AS PlanCodeConstraints,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[year_differentiated_keys] ydk
LEFT JOIN #max_proposal_num mpn ON mpn.GroupId = CONCAT('G', ydk.GroupId)
LEFT JOIN [etl].[stg_groups] g ON g.Id = CONCAT('G', ydk.GroupId)
LEFT JOIN [etl].[stg_brokers] b ON b.ExternalPartyId = REPLACE(REPLACE(JSON_VALUE(ydk.ConfigJson, '$[0].brokerId'), 'P', ''), ' ', '');

DECLARE @proposals_created INT = @@ROWCOUNT;
PRINT 'Proposals created: ' + CAST(@proposals_created AS VARCHAR);

-- =============================================================================
-- Step 3: Add key mappings for year-differentiated keys
-- =============================================================================
PRINT '';
PRINT 'Step 3: Adding key mappings...';

INSERT INTO [etl].[stg_proposal_key_mapping] (
    GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
)
SELECT DISTINCT
    CONCAT('G', csc.GroupId) AS GroupId,
    YEAR(csc.EffectiveDate) AS EffectiveYear,
    csc.ProductCode,
    csc.PlanCode,
    p.Id AS ProposalId,
    p.SplitConfigHash
FROM [etl].[cert_split_configs_remainder2] csc
INNER JOIN [etl].[year_differentiated_keys] ydk
    ON csc.GroupId = ydk.GroupId
    AND YEAR(csc.EffectiveDate) = ydk.EffYear
    AND csc.ProductCode = ydk.ProductCode
    AND csc.PlanCode = ydk.PlanCode
INNER JOIN [etl].[stg_proposals] p 
    ON p.GroupId = CONCAT('G', csc.GroupId)
    AND p.ProductCodes = CONCAT('["', csc.ProductCode, '"]')
    AND (p.PlanCodes = CONCAT('["', csc.PlanCode, '"]') OR p.PlanCodes = '*')
    AND p.DateRangeFrom = YEAR(csc.EffectiveDate)
    AND p.Notes = 'Year-differentiated'
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[stg_proposal_key_mapping] pkm
    WHERE pkm.GroupId = CONCAT('G', csc.GroupId)
      AND pkm.EffectiveYear = YEAR(csc.EffectiveDate)
      AND pkm.ProductCode = csc.ProductCode
      AND pkm.PlanCode = csc.PlanCode
);

DECLARE @mappings_created INT = @@ROWCOUNT;
PRINT 'Key mappings created: ' + CAST(@mappings_created AS VARCHAR);

-- =============================================================================
-- Step 4: Create PremiumSplitVersions for year-differentiated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating PremiumSplitVersions for year-differentiated proposals...';

INSERT INTO [etl].[stg_premium_split_versions] (
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
        FROM OPENJSON(ydk.ConfigJson)
        WITH ([level] INT '$.level', [percent] DECIMAL(5,2) '$.percent') j
        WHERE j.[level] = 1
    ) AS TotalSplitPercent,
    1 AS [Status],
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_proposals] p
INNER JOIN [etl].[year_differentiated_keys] ydk
    ON p.GroupId = CONCAT('G', ydk.GroupId)
    AND p.ProductCodes = CONCAT('["', ydk.ProductCode, '"]')
    AND (p.PlanCodes = CONCAT('["', ydk.PlanCode, '"]') OR (p.PlanCodes = '*' AND ydk.PlanCode = '*'))
    AND p.DateRangeFrom = ydk.EffYear
    AND p.Notes = 'Year-differentiated'
LEFT JOIN [etl].[stg_groups] g ON g.Id = p.GroupId;

DECLARE @yd_split_versions INT = @@ROWCOUNT;
PRINT 'Split versions created: ' + CAST(@yd_split_versions AS VARCHAR);

-- =============================================================================
-- Step 5: Create PremiumSplitParticipants for year-differentiated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating PremiumSplitParticipants for year-differentiated proposals...';

-- Note: HierarchyId will be set later in 07-hierarchies.sql via stg_splitseq_hierarchy_map
INSERT INTO [etl].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, HierarchyName, Sequence, WritingBrokerId, GroupId,
    EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    (SELECT COALESCE(MAX(TRY_CAST(Id AS INT)), 0) + 1 FROM [etl].[stg_premium_split_participants]) + 
        ROW_NUMBER() OVER (ORDER BY p.Id, j.splitSeq) - 1 AS Id,
    CONCAT('PSV-', p.Id) AS VersionId,
    -- BrokerId (required, deprecated but still needed)
    COALESCE(b.Id, TRY_CAST(REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '') AS BIGINT), 0) AS BrokerId,
    -- NEW: Use BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [etl].[stg_brokers] b2 
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
FROM [etl].[stg_proposals] p
INNER JOIN [etl].[year_differentiated_keys] ydk
    ON p.GroupId = CONCAT('G', ydk.GroupId)
    AND p.ProductCodes = CONCAT('["', ydk.ProductCode, '"]')
    AND (p.PlanCodes = CONCAT('["', ydk.PlanCode, '"]') OR (p.PlanCodes = '*' AND ydk.PlanCode = '*'))
    AND p.DateRangeFrom = ydk.EffYear
    AND p.Notes = 'Year-differentiated'
CROSS APPLY OPENJSON(ydk.ConfigJson)
    WITH (
        splitSeq INT '$.splitSeq',
        [level] INT '$.level',
        brokerId NVARCHAR(50) '$.brokerId',
        [percent] DECIMAL(5,2) '$.percent',
        schedule NVARCHAR(100) '$.schedule'
    ) j
LEFT JOIN [etl].[stg_brokers] b 
    ON b.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
WHERE j.[level] = 1;

DECLARE @yd_split_participants INT = @@ROWCOUNT;
PRINT 'Split participants created: ' + CAST(@yd_split_participants AS VARCHAR);

-- =============================================================================
-- Step 6: Create remainder table excluding year-differentiated
-- =============================================================================
PRINT '';
PRINT 'Step 6: Creating remainder table...';

DROP TABLE IF EXISTS [etl].[cert_split_configs_remainder3];

SELECT csc.*
INTO [etl].[cert_split_configs_remainder3]
FROM [etl].[cert_split_configs_remainder2] csc
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[year_differentiated_keys] ydk
    WHERE csc.GroupId = ydk.GroupId
      AND YEAR(csc.EffectiveDate) = ydk.EffYear
      AND csc.ProductCode = ydk.ProductCode
      AND csc.PlanCode = ydk.PlanCode
);

DECLARE @remainder_certs INT = @@ROWCOUNT;
PRINT 'Certificates remaining: ' + CAST(@remainder_certs AS VARCHAR);

DECLARE @remainder_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [etl].[cert_split_configs_remainder3]);
PRINT 'Groups remaining: ' + CAST(@remainder_groups AS VARCHAR);

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';
PRINT '';
PRINT 'YEAR-DIFFERENTIATED:';
PRINT '  Keys: ' + CAST(@yd_keys AS VARCHAR);
PRINT '  Certificates: ' + CAST(ISNULL(@yd_certs, 0) AS VARCHAR);
PRINT '  Proposals created: ' + CAST(@proposals_created AS VARCHAR);
PRINT '  Key mappings: ' + CAST(@mappings_created AS VARCHAR);
PRINT '  Split versions: ' + CAST(@yd_split_versions AS VARCHAR);
PRINT '  Split participants: ' + CAST(@yd_split_participants AS VARCHAR);
PRINT '';
PRINT 'REMAINDER:';
PRINT '  Certificates: ' + CAST(@remainder_certs AS VARCHAR);
PRINT '  Groups: ' + CAST(@remainder_groups AS VARCHAR);
PRINT '';
DECLARE @total_proposals INT = (SELECT COUNT(*) FROM [etl].[stg_proposals]);
DECLARE @total_mappings INT = (SELECT COUNT(*) FROM [etl].[stg_proposal_key_mapping]);
PRINT 'TOTAL PROPOSALS SO FAR: ' + CAST(@total_proposals AS VARCHAR);
PRINT 'TOTAL KEY MAPPINGS: ' + CAST(@total_mappings AS VARCHAR);
PRINT '';
PRINT '============================================================';
PRINT 'STEP 4 COMPLETED';
PRINT '============================================================';

GO
