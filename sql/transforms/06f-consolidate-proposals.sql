-- =============================================================================
-- Transform: Proposals - Step 6: Consolidate Proposals
-- 
-- Walk through granular proposals ordered by (Group, Config, EffYear, Product, Plan)
-- and merge adjacent proposals with the same (Group, Config) into broader proposals.
-- 
-- Consolidation rules:
--   - Same GroupId + Same SplitConfigHash => can be merged
--   - Merge extends DateRangeFrom/To to cover all years
--   - Merge accumulates ProductCodes and PlanCodes
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 6: Consolidate Proposals';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build sorted proposals table for consolidation
-- =============================================================================
PRINT 'Step 1: Building sorted proposals for consolidation...';

DROP TABLE IF EXISTS #proposals_sorted;

SELECT 
    Id,
    GroupId,
    SplitConfigHash,
    DateRangeFrom,
    DateRangeTo,
    ProductCodes,
    PlanCodes,
    EffectiveDateFrom,
    EffectiveDateTo,
    BrokerUniquePartyId,  -- NEW: Use BrokerUniquePartyId instead of BrokerId
    BrokerName,
    SitusState,
    Notes,
    ROW_NUMBER() OVER (
        ORDER BY GroupId, SplitConfigHash, DateRangeFrom, ProductCodes, PlanCodes
    ) AS SortOrder
INTO #proposals_sorted
FROM [$(ETL_SCHEMA)].[stg_proposals]
WHERE Notes = 'Granular';

DECLARE @granular_count INT = @@ROWCOUNT;
PRINT 'Granular proposals to consolidate: ' + CAST(@granular_count AS VARCHAR);

-- =============================================================================
-- Step 2: Identify consolidation groups
-- Each (GroupId, SplitConfigHash) combination becomes one consolidated proposal
-- =============================================================================
PRINT '';
PRINT 'Step 2: Identifying consolidation groups...';

DROP TABLE IF EXISTS #consolidation_groups;

SELECT 
    GroupId,
    SplitConfigHash,
    MIN(DateRangeFrom) AS DateRangeFrom,
    MAX(DateRangeTo) AS DateRangeTo,
    MIN(EffectiveDateFrom) AS EffectiveDateFrom,
    MAX(EffectiveDateTo) AS EffectiveDateTo,
    COUNT(*) AS OriginalProposalCount,
    MIN(BrokerUniquePartyId) AS BrokerUniquePartyId,  -- NEW: Use BrokerUniquePartyId instead of BrokerId
    MIN(BrokerName) AS BrokerName,
    MIN(SitusState) AS SitusState
INTO #consolidation_groups
FROM #proposals_sorted
GROUP BY GroupId, SplitConfigHash;

DECLARE @group_count INT = @@ROWCOUNT;
PRINT 'Consolidation groups (unique Group+Config): ' + CAST(@group_count AS VARCHAR);
PRINT 'Reduction: ' + CAST(@granular_count AS VARCHAR) + ' -> ' + CAST(@group_count AS VARCHAR);

-- =============================================================================
-- Step 3: Build aggregated ProductCodes and PlanCodes per group
-- =============================================================================
PRINT '';
PRINT 'Step 3: Aggregating product and plan codes...';

DROP TABLE IF EXISTS #group_products;

-- Get distinct products per group
SELECT 
    ps.GroupId,
    ps.SplitConfigHash,
    STRING_AGG(CAST(REPLACE(REPLACE(ps.ProductCodes, '["', ''), '"]', '') AS NVARCHAR(MAX)), '","') 
        WITHIN GROUP (ORDER BY REPLACE(REPLACE(ps.ProductCodes, '["', ''), '"]', '')) AS ProductList
INTO #group_products
FROM (
    SELECT DISTINCT GroupId, SplitConfigHash, ProductCodes
    FROM #proposals_sorted
) ps
GROUP BY ps.GroupId, ps.SplitConfigHash;

DROP TABLE IF EXISTS #group_plans;

-- Get distinct plans per group
SELECT 
    ps.GroupId,
    ps.SplitConfigHash,
    STRING_AGG(CAST(REPLACE(REPLACE(ps.PlanCodes, '["', ''), '"]', '') AS NVARCHAR(MAX)), '","')
        WITHIN GROUP (ORDER BY REPLACE(REPLACE(ps.PlanCodes, '["', ''), '"]', '')) AS PlanList,
    COUNT(DISTINCT ps.PlanCodes) AS DistinctPlanCount
INTO #group_plans
FROM (
    SELECT DISTINCT GroupId, SplitConfigHash, PlanCodes
    FROM #proposals_sorted
) ps
GROUP BY ps.GroupId, ps.SplitConfigHash;

-- =============================================================================
-- Step 4: Create consolidated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating consolidated proposals...';

-- First, delete the granular proposals (we'll replace them with consolidated ones)
DELETE FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE Notes = 'Granular';

DECLARE @deleted INT = @@ROWCOUNT;
PRINT 'Deleted granular proposals: ' + CAST(@deleted AS VARCHAR);

-- Delete key mappings for granular proposals (they reference deleted proposals)
DELETE pkm
FROM [$(ETL_SCHEMA)].[stg_proposal_key_mapping] pkm
WHERE NOT EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[stg_proposals] p WHERE p.Id = pkm.ProposalId
);

DECLARE @mappings_deleted INT = @@ROWCOUNT;
PRINT 'Deleted orphaned key mappings: ' + CAST(@mappings_deleted AS VARCHAR);

-- Get max proposal number per group from existing proposals
DROP TABLE IF EXISTS #max_proposal_num;
SELECT 
    GroupId,
    MAX(TRY_CAST(SUBSTRING(Id, CHARINDEX('-', Id, 4) + 1, 10) AS INT)) AS MaxNum
INTO #max_proposal_num
FROM [$(ETL_SCHEMA)].[stg_proposals]
WHERE Id LIKE 'P-G%'
GROUP BY GroupId;

-- Insert consolidated proposals
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
    CONCAT('P-', cg.GroupId, '-C', ROW_NUMBER() OVER (PARTITION BY cg.GroupId ORDER BY cg.DateRangeFrom)) AS Id,
    CONCAT(cg.GroupId, '-C', ROW_NUMBER() OVER (PARTITION BY cg.GroupId ORDER BY cg.DateRangeFrom)) AS ProposalNumber,
    2 AS [Status],
    cg.EffectiveDateFrom AS SubmittedDate,
    cg.EffectiveDateFrom AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    cg.SitusState,
    cg.BrokerUniquePartyId,  -- NEW: Use BrokerUniquePartyId instead of BrokerId
    cg.BrokerName,
    cg.GroupId,
    g.Name AS GroupName,
    'Consolidated' AS Notes,
    CONCAT('["', gp.ProductList, '"]') AS ProductCodes,
    CASE 
        WHEN gpl.DistinctPlanCount > 10 THEN '*'  -- Too many plans, use wildcard
        ELSE CONCAT('["', gpl.PlanList, '"]') 
    END AS PlanCodes,
    cg.SplitConfigHash,
    cg.DateRangeFrom,
    cg.DateRangeTo,
    1 AS EnableEffectiveDateFiltering,
    cg.EffectiveDateFrom,
    CASE WHEN cg.EffectiveDateTo = cg.EffectiveDateFrom THEN NULL ELSE cg.EffectiveDateTo END AS EffectiveDateTo,
    CASE WHEN gpl.DistinctPlanCount > 10 THEN 0 ELSE 1 END AS EnablePlanCodeFiltering,
    CASE 
        WHEN gpl.DistinctPlanCount > 10 THEN NULL 
        ELSE CONCAT('["', gpl.PlanList, '"]') 
    END AS PlanCodeConstraints,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #consolidation_groups cg
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = cg.GroupId
LEFT JOIN #group_products gp ON gp.GroupId = cg.GroupId AND gp.SplitConfigHash = cg.SplitConfigHash
LEFT JOIN #group_plans gpl ON gpl.GroupId = cg.GroupId AND gpl.SplitConfigHash = cg.SplitConfigHash;

DECLARE @consolidated_created INT = @@ROWCOUNT;
PRINT 'Consolidated proposals created: ' + CAST(@consolidated_created AS VARCHAR);

-- =============================================================================
-- Step 5: Recreate key mappings for consolidated proposals
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating key mappings for consolidated proposals...';

-- Map keys to consolidated proposals based on Group and Config
-- Use ROW_NUMBER to pick just one proposal per key when multiple match
INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_key_mapping] (
    GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
)
SELECT GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
FROM (
    SELECT 
        CONCAT('G', csc.GroupId) AS GroupId,
        YEAR(csc.EffectiveDate) AS EffectiveYear,
        csc.ProductCode,
        csc.PlanCode,
        p.Id AS ProposalId,
        p.SplitConfigHash,
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT('G', csc.GroupId), YEAR(csc.EffectiveDate), csc.ProductCode, csc.PlanCode
            ORDER BY p.Id
        ) AS rn
    FROM [$(ETL_SCHEMA)].[cert_split_configs_remainder3] csc
    INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p 
        ON p.GroupId = CONCAT('G', csc.GroupId)
        AND p.SplitConfigHash = CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', csc.ConfigJson), 2)
        AND p.Notes = 'Consolidated'
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_proposal_key_mapping] pkm
        WHERE pkm.GroupId = CONCAT('G', csc.GroupId)
          AND pkm.EffectiveYear = YEAR(csc.EffectiveDate)
          AND pkm.ProductCode = csc.ProductCode
          AND pkm.PlanCode = csc.PlanCode
    )
) ranked
WHERE rn = 1;

DECLARE @new_mappings INT = @@ROWCOUNT;
PRINT 'New key mappings created: ' + CAST(@new_mappings AS VARCHAR);

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'CONSOLIDATION SUMMARY';
PRINT '============================================================';
PRINT '';
PRINT 'BEFORE: ' + CAST(@granular_count AS VARCHAR) + ' granular proposals';
PRINT 'AFTER: ' + CAST(@consolidated_created AS VARCHAR) + ' consolidated proposals';
IF @granular_count > 0
  PRINT 'REDUCTION: ' + CAST(@granular_count - @consolidated_created AS VARCHAR) + ' proposals (' + 
        CAST(CAST(100.0 * (@granular_count - @consolidated_created) / @granular_count AS DECIMAL(5,1)) AS VARCHAR) + '%)';
ELSE
  PRINT 'REDUCTION: N/A (no granular proposals to consolidate)';
PRINT '';

DECLARE @total_proposals INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]);
DECLARE @total_mappings INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposal_key_mapping]);
PRINT 'FINAL TOTALS:';
PRINT '  Total proposals: ' + CAST(@total_proposals AS VARCHAR);
PRINT '  Total key mappings: ' + CAST(@total_mappings AS VARCHAR);
PRINT '';

-- Breakdown by type
PRINT 'PROPOSALS BY TYPE:';
SELECT Notes, COUNT(*) as cnt
FROM [$(ETL_SCHEMA)].[stg_proposals]
GROUP BY Notes
ORDER BY cnt DESC;

GO

-- =============================================================================
-- Step 6: Populate stg_proposal_products
-- Links each proposal to its product codes based on certificates in the group
-- =============================================================================
PRINT '';
PRINT 'Step 6: Populating stg_proposal_products...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposal_products];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_products] (Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.Id, pp.ProductCode) AS Id,
    p.Id AS ProposalId,
    pp.ProductCode,
    CONCAT(COALESCE(pp.ProductCategory, 'Unknown'), ' - ', pp.ProductCode) AS ProductName,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        LTRIM(RTRIM(ci.Product)) AS ProductCode,
        MAX(LTRIM(RTRIM(ci.ProductCategory))) AS ProductCategory
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.SplitBrokerSeq = 1
      AND LTRIM(RTRIM(ci.Product)) <> ''
      AND LTRIM(RTRIM(ci.GroupId)) <> ''
      AND ci.RecStatus = 'A'  -- Only active split configurations
    GROUP BY LTRIM(RTRIM(ci.GroupId)), LTRIM(RTRIM(ci.Product))
) pp
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.GroupId = pp.GroupId;

DECLARE @pp_count INT = @@ROWCOUNT;
PRINT 'Proposal products staged: ' + CAST(@pp_count AS VARCHAR);

GO

-- =============================================================================
-- Step 7: Create PremiumSplitVersions for consolidated proposals
-- One split version per consolidated proposal
-- =============================================================================
PRINT '';
PRINT 'Step 7: Creating PremiumSplitVersions for consolidated proposals...';

-- Get the ConfigJson for each consolidated proposal by matching back to the original certs
DROP TABLE IF EXISTS #consolidated_configs;

SELECT DISTINCT
    p.Id AS ProposalId,
    p.GroupId,
    p.SplitConfigHash,
    p.EffectiveDateFrom,
    p.EffectiveDateTo,
    csc.ConfigJson
INTO #consolidated_configs
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[cert_split_configs_remainder3] csc 
    ON CONCAT('G', csc.GroupId) = p.GroupId
    AND CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', csc.ConfigJson), 2) = p.SplitConfigHash
WHERE p.Notes = 'Consolidated';

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_versions] (
    Id, GroupId, GroupName, ProposalId, ProposalNumber,
    VersionNumber, EffectiveFrom, EffectiveTo,
    TotalSplitPercent, [Status], [Source], CreationTime, IsDeleted
)
SELECT
    CONCAT('PSV-', cc.ProposalId) AS Id,
    cc.GroupId,
    g.Name AS GroupName,
    cc.ProposalId,
    p.ProposalNumber,
    '1.0' AS VersionNumber,
    cc.EffectiveDateFrom AS EffectiveFrom,
    cc.EffectiveDateTo AS EffectiveTo,
    (
        SELECT SUM(TRY_CAST(j.[percent] AS DECIMAL(5,2)))
        FROM OPENJSON(cc.ConfigJson)
        WITH ([level] INT '$.level', [percent] DECIMAL(5,2) '$.percent') j
        WHERE j.[level] = 1
    ) AS TotalSplitPercent,
    1 AS [Status],
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #consolidated_configs cc
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.Id = cc.ProposalId
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = cc.GroupId
WHERE NOT EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv 
    WHERE psv.ProposalId = cc.ProposalId
);

DECLARE @split_versions_created INT = @@ROWCOUNT;
PRINT 'Split versions created for consolidated proposals: ' + CAST(@split_versions_created AS VARCHAR);

-- =============================================================================
-- Step 8: Create PremiumSplitParticipants for consolidated proposals
-- Extract level=1 participants from ConfigJson
-- =============================================================================
PRINT '';
PRINT 'Step 8: Creating PremiumSplitParticipants for consolidated proposals...';

-- Get max ID from existing participants
DECLARE @max_psp_id INT = (SELECT COALESCE(MAX(TRY_CAST(Id AS INT)), 0) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants]);

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, HierarchyName, Sequence, WritingBrokerId, GroupId,
    EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    @max_psp_id + ROW_NUMBER() OVER (ORDER BY cc.ProposalId, j.splitSeq) AS Id,
    CONCAT('PSV-', cc.ProposalId) AS VersionId,
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
    NULL AS HierarchyId,  -- Will be linked in 08-hierarchy-splits.sql
    NULL AS HierarchyName,
    j.splitSeq AS Sequence,
    TRY_CAST(REPLACE(j.brokerId, 'P', '') AS BIGINT) AS WritingBrokerId,
    cc.GroupId,
    cc.EffectiveDateFrom AS EffectiveFrom,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #consolidated_configs cc
CROSS APPLY OPENJSON(cc.ConfigJson)
    WITH (
        splitSeq INT '$.splitSeq',
        [level] INT '$.level',
        brokerId NVARCHAR(50) '$.brokerId',
        [percent] DECIMAL(5,2) '$.percent'
    ) j
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
WHERE j.[level] = 1
  AND j.brokerId IS NOT NULL
  AND TRY_CAST(REPLACE(j.brokerId, 'P', '') AS BIGINT) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
      WHERE psp.VersionId = CONCAT('PSV-', cc.ProposalId)
        AND psp.Sequence = j.splitSeq
  );

DECLARE @split_participants_created INT = @@ROWCOUNT;
PRINT 'Split participants created for consolidated proposals: ' + CAST(@split_participants_created AS VARCHAR);

DROP TABLE IF EXISTS #consolidated_configs;

PRINT '';
PRINT '============================================================';
PRINT 'CONSOLIDATION COMPLETED';
PRINT '============================================================';

-- Final split counts
DECLARE @total_split_versions INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]);
DECLARE @total_split_participants INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants]);
PRINT '';
PRINT 'FINAL SPLIT TOTALS:';
PRINT '  Total split versions: ' + CAST(@total_split_versions AS VARCHAR);
PRINT '  Total split participants: ' + CAST(@total_split_participants AS VARCHAR);

GO
