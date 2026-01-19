-- =============================================================================
-- Transform: Proposals - Step 5: Granular Proposals
-- 
-- Create one proposal per unique (Group, Year, Product, Plan) key
-- from the remaining certificates after plan/year differentiated processing.
-- 
-- These will be consolidated in the next step (06f).
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 5: Granular Proposals';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Count remaining keys
-- =============================================================================
PRINT 'Step 1: Analyzing remaining certificates...';

DECLARE @input_certs INT = (SELECT COUNT(*) FROM [etl].[cert_split_configs_remainder3]);
DECLARE @input_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [etl].[cert_split_configs_remainder3]);
PRINT 'Input certificates: ' + CAST(@input_certs AS VARCHAR);
PRINT 'Input groups: ' + CAST(@input_groups AS VARCHAR);

-- =============================================================================
-- Step 2: Build distinct keys table
-- =============================================================================
PRINT '';
PRINT 'Step 2: Building distinct keys...';

DROP TABLE IF EXISTS [etl].[granular_keys];

SELECT 
    GroupId,
    YEAR(EffectiveDate) AS EffYear,
    ProductCode,
    PlanCode,
    MAX(ConfigJson) AS ConfigJson,
    COUNT(*) AS CertCount,
    MIN(EffectiveDate) AS MinEffDate,
    MAX(EffectiveDate) AS MaxEffDate
INTO [etl].[granular_keys]
FROM [etl].[cert_split_configs_remainder3]
GROUP BY GroupId, YEAR(EffectiveDate), ProductCode, PlanCode;

DECLARE @keys_count INT = @@ROWCOUNT;
PRINT 'Distinct keys: ' + CAST(@keys_count AS VARCHAR);

-- =============================================================================
-- Step 3: Create proposals for each key
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating granular proposals...';

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
    BrokerId, BrokerName, GroupId, GroupName, Notes,
    ProductCodes, PlanCodes, SplitConfigHash, DateRangeFrom, DateRangeTo,
    EnableEffectiveDateFiltering, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, PlanCodeConstraints,
    CreationTime, IsDeleted
)
SELECT
    CONCAT('P-G', gk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY gk.GroupId ORDER BY gk.EffYear, gk.ProductCode, gk.PlanCode)) AS Id,
    CONCAT('G', gk.GroupId, '-', COALESCE(mpn.MaxNum, 0) + ROW_NUMBER() OVER (PARTITION BY gk.GroupId ORDER BY gk.EffYear, gk.ProductCode, gk.PlanCode)) AS ProposalNumber,
    2 AS [Status],
    gk.MinEffDate AS SubmittedDate,
    gk.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    g.[State] AS SitusState,
    TRY_CAST(REPLACE(JSON_VALUE(gk.ConfigJson, '$[0].brokerId'), 'P', '') AS BIGINT) AS BrokerId,
    b.Name AS BrokerName,
    CONCAT('G', gk.GroupId) AS GroupId,
    g.Name AS GroupName,
    'Granular' AS Notes,
    CONCAT('["', gk.ProductCode, '"]') AS ProductCodes,
    CASE WHEN gk.PlanCode = '*' THEN '*' ELSE CONCAT('["', gk.PlanCode, '"]') END AS PlanCodes,
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', gk.ConfigJson), 2) AS SplitConfigHash,
    gk.EffYear AS DateRangeFrom,
    gk.EffYear AS DateRangeTo,
    1 AS EnableEffectiveDateFiltering,
    gk.MinEffDate AS EffectiveDateFrom,
    CASE WHEN gk.MaxEffDate <> gk.MinEffDate THEN gk.MaxEffDate ELSE NULL END AS EffectiveDateTo,
    CASE WHEN gk.PlanCode = '*' THEN 0 ELSE 1 END AS EnablePlanCodeFiltering,
    CASE WHEN gk.PlanCode = '*' THEN NULL ELSE CONCAT('["', gk.PlanCode, '"]') END AS PlanCodeConstraints,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[granular_keys] gk
LEFT JOIN #max_proposal_num mpn ON mpn.GroupId = CONCAT('G', gk.GroupId)
LEFT JOIN [etl].[stg_groups] g ON g.Id = CONCAT('G', gk.GroupId)
LEFT JOIN [etl].[stg_brokers] b ON b.Id = TRY_CAST(REPLACE(JSON_VALUE(gk.ConfigJson, '$[0].brokerId'), 'P', '') AS BIGINT);

DECLARE @proposals_created INT = @@ROWCOUNT;
PRINT 'Proposals created: ' + CAST(@proposals_created AS VARCHAR);

-- =============================================================================
-- Step 4: Add key mappings for granular proposals
-- =============================================================================
PRINT '';
PRINT 'Step 4: Adding key mappings...';

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
FROM [etl].[cert_split_configs_remainder3] csc
INNER JOIN [etl].[stg_proposals] p 
    ON p.GroupId = CONCAT('G', csc.GroupId)
    AND p.ProductCodes = CONCAT('["', csc.ProductCode, '"]')
    AND (p.PlanCodes = CONCAT('["', csc.PlanCode, '"]') OR (p.PlanCodes = '*' AND csc.PlanCode = '*'))
    AND p.DateRangeFrom = YEAR(csc.EffectiveDate)
    AND p.Notes = 'Granular'
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
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';
PRINT '';
PRINT 'GRANULAR PROPOSALS:';
PRINT '  Input certificates: ' + CAST(@input_certs AS VARCHAR);
PRINT '  Input groups: ' + CAST(@input_groups AS VARCHAR);
PRINT '  Distinct keys: ' + CAST(@keys_count AS VARCHAR);
PRINT '  Proposals created: ' + CAST(@proposals_created AS VARCHAR);
PRINT '  Key mappings: ' + CAST(@mappings_created AS VARCHAR);
PRINT '';

DECLARE @total_proposals INT = (SELECT COUNT(*) FROM [etl].[stg_proposals]);
DECLARE @total_mappings INT = (SELECT COUNT(*) FROM [etl].[stg_proposal_key_mapping]);
PRINT 'TOTALS:';
PRINT '  Total proposals: ' + CAST(@total_proposals AS VARCHAR);
PRINT '  Total key mappings: ' + CAST(@total_mappings AS VARCHAR);
PRINT '';
PRINT '============================================================';
PRINT 'STEP 5 COMPLETED - Ready for consolidation';
PRINT '============================================================';

GO
