-- =============================================================================
-- Transform: Populate Pre-Stage Split Configuration JSON
-- 
-- Generates SplitConfigurationJSON and SplitConfigurationMD5 for all pre-stage
-- proposals. This JSON contains the full split configuration with hierarchy
-- details, enabling transparent consolidation with full audit trail.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'POPULATE SPLIT CONFIGURATION JSON';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Update proposals with split configuration JSON
-- =============================================================================
PRINT 'Step 1: Generating split configuration JSON for all proposals...';

-- Build JSON structure with full hierarchy details
UPDATE pp
SET 
    pp.SplitConfigurationJSON = (
        SELECT
            psv.TotalSplitPercent AS totalSplitPercent,
            (
                SELECT 
                    psp.SplitPercent AS splitPercent,
                    psp.HierarchyId AS hierarchyId,
                    (
                        SELECT
                            h.Id AS hierarchyId,
                            h.Name AS hierarchyName,
                            h.GroupId AS groupId,
                            h.BrokerId AS brokerId,
                            h.BrokerName AS brokerName,
                            hv.EffectiveFrom AS effectiveFrom,
                            hv.EffectiveTo AS effectiveTo,
                            (
                                SELECT
                                    hp.[Level] AS [level],
                                    hp.EntityId AS brokerId,
                                    hp.EntityName AS brokerName,
                                    hp.SplitPercent AS splitPercent,
                                    hp.CommissionRate AS commissionRate,
                                    hp.ScheduleCode AS scheduleCode,
                                    COALESCE(hp.ScheduleId, 0) AS scheduleId,
                                    COALESCE(s.Name, '') AS scheduleName,
                                    s.ExternalId AS scheduleExternalId
                                FROM [prestage].[prestage_hierarchy_participants] hp
                                LEFT JOIN [$(ETL_SCHEMA)].[stg_schedules] s ON s.Id = hp.ScheduleId
                                WHERE hp.HierarchyVersionId = hv.Id
                                ORDER BY hp.[Level], hp.SortOrder
                                FOR JSON PATH
                            ) AS participants
                        FROM [prestage].[prestage_hierarchies] h
                        INNER JOIN [prestage].[prestage_hierarchy_versions] hv ON hv.HierarchyId = h.Id
                        WHERE h.Id = psp.HierarchyId
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                    ) AS hierarchy
                FROM [prestage].[prestage_premium_split_participants] psp
                WHERE psp.VersionId = psv.Id
                ORDER BY psp.Sequence
                FOR JSON PATH
            ) AS splits
        FROM [prestage].[prestage_premium_split_versions] psv
        WHERE psv.ProposalId = pp.Id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
FROM [prestage].[prestage_proposals] pp
WHERE EXISTS (
    SELECT 1 FROM [prestage].[prestage_premium_split_versions] psv
    WHERE psv.ProposalId = pp.Id
);

DECLARE @proposals_with_json INT = @@ROWCOUNT;
PRINT 'Proposals with split configuration JSON: ' + CAST(@proposals_with_json AS VARCHAR);

-- =============================================================================
-- Step 2: Generate MD5 hashes for split configurations
-- =============================================================================
PRINT '';
PRINT 'Step 2: Generating MD5 hashes for split configurations...';

UPDATE pp
SET 
    pp.SplitConfigurationMD5 = CONVERT(
        CHAR(32), 
        HASHBYTES('MD5', pp.SplitConfigurationJSON), 
        2
    )
FROM [prestage].[prestage_proposals] pp
WHERE pp.SplitConfigurationJSON IS NOT NULL;

DECLARE @proposals_with_md5 INT = @@ROWCOUNT;
PRINT 'Proposals with MD5 hash: ' + CAST(@proposals_with_md5 AS VARCHAR);

-- =============================================================================
-- Step 3: Verification
-- =============================================================================
PRINT '';
PRINT 'Step 3: Verifying split configuration data...';

-- Count proposals by split config status
DECLARE @total_proposals INT = (SELECT COUNT(*) FROM [prestage].[prestage_proposals]);
DECLARE @with_config INT = (SELECT COUNT(*) FROM [prestage].[prestage_proposals] WHERE SplitConfigurationJSON IS NOT NULL);
DECLARE @with_md5 INT = (SELECT COUNT(*) FROM [prestage].[prestage_proposals] WHERE SplitConfigurationMD5 IS NOT NULL);
DECLARE @missing_config INT = @total_proposals - @with_config;

PRINT '';
PRINT 'Split Configuration Summary:';
PRINT '  Total proposals: ' + CAST(@total_proposals AS VARCHAR);
PRINT '  With split config JSON: ' + CAST(@with_config AS VARCHAR);
PRINT '  With MD5 hash: ' + CAST(@with_md5 AS VARCHAR);
PRINT '  Missing config: ' + CAST(@missing_config AS VARCHAR);

-- Show sample split configuration (first proposal with config)
PRINT '';
PRINT 'Sample split configuration (first 500 chars):';
SELECT TOP 1
    Id,
    LEFT(SplitConfigurationJSON, 500) AS ConfigSample,
    SplitConfigurationMD5
FROM [prestage].[prestage_proposals]
WHERE SplitConfigurationJSON IS NOT NULL
ORDER BY Id;

-- Check for duplicate MD5 hashes (proposals with same split config)
DECLARE @unique_configs INT = (SELECT COUNT(DISTINCT SplitConfigurationMD5) FROM [prestage].[prestage_proposals] WHERE SplitConfigurationMD5 IS NOT NULL);
PRINT '';
PRINT 'Unique split configurations (MD5): ' + CAST(@unique_configs AS VARCHAR);

IF @unique_configs < @with_md5
BEGIN
    PRINT 'Note: Multiple proposals share the same split configuration (will consolidate)';
    
    -- Show top 5 most common configurations
    SELECT TOP 5
        SplitConfigurationMD5,
        COUNT(*) AS ProposalCount
    FROM [prestage].[prestage_proposals]
    WHERE SplitConfigurationMD5 IS NOT NULL
    GROUP BY SplitConfigurationMD5
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC;
END

-- =============================================================================
-- Warnings
-- =============================================================================
IF @missing_config > 0
BEGIN
    PRINT '';
    PRINT 'WARNING: ' + CAST(@missing_config AS VARCHAR) + ' proposals have no split configuration JSON.';
    PRINT 'This may be expected for special case proposals or non-conformant groups.';
    PRINT '';
    PRINT 'Sample proposals without split config:';
    SELECT TOP 5
        Id,
        GroupId,
        Notes,
        SpecialCase,
        SpecialCaseCode
    FROM [prestage].[prestage_proposals]
    WHERE SplitConfigurationJSON IS NULL
    ORDER BY Id;
END

PRINT '';
PRINT '============================================================';
PRINT 'SPLIT CONFIGURATION POPULATION COMPLETE';
PRINT '============================================================';

GO
