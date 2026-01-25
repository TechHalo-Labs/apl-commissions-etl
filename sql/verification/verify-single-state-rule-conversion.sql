-- =============================================================================
-- Verification: Single State Rule to Catch-All Conversion
-- =============================================================================
-- Verifies that hierarchy versions with exactly one state rule have been
-- correctly converted to catch-all rules (Type=1, ShortName='ALL', no states)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'VERIFICATION: Single State Rule to Catch-All Conversion';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Count hierarchy versions by state rule count
-- =============================================================================
PRINT 'Step 1: Counting hierarchy versions by state rule count...';
PRINT '';

SELECT 
    CASE 
        WHEN state_rule_count = 1 THEN 'Exactly 1 state rule'
        WHEN state_rule_count > 1 THEN 'Multiple state rules'
        ELSE 'No state rules'
    END AS category,
    COUNT(*) AS hierarchy_version_count
FROM (
    SELECT 
        HierarchyVersionId,
        COUNT(*) AS state_rule_count
    FROM [etl].[stg_state_rules]
    WHERE IsDeleted = 0
    GROUP BY HierarchyVersionId
) counts
GROUP BY 
    CASE 
        WHEN state_rule_count = 1 THEN 'Exactly 1 state rule'
        WHEN state_rule_count > 1 THEN 'Multiple state rules'
        ELSE 'No state rules'
    END
ORDER BY category;

PRINT '';

-- =============================================================================
-- Step 2: Verify single-state-rule hierarchies are catch-all
-- =============================================================================
PRINT 'Step 2: Verifying single-state-rule hierarchies are catch-all...';
PRINT '';

WITH SingleRuleHierarchies AS (
    SELECT HierarchyVersionId
    FROM [etl].[stg_state_rules]
    WHERE IsDeleted = 0
    GROUP BY HierarchyVersionId
    HAVING COUNT(*) = 1
),
CatchAllRules AS (
    SELECT 
        sr.Id,
        sr.HierarchyVersionId,
        sr.ShortName,
        sr.Name,
        sr.[Type],
        COUNT(srs.Id) AS state_count
    FROM [etl].[stg_state_rules] sr
    INNER JOIN SingleRuleHierarchies srh ON sr.HierarchyVersionId = srh.HierarchyVersionId
    LEFT JOIN [etl].[stg_state_rule_states] srs ON srs.StateRuleId = sr.Id AND srs.IsDeleted = 0
    WHERE sr.IsDeleted = 0
    GROUP BY sr.Id, sr.HierarchyVersionId, sr.ShortName, sr.Name, sr.[Type]
)
SELECT 
    CASE 
        WHEN ShortName = 'ALL' AND [Type] = 1 AND state_count = 0 THEN '✅ CORRECT (catch-all)'
        ELSE '❌ INCORRECT (should be catch-all)'
    END AS status,
    COUNT(*) AS count,
    STRING_AGG(CAST(HierarchyVersionId AS NVARCHAR(MAX)), ', ') AS hierarchy_version_ids
FROM CatchAllRules
GROUP BY 
    CASE 
        WHEN ShortName = 'ALL' AND [Type] = 1 AND state_count = 0 THEN '✅ CORRECT (catch-all)'
        ELSE '❌ INCORRECT (should be catch-all)'
    END;

PRINT '';

-- =============================================================================
-- Step 3: Verify multi-state-rule hierarchies are NOT affected
-- =============================================================================
PRINT 'Step 3: Verifying multi-state-rule hierarchies are NOT affected...';
PRINT '';

WITH MultiRuleHierarchies AS (
    SELECT HierarchyVersionId
    FROM [etl].[stg_state_rules]
    WHERE IsDeleted = 0
    GROUP BY HierarchyVersionId
    HAVING COUNT(*) > 1
),
MultiRuleStateRules AS (
    SELECT 
        sr.Id,
        sr.HierarchyVersionId,
        sr.ShortName,
        sr.Name,
        sr.[Type],
        COUNT(srs.Id) AS state_count
    FROM [etl].[stg_state_rules] sr
    INNER JOIN MultiRuleHierarchies mrh ON sr.HierarchyVersionId = mrh.HierarchyVersionId
    LEFT JOIN [etl].[stg_state_rule_states] srs ON srs.StateRuleId = sr.Id AND srs.IsDeleted = 0
    WHERE sr.IsDeleted = 0
    GROUP BY sr.Id, sr.HierarchyVersionId, sr.ShortName, sr.Name, sr.[Type]
)
SELECT 
    CASE 
        WHEN ShortName = 'ALL' AND [Type] = 1 THEN '❌ INCORRECT (should NOT be catch-all)'
        ELSE '✅ CORRECT (not catch-all)'
    END AS status,
    COUNT(*) AS count,
    STRING_AGG(CAST(HierarchyVersionId AS NVARCHAR(MAX)), ', ') AS hierarchy_version_ids
FROM MultiRuleStateRules
GROUP BY 
    CASE 
        WHEN ShortName = 'ALL' AND [Type] = 1 THEN '❌ INCORRECT (should NOT be catch-all)'
        ELSE '✅ CORRECT (not catch-all)'
    END;

PRINT '';

-- =============================================================================
-- Step 4: Detailed breakdown of single-rule hierarchies
-- =============================================================================
PRINT 'Step 4: Detailed breakdown of single-rule hierarchies (sample)...';
PRINT '';

WITH SingleRuleHierarchies AS (
    SELECT HierarchyVersionId
    FROM [etl].[stg_state_rules]
    WHERE IsDeleted = 0
    GROUP BY HierarchyVersionId
    HAVING COUNT(*) = 1
)
SELECT TOP 20
    sr.HierarchyVersionId,
    sr.Id AS StateRuleId,
    sr.ShortName,
    sr.Name,
    sr.[Type],
    sr.[Description],
    COUNT(srs.Id) AS state_count,
    CASE 
        WHEN sr.ShortName = 'ALL' AND sr.[Type] = 1 AND COUNT(srs.Id) = 0 THEN '✅ Catch-all'
        ELSE '❌ Not catch-all'
    END AS conversion_status
FROM [etl].[stg_state_rules] sr
INNER JOIN SingleRuleHierarchies srh ON sr.HierarchyVersionId = srh.HierarchyVersionId
LEFT JOIN [etl].[stg_state_rule_states] srs ON srs.StateRuleId = sr.Id AND srs.IsDeleted = 0
WHERE sr.IsDeleted = 0
GROUP BY sr.HierarchyVersionId, sr.Id, sr.ShortName, sr.Name, sr.[Type], sr.[Description]
ORDER BY conversion_status, sr.HierarchyVersionId;

PRINT '';

-- =============================================================================
-- Step 5: Summary statistics
-- =============================================================================
PRINT 'Step 5: Summary statistics...';
PRINT '';

SELECT 
    'Total hierarchy versions' AS metric,
    COUNT(DISTINCT HierarchyVersionId) AS value
FROM [etl].[stg_state_rules]
WHERE IsDeleted = 0

UNION ALL

SELECT 
    'Hierarchy versions with exactly 1 state rule' AS metric,
    COUNT(DISTINCT HierarchyVersionId) AS value
FROM [etl].[stg_state_rules]
WHERE IsDeleted = 0
GROUP BY HierarchyVersionId
HAVING COUNT(*) = 1

UNION ALL

SELECT 
    'Hierarchy versions with multiple state rules' AS metric,
    COUNT(DISTINCT HierarchyVersionId) AS value
FROM [etl].[stg_state_rules]
WHERE IsDeleted = 0
GROUP BY HierarchyVersionId
HAVING COUNT(*) > 1

UNION ALL

SELECT 
    'Catch-all state rules (Type=1, ShortName=ALL)' AS metric,
    COUNT(*) AS value
FROM [etl].[stg_state_rules]
WHERE IsDeleted = 0
  AND [Type] = 1
  AND ShortName = 'ALL'

UNION ALL

SELECT 
    'State rules with no states (catch-all)' AS metric,
    COUNT(*) AS value
FROM [etl].[stg_state_rules] sr
WHERE sr.IsDeleted = 0
  AND NOT EXISTS (
      SELECT 1 
      FROM [etl].[stg_state_rule_states] srs 
      WHERE srs.StateRuleId = sr.Id 
        AND srs.IsDeleted = 0
  );

PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION COMPLETE';
PRINT '============================================================';
PRINT '';

GO
