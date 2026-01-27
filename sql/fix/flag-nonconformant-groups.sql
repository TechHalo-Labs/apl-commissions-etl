/**
 * Flag Non-Conformant Groups
 * ===========================
 * Sets IsNonConformant = 1 on EmployerGroups identified in etl.non_conformant_keys.
 * 
 * Non-conformant groups are those with multiple split configurations for the same
 * (GroupId, EffectiveDate, ProductCode, PlanCode) key.
 * 
 * Expected: ~462 groups to be flagged
 */

SET NOCOUNT ON;
PRINT '=========================================================';
PRINT 'Flag Non-Conformant Groups';
PRINT '=========================================================';
PRINT '';

-- =====================================================
-- Step 1: Analyze non-conformant keys
-- =====================================================
PRINT 'Step 1: Analyzing non-conformant keys...';

DECLARE @distinctGroups INT;
DECLARE @totalKeys INT;

SELECT 
    @distinctGroups = COUNT(DISTINCT GroupId),
    @totalKeys = COUNT(*)
FROM [etl].[non_conformant_keys];

PRINT '  Distinct non-conformant groups in ETL: ' + CAST(@distinctGroups AS VARCHAR);
PRINT '  Total non-conformant keys: ' + CAST(@totalKeys AS VARCHAR);
PRINT '';

-- Sample non-conformant groups
PRINT 'Sample non-conformant groups:';
SELECT TOP 10
    GroupId,
    EffectiveDate,
    ProductCode,
    PlanCode,
    DistinctConfigs,
    CertCount
FROM [etl].[non_conformant_keys]
ORDER BY GroupId, EffectiveDate;

PRINT '';

-- =====================================================
-- Step 2: Check current flagging status
-- =====================================================
PRINT 'Step 2: Checking current flagging status in production...';

DECLARE @currentlyFlagged INT = (
    SELECT COUNT(*) FROM [dbo].[EmployerGroups] 
    WHERE IsNonConformant = 1
);

PRINT '  Currently flagged in production: ' + CAST(@currentlyFlagged AS VARCHAR) + ' groups';
PRINT '';

-- =====================================================
-- Step 3: Flag non-conformant groups
-- =====================================================
PRINT 'Step 3: Flagging non-conformant groups...';

UPDATE g
SET g.IsNonConformant = 1,
    g.LastModificationTime = GETUTCDATE()
FROM [dbo].[EmployerGroups] g
WHERE EXISTS (
    SELECT 1 FROM [etl].[non_conformant_keys] nck
    WHERE CONCAT('G', nck.GroupId) = g.Id
)
AND (g.IsNonConformant IS NULL OR g.IsNonConformant = 0);  -- Only update if not already flagged

DECLARE @flaggedCount INT = @@ROWCOUNT;
PRINT '  Flagged: ' + CAST(@flaggedCount AS VARCHAR) + ' groups';
PRINT '';

-- =====================================================
-- Step 4: Verify flagging
-- =====================================================
PRINT 'Step 4: Verifying flagging...';

DECLARE @totalGroups INT;
DECLARE @flaggedGroups INT;
DECLARE @conformantGroups INT;

SELECT 
    @totalGroups = COUNT(*),
    @flaggedGroups = COUNT(CASE WHEN IsNonConformant = 1 THEN 1 END),
    @conformantGroups = COUNT(CASE WHEN IsNonConformant = 0 OR IsNonConformant IS NULL THEN 1 END)
FROM [dbo].[EmployerGroups];

PRINT '  Total groups: ' + CAST(@totalGroups AS VARCHAR);
PRINT '  Non-conformant (IsNonConformant = 1): ' + CAST(@flaggedGroups AS VARCHAR) + ' (' + 
      CAST(CAST(@flaggedGroups * 100.0 / @totalGroups AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '  Conformant (IsNonConformant = 0 or NULL): ' + CAST(@conformantGroups AS VARCHAR) + ' (' + 
      CAST(CAST(@conformantGroups * 100.0 / @totalGroups AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '';

-- Sample flagged groups
PRINT 'Sample flagged groups:';
SELECT TOP 10
    g.Id as GroupId,
    g.GroupName,
    g.IsNonConformant,
    g.GroupSize,
    g.SitusState
FROM [dbo].[EmployerGroups] g
WHERE g.IsNonConformant = 1
ORDER BY g.Id;

PRINT '';

-- =====================================================
-- Step 5: Cross-reference with non_conformant_keys
-- =====================================================
PRINT '5. CROSS-REFERENCE CHECK';
PRINT '---------------------------------------------------';

-- Groups in non_conformant_keys but not flagged
DECLARE @missing_flags INT;
SELECT @missing_flags = COUNT(DISTINCT nck.GroupId)
FROM [etl].[non_conformant_keys] nck
LEFT JOIN [dbo].[EmployerGroups] g ON CONCAT('G', nck.GroupId) = g.Id
WHERE g.IsNonConformant IS NULL OR g.IsNonConformant = 0;

IF @missing_flags > 0
    PRINT '   ⚠️  WARNING: ' + CAST(@missing_flags AS VARCHAR) + ' groups in non_conformant_keys not flagged';
ELSE
    PRINT '   ✅ PASS: All non-conformant groups are flagged';

-- Groups flagged but not in non_conformant_keys (should be 0 or manual flags)
DECLARE @extra_flags INT;
SELECT @extra_flags = COUNT(*)
FROM [dbo].[EmployerGroups] g
WHERE g.IsNonConformant = 1
  AND NOT EXISTS (
      SELECT 1 FROM [etl].[non_conformant_keys] nck
      WHERE CONCAT('G', nck.GroupId) = g.Id
  );

IF @extra_flags > 0
    PRINT '   ⚠️  INFO: ' + CAST(@extra_flags AS VARCHAR) + ' groups flagged but not in non_conformant_keys (manual flags?)';
ELSE
    PRINT '   ✅ PASS: All flagged groups are in non_conformant_keys';

PRINT '';

-- =====================================================
-- Step 6: Summary
-- =====================================================
PRINT '=========================================================';
PRINT 'FLAGGING SUMMARY';
PRINT '=========================================================';
PRINT 'Groups in ETL non_conformant_keys: ' + CAST(@distinctGroups AS VARCHAR);
PRINT 'Groups flagged in this run: ' + CAST(@flaggedCount AS VARCHAR);
PRINT 'Total groups now flagged: ' + CAST(@flaggedGroups AS VARCHAR);
PRINT '';

IF @flaggedGroups >= @distinctGroups AND @missing_flags = 0
BEGIN
    PRINT '✅ SUCCESS: All non-conformant groups are properly flagged!';
    PRINT '   Groups can now be filtered using IsNonConformant column';
    PRINT '   Feature flag USE_NONCONFORMANT_FLAG can control PHA routing';
END
ELSE
BEGIN
    PRINT '⚠️  REVIEW NEEDED: Check cross-reference warnings above';
END

PRINT '=========================================================';
