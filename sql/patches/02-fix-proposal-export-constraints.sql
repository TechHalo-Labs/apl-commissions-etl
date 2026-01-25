-- =============================================================================
-- PATCH: Fix Proposal Export - Enable Plan Code & Effective Date Constraints
-- =============================================================================
-- The original 07-export-proposals.sql hardcodes:
--   EnablePlanCodeFiltering = 0
--   EnableEffectiveDateFiltering = 0
-- 
-- This patch updates existing proposals to use staging values and provides
-- a corrected INSERT statement for future exports.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PATCH: Fix Proposal Constraints Export';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Update existing proposals from staging
-- =============================================================================
PRINT 'Step 1: Updating existing proposals with constraint values from staging...';

UPDATE p
SET 
    p.EnablePlanCodeFiltering = COALESCE(sp.EnablePlanCodeFiltering, 0),
    p.EnableEffectiveDateFiltering = COALESCE(sp.EnableEffectiveDateFiltering, 0),
    p.ConstrainingEffectiveDateFrom = sp.ConstrainingEffectiveDateFrom,
    p.ConstrainingEffectiveDateTo = sp.ConstrainingEffectiveDateTo,
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
INNER JOIN [etl].[stg_proposals] sp ON sp.Id = p.Id
WHERE 
    -- Only update if staging has non-default values
    (sp.EnablePlanCodeFiltering = 1 OR sp.EnableEffectiveDateFiltering = 1
     OR sp.ConstrainingEffectiveDateFrom IS NOT NULL 
     OR sp.ConstrainingEffectiveDateTo IS NOT NULL)
    -- And current values differ from staging
    AND (
        p.EnablePlanCodeFiltering != COALESCE(sp.EnablePlanCodeFiltering, 0)
        OR p.EnableEffectiveDateFiltering != COALESCE(sp.EnableEffectiveDateFiltering, 0)
        OR COALESCE(p.ConstrainingEffectiveDateFrom, '1900-01-01') != COALESCE(sp.ConstrainingEffectiveDateFrom, '1900-01-01')
        OR COALESCE(p.ConstrainingEffectiveDateTo, '1900-01-01') != COALESCE(sp.ConstrainingEffectiveDateTo, '1900-01-01')
    );

DECLARE @updated INT = @@ROWCOUNT;
PRINT 'Proposals updated: ' + CAST(@updated AS VARCHAR);

-- =============================================================================
-- Step 2: Report proposals with constraints enabled
-- =============================================================================
PRINT '';
PRINT 'Step 2: Proposals with constraints enabled...';

SELECT 
    'With Plan Code Filtering' AS ConstraintType,
    COUNT(*) AS [Count]
FROM [dbo].[Proposals]
WHERE EnablePlanCodeFiltering = 1

UNION ALL

SELECT 
    'With Effective Date Filtering' AS ConstraintType,
    COUNT(*) AS [Count]
FROM [dbo].[Proposals]
WHERE EnableEffectiveDateFiltering = 1

UNION ALL

SELECT 
    'Total Proposals' AS ConstraintType,
    COUNT(*) AS [Count]
FROM [dbo].[Proposals];

-- =============================================================================
-- Step 3: Sample of proposals with constraints
-- =============================================================================
PRINT '';
PRINT 'Step 3: Sample proposals with constraints enabled...';

SELECT TOP 10
    p.Id,
    p.ProposalNumber,
    p.GroupName,
    p.EnablePlanCodeFiltering,
    p.EnableEffectiveDateFiltering,
    p.ConstrainingEffectiveDateFrom,
    p.ConstrainingEffectiveDateTo
FROM [dbo].[Proposals] p
WHERE p.EnablePlanCodeFiltering = 1 OR p.EnableEffectiveDateFiltering = 1
ORDER BY p.ProposalNumber;

-- =============================================================================
-- Documentation: Corrected INSERT statement for 07-export-proposals.sql
-- =============================================================================
/*
Replace lines 37-38 in 07-export-proposals.sql:

OLD:
    0 AS EnablePlanCodeFiltering,
    0 AS EnableEffectiveDateFiltering,

NEW:
    COALESCE(sp.EnablePlanCodeFiltering, 0) AS EnablePlanCodeFiltering,
    COALESCE(sp.EnableEffectiveDateFiltering, 0) AS EnableEffectiveDateFiltering,

Also add these columns to the INSERT and SELECT:
    ConstrainingEffectiveDateFrom,
    ConstrainingEffectiveDateTo,
    
In SELECT:
    sp.ConstrainingEffectiveDateFrom,
    sp.ConstrainingEffectiveDateTo,
*/

PRINT '';
PRINT '============================================================';
PRINT 'PATCH COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'NOTE: Also update 07-export-proposals.sql to use staging values';
PRINT 'instead of hardcoded 0 for EnablePlanCodeFiltering and EnableEffectiveDateFiltering.';
PRINT 'See comments in this script for the corrected INSERT statement.';

GO
