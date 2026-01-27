-- =============================================================================
-- Fix Proposal EffectiveDateFrom to Cover All Policies
-- 
-- This script selectively updates proposals where EffectiveDateFrom is set
-- too high (later than some policies), causing policy resolution failures.
--
-- Strategy:
-- 1. Calculate the TRUE minimum effective date from ALL certificates/policies
-- 2. Only update proposals where EffectiveDateFrom > true minimum
-- 3. Preserve other proposal data (no full export needed)
-- =============================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

PRINT '============================================================';
PRINT 'FIX: Proposal EffectiveDateFrom to Cover All Policies';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Identify proposals with incorrect EffectiveDateFrom
-- =============================================================================
PRINT 'Step 1: Identifying proposals with incorrect EffectiveDateFrom...';

DROP TABLE IF EXISTS #proposals_to_fix;

-- Step 1a: Calculate true minimum effective date per group
DROP TABLE IF EXISTS #group_min_dates;

SELECT 
    CAST(GroupId AS NVARCHAR(100)) AS GroupId,
    MIN(EffectiveDate) AS TrueMinEffectiveDate
INTO #group_min_dates
FROM [dbo].[Policies]
WHERE GroupId IS NOT NULL
  AND EffectiveDate IS NOT NULL
GROUP BY CAST(GroupId AS NVARCHAR(100))

UNION

SELECT 
    CAST(GroupId AS NVARCHAR(100)) AS GroupId,
    MIN(EffectiveDate) AS TrueMinEffectiveDate
FROM [dbo].[Certificates]
WHERE GroupId IS NOT NULL
  AND EffectiveDate IS NOT NULL
GROUP BY CAST(GroupId AS NVARCHAR(100));

-- Step 1b: Get the absolute minimum per group
DROP TABLE IF EXISTS #group_absolute_min;

SELECT 
    GroupId,
    MIN(TrueMinEffectiveDate) AS TrueMinEffectiveDate
INTO #group_absolute_min
FROM #group_min_dates
GROUP BY GroupId;

-- Step 1c: Count affected policies per proposal
DROP TABLE IF EXISTS #affected_policy_counts;

SELECT 
    pol.ProposalId,
    pol.GroupId,
    COUNT(*) AS AffectedPolicyCount
INTO #affected_policy_counts
FROM [dbo].[Policies] pol
INNER JOIN [dbo].[Proposals] p ON p.Id = pol.ProposalId
WHERE pol.EffectiveDate < p.EffectiveDateFrom
  AND pol.GroupId = p.GroupId
GROUP BY pol.ProposalId, pol.GroupId;

-- Step 1d: Find proposals where EffectiveDateFrom is later than some policies
SELECT 
    p.Id AS ProposalId,
    p.GroupId,
    p.EffectiveDateFrom AS CurrentEffectiveDateFrom,
    COALESCE(gam.TrueMinEffectiveDate, p.EffectiveDateFrom) AS TrueMinEffectiveDate,
    COALESCE(apc.AffectedPolicyCount, 0) AS AffectedPolicyCount
INTO #proposals_to_fix
FROM [dbo].[Proposals] p
LEFT JOIN #group_absolute_min gam ON gam.GroupId = p.GroupId
LEFT JOIN #affected_policy_counts apc ON apc.ProposalId = p.Id
WHERE p.EffectiveDateFrom IS NOT NULL
  AND COALESCE(apc.AffectedPolicyCount, 0) > 0;

DECLARE @proposals_to_fix_count INT = (SELECT COUNT(*) FROM #proposals_to_fix);
PRINT 'Found ' + CAST(@proposals_to_fix_count AS VARCHAR) + ' proposals with incorrect EffectiveDateFrom';
PRINT '';

-- Show affected proposals
IF @proposals_to_fix_count > 0
BEGIN
    PRINT 'Affected Proposals:';
    SELECT 
        ProposalId,
        GroupId,
        CurrentEffectiveDateFrom,
        TrueMinEffectiveDate,
        AffectedPolicyCount,
        CASE 
            WHEN TrueMinEffectiveDate < CurrentEffectiveDateFrom THEN 'NEEDS FIX'
            ELSE 'OK'
        END AS Status
    FROM #proposals_to_fix
    ORDER BY AffectedPolicyCount DESC, GroupId;
    PRINT '';
END
ELSE
BEGIN
    PRINT 'No proposals need fixing. All EffectiveDateFrom values are correct.';
    PRINT '';
    PRINT '============================================================';
    PRINT 'FIX COMPLETE - NO CHANGES NEEDED';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 2: Preview changes (DRY RUN)
-- =============================================================================
PRINT 'Step 2: Preview of changes (DRY RUN)...';
PRINT '';

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    p.GroupName,
    p.CurrentEffectiveDateFrom AS OldEffectiveDateFrom,
    ptf.TrueMinEffectiveDate AS NewEffectiveDateFrom,
    ptf.AffectedPolicyCount,
    DATEDIFF(DAY, p.CurrentEffectiveDateFrom, ptf.TrueMinEffectiveDate) AS DaysDifference
FROM [dbo].[Proposals] p
INNER JOIN #proposals_to_fix ptf ON ptf.ProposalId = p.Id
WHERE ptf.TrueMinEffectiveDate < ptf.CurrentEffectiveDateFrom
ORDER BY ptf.AffectedPolicyCount DESC, p.GroupId;

DECLARE @total_affected_policies INT;
SELECT @total_affected_policies = SUM(AffectedPolicyCount) FROM #proposals_to_fix;
PRINT '';
PRINT 'Total policies that will be fixed: ' + CAST(COALESCE(@total_affected_policies, 0) AS VARCHAR);
PRINT '';

-- =============================================================================
-- Step 3: Apply fixes (COMMENTED OUT FOR SAFETY - UNCOMMENT TO EXECUTE)
-- =============================================================================
PRINT 'Step 3: Applying fixes...';
PRINT '';
PRINT '⚠️  WARNING: The UPDATE statement below is COMMENTED OUT for safety.';
PRINT '    Review the preview above, then uncomment the UPDATE to execute.';
PRINT '';

/*
-- Uncomment this block to execute the fix
UPDATE p
SET 
    p.EffectiveDateFrom = ptf.TrueMinEffectiveDate,
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
INNER JOIN #proposals_to_fix ptf ON ptf.ProposalId = p.Id
WHERE ptf.TrueMinEffectiveDate < ptf.CurrentEffectiveDateFrom
  AND ptf.TrueMinEffectiveDate IS NOT NULL;

DECLARE @updated_count INT = @@ROWCOUNT;
PRINT 'Updated EffectiveDateFrom for ' + CAST(@updated_count AS VARCHAR) + ' proposals';
PRINT '';

-- =============================================================================
-- Step 4: Verify fixes
-- =============================================================================
PRINT 'Step 4: Verifying fixes...';
PRINT '';

SELECT 
    p.Id AS ProposalId,
    p.GroupId,
    p.EffectiveDateFrom,
    COUNT(pol.Id) AS PoliciesWithEarlierDates,
    CASE 
        WHEN COUNT(pol.Id) = 0 THEN '✅ FIXED'
        ELSE '❌ STILL HAS ISSUES'
    END AS Status
FROM [dbo].[Proposals] p
LEFT JOIN [dbo].[Policies] pol ON pol.GroupId = p.GroupId 
    AND pol.EffectiveDate < p.EffectiveDateFrom
    AND pol.ProposalId = p.Id
WHERE p.Id IN (SELECT ProposalId FROM #proposals_to_fix)
GROUP BY p.Id, p.GroupId, p.EffectiveDateFrom
ORDER BY PoliciesWithEarlierDates DESC;
*/

-- Cleanup
DROP TABLE IF EXISTS #proposals_to_fix;
DROP TABLE IF EXISTS #affected_policy_counts;
DROP TABLE IF EXISTS #group_absolute_min;
DROP TABLE IF EXISTS #group_min_dates;

PRINT '';
PRINT '============================================================';
PRINT 'FIX SCRIPT COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Review the preview above';
PRINT '2. Verify the affected proposals and policy counts';
PRINT '3. Uncomment the UPDATE block to execute the fix';
PRINT '4. Run verification query to confirm fixes';
PRINT '';

GO
