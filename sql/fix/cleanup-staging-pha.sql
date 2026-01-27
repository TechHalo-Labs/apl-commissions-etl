/**
 * Cleanup Staging PolicyHierarchyAssignments
 * ===========================================
 * Removes policies from staging PHA that have successfully resolved to proposals.
 * 
 * Logic:
 * - Keep policies with ProposalId = NULL (cannot resolve)
 * - Keep DTC policies (GroupId = 'G00000') even if they have ProposalId
 * - Remove all other policies with ProposalId (can use proposal resolution)
 * 
 * Expected Impact:
 * - Before: 120,352 policies in staging PHA
 * - Should be removed: 117,381 policies (have ProposalId, not DTC)
 * - After: 2,971 policies (2,866 DTC + 105 unresolved)
 */

SET NOCOUNT ON;
PRINT '=========================================================';
PRINT 'Staging PolicyHierarchyAssignments Cleanup';
PRINT '=========================================================';
PRINT '';

-- =====================================================
-- Step 1: Analyze current staging PHA state
-- =====================================================
PRINT 'Step 1: Analyzing current staging PHA state...';

DECLARE @totalPHA INT;
DECLARE @dtcPolicies INT;
DECLARE @withProposal INT;
DECLARE @withoutProposal INT;
DECLARE @willBeDeleted INT;

SELECT 
    @totalPHA = COUNT(DISTINCT pha.PolicyId),
    @dtcPolicies = COUNT(DISTINCT CASE WHEN p.GroupId = 'G00000' THEN pha.PolicyId END),
    @withProposal = COUNT(DISTINCT CASE WHEN p.ProposalId IS NOT NULL THEN pha.PolicyId END),
    @withoutProposal = COUNT(DISTINCT CASE WHEN p.ProposalId IS NULL THEN pha.PolicyId END),
    @willBeDeleted = COUNT(DISTINCT CASE WHEN p.ProposalId IS NOT NULL AND p.GroupId != 'G00000' THEN pha.PolicyId END)
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId;

PRINT '  Total unique policies in staging PHA: ' + CAST(@totalPHA AS VARCHAR);
PRINT '  DTC policies (GroupId = G00000): ' + CAST(@dtcPolicies AS VARCHAR);
PRINT '  With ProposalId: ' + CAST(@withProposal AS VARCHAR);
PRINT '  Without ProposalId: ' + CAST(@withoutProposal AS VARCHAR);
PRINT '';
PRINT '  Will be deleted (ProposalId NOT NULL AND not DTC): ' + CAST(@willBeDeleted AS VARCHAR);
PRINT '  Will remain (ProposalId IS NULL OR DTC): ' + CAST(@totalPHA - @willBeDeleted AS VARCHAR);
PRINT '';

-- =====================================================
-- Step 2: Delete policies with ProposalId (except DTC)
-- =====================================================
PRINT 'Step 2: Deleting policies with ProposalId from staging PHA (keeping DTC)...';

DELETE FROM [etl].[stg_policy_hierarchy_assignments]
WHERE PolicyId IN (
    SELECT p.Id 
    FROM [etl].[stg_policies] p
    WHERE p.ProposalId IS NOT NULL  -- Can resolve to proposal
      AND p.GroupId != 'G00000'     -- Not DTC
);

DECLARE @deletedCount INT = @@ROWCOUNT;
PRINT '  Deleted: ' + CAST(@deletedCount AS VARCHAR) + ' PHA records';
PRINT '';

-- =====================================================
-- Step 3: Verify cleanup
-- =====================================================
PRINT 'Step 3: Verifying cleanup...';

DECLARE @remainingRecords INT;
DECLARE @remainingPolicies INT;
DECLARE @remainingDTC INT;
DECLARE @remainingWithoutProposal INT;
DECLARE @remainingShouldNotBeHere INT;

SELECT 
    @remainingRecords = COUNT(*),
    @remainingPolicies = COUNT(DISTINCT pha.PolicyId),
    @remainingDTC = COUNT(DISTINCT CASE WHEN p.GroupId = 'G00000' THEN pha.PolicyId END),
    @remainingWithoutProposal = COUNT(DISTINCT CASE WHEN p.ProposalId IS NULL THEN pha.PolicyId END),
    @remainingShouldNotBeHere = COUNT(DISTINCT CASE WHEN p.ProposalId IS NOT NULL AND p.GroupId != 'G00000' THEN pha.PolicyId END)
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId;

PRINT '  Remaining PHA records: ' + CAST(@remainingRecords AS VARCHAR);
PRINT '  Remaining unique policies: ' + CAST(@remainingPolicies AS VARCHAR);
PRINT '  DTC policies: ' + CAST(@remainingDTC AS VARCHAR);
PRINT '  Without ProposalId: ' + CAST(@remainingWithoutProposal AS VARCHAR);
PRINT '  Should not be here (ProposalId NOT NULL, not DTC): ' + CAST(@remainingShouldNotBeHere AS VARCHAR);
PRINT '';

-- =====================================================
-- Step 4: Sample remaining policies
-- =====================================================
PRINT 'Step 4: Sample of remaining policies in staging PHA:';
PRINT '';

SELECT TOP 10
    pha.Id as PHA_Id,
    p.Id as PolicyId,
    p.GroupId,
    p.ProposalId,
    CASE WHEN p.GroupId = 'G00000' THEN 'DTC' 
         WHEN p.ProposalId IS NULL THEN 'No Proposal' 
         ELSE 'OTHER' END as Reason
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId
ORDER BY pha.Id;

PRINT '';

-- =====================================================
-- Step 5: Summary
-- =====================================================
PRINT '=========================================================';
PRINT 'STAGING PHA CLEANUP SUMMARY';
PRINT '=========================================================';
PRINT 'Before: ' + CAST(@totalPHA AS VARCHAR) + ' unique policies in staging PHA';
PRINT 'Deleted: ' + CAST(@deletedCount AS VARCHAR) + ' PHA records (policies had ProposalId, not DTC)';
PRINT 'Remaining: ' + CAST(@remainingPolicies AS VARCHAR) + ' unique policies (' + CAST(@remainingRecords AS VARCHAR) + ' records)';
PRINT '';

IF @remainingShouldNotBeHere > 0
BEGIN
    PRINT '⚠️  WARNING: ' + CAST(@remainingShouldNotBeHere AS VARCHAR) + ' policies should not be in staging PHA!';
    PRINT '   These have ProposalId but are not DTC. Manual investigation needed.';
END
ELSE
BEGIN
    PRINT '✅ SUCCESS: All remaining policies are correctly in staging PHA';
    PRINT '   ' + CAST(@remainingDTC AS VARCHAR) + ' DTC policies + ' + CAST(@remainingWithoutProposal AS VARCHAR) + ' unresolved policies';
END

PRINT '';
PRINT 'Next step: Export to production using 14-export-policy-hierarchy-assignments.sql';
PRINT '=========================================================';
