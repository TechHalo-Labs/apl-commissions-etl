/**
 * Cleanup PolicyHierarchyAssignments
 * ===================================
 * Removes policies from PHA that have successfully resolved to proposals.
 * 
 * Logic:
 * - Keep policies with ProposalId = NULL (cannot resolve)
 * - Keep DTC policies (GroupId = 'G00000') even if they have ProposalId
 * - Remove all other policies with ProposalId (can use proposal resolution)
 * 
 * Expected Impact:
 * - Before: 92,324 policies in PHA (all with ProposalId)
 * - After: 0-1,000 policies (only DTC and truly unresolved)
 */

SET NOCOUNT ON;
PRINT '=========================================================';
PRINT 'PolicyHierarchyAssignments Cleanup';
PRINT '=========================================================';
PRINT '';

-- =====================================================
-- Step 1: Backup PHA table
-- =====================================================
PRINT 'Step 1: Backing up PolicyHierarchyAssignments...';

-- Drop backup table if it exists
IF OBJECT_ID('[new_data].[PolicyHierarchyAssignments_cleanup_backup]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [new_data].[PolicyHierarchyAssignments_cleanup_backup];
    PRINT '  Dropped existing backup table';
END

-- Create backup
SELECT * 
INTO [new_data].[PolicyHierarchyAssignments_cleanup_backup]
FROM [dbo].[PolicyHierarchyAssignments];

DECLARE @backupCount INT = @@ROWCOUNT;
PRINT '  Backed up ' + CAST(@backupCount AS VARCHAR) + ' records to new_data.PolicyHierarchyAssignments_cleanup_backup';
PRINT '';

-- =====================================================
-- Step 2: Analyze current state
-- =====================================================
PRINT 'Step 2: Analyzing current PHA state...';

DECLARE @totalPHA INT;
DECLARE @withProposal INT;
DECLARE @withoutProposal INT;
DECLARE @dtcPolicies INT;
DECLARE @willBeDeleted INT;

SELECT 
    @totalPHA = COUNT(*),
    @withProposal = COUNT(CASE WHEN p.ProposalId IS NOT NULL THEN 1 END),
    @withoutProposal = COUNT(CASE WHEN p.ProposalId IS NULL THEN 1 END),
    @dtcPolicies = COUNT(CASE WHEN p.GroupId = 'G00000' THEN 1 END),
    @willBeDeleted = COUNT(CASE WHEN p.ProposalId IS NOT NULL AND p.GroupId != 'G00000' THEN 1 END)
FROM [dbo].[PolicyHierarchyAssignments] pha
INNER JOIN [dbo].[Policies] p ON p.Id = pha.PolicyId;

PRINT '  Total policies in PHA: ' + CAST(@totalPHA AS VARCHAR);
PRINT '  With ProposalId: ' + CAST(@withProposal AS VARCHAR);
PRINT '  Without ProposalId: ' + CAST(@withoutProposal AS VARCHAR);
PRINT '  DTC policies (GroupId = G00000): ' + CAST(@dtcPolicies AS VARCHAR);
PRINT '';
PRINT '  Will be deleted (ProposalId NOT NULL AND not DTC): ' + CAST(@willBeDeleted AS VARCHAR);
PRINT '  Will remain (ProposalId IS NULL OR DTC): ' + CAST(@totalPHA - @willBeDeleted AS VARCHAR);
PRINT '';

-- =====================================================
-- Step 3: Delete policies with ProposalId (except DTC)
-- =====================================================
PRINT 'Step 3: Deleting policies with ProposalId from PHA (keeping DTC)...';

DELETE FROM [dbo].[PolicyHierarchyAssignments]
WHERE PolicyId IN (
    SELECT p.Id 
    FROM [dbo].[Policies] p
    WHERE p.ProposalId IS NOT NULL  -- Can resolve to proposal
      AND p.GroupId != 'G00000'     -- Not DTC
);

DECLARE @deletedCount INT = @@ROWCOUNT;
PRINT '  Deleted: ' + CAST(@deletedCount AS VARCHAR) + ' policies from PHA';
PRINT '';

-- =====================================================
-- Step 4: Verify cleanup
-- =====================================================
PRINT 'Step 4: Verifying cleanup...';

DECLARE @remainingTotal INT;
DECLARE @remainingDTC INT;
DECLARE @remainingWithoutProposal INT;
DECLARE @remainingShouldNotBeHere INT;

SELECT 
    @remainingTotal = COUNT(*),
    @remainingDTC = COUNT(CASE WHEN p.GroupId = 'G00000' THEN 1 END),
    @remainingWithoutProposal = COUNT(CASE WHEN p.ProposalId IS NULL THEN 1 END),
    @remainingShouldNotBeHere = COUNT(CASE WHEN p.ProposalId IS NOT NULL AND p.GroupId != 'G00000' THEN 1 END)
FROM [dbo].[PolicyHierarchyAssignments] pha
INNER JOIN [dbo].[Policies] p ON p.Id = pha.PolicyId;

PRINT '  Remaining in PHA: ' + CAST(@remainingTotal AS VARCHAR);
PRINT '  DTC policies: ' + CAST(@remainingDTC AS VARCHAR);
PRINT '  Without ProposalId: ' + CAST(@remainingWithoutProposal AS VARCHAR);
PRINT '  Should not be here (ProposalId NOT NULL, not DTC): ' + CAST(@remainingShouldNotBeHere AS VARCHAR);
PRINT '';

-- =====================================================
-- Step 5: Sample remaining policies
-- =====================================================
PRINT 'Step 5: Sample of remaining policies in PHA:';
PRINT '';

SELECT TOP 10
    pha.Id as PHA_Id,
    p.Id as PolicyId,
    p.GroupId,
    p.ProposalId,
    CASE WHEN p.GroupId = 'G00000' THEN 'DTC' 
         WHEN p.ProposalId IS NULL THEN 'No Proposal' 
         ELSE 'OTHER' END as Reason
FROM [dbo].[PolicyHierarchyAssignments] pha
INNER JOIN [dbo].[Policies] p ON p.Id = pha.PolicyId
ORDER BY pha.Id;

PRINT '';

-- =====================================================
-- Step 6: Summary
-- =====================================================
PRINT '=========================================================';
PRINT 'CLEANUP SUMMARY';
PRINT '=========================================================';
PRINT 'Before: ' + CAST(@totalPHA AS VARCHAR) + ' policies in PHA';
PRINT 'Deleted: ' + CAST(@deletedCount AS VARCHAR) + ' policies (had ProposalId, not DTC)';
PRINT 'Remaining: ' + CAST(@remainingTotal AS VARCHAR) + ' policies';
PRINT '';

IF @remainingShouldNotBeHere > 0
BEGIN
    PRINT '⚠️  WARNING: ' + CAST(@remainingShouldNotBeHere AS VARCHAR) + ' policies should not be in PHA!';
    PRINT '   These have ProposalId but are not DTC. Manual investigation needed.';
END
ELSE
BEGIN
    PRINT '✅ SUCCESS: All remaining policies are correctly in PHA';
    PRINT '   (DTC policies or policies without ProposalId)';
END

PRINT '';
PRINT 'Backup location: new_data.PolicyHierarchyAssignments_cleanup_backup';
PRINT '=========================================================';
