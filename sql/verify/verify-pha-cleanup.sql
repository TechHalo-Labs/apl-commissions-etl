/**
 * Verification: PolicyHierarchyAssignments Cleanup
 * ==================================================
 * Verifies that PHA contains only the correct policies and that
 * policy resolution is configured correctly.
 */

SET NOCOUNT ON;
PRINT '=========================================================';
PRINT 'PolicyHierarchyAssignments Cleanup Verification';
PRINT '=========================================================';
PRINT '';

-- =====================================================
-- 1. Production Policy Distribution
-- =====================================================
PRINT '1. PRODUCTION POLICY DISTRIBUTION';
PRINT '   (How policies should resolve to commissions)';
PRINT '---------------------------------------------------';

SELECT 
    CASE 
        WHEN p.GroupId = 'G00000' THEN '1. DTC (Direct-to-Consumer)'
        WHEN p.ProposalId IS NOT NULL THEN '2. Proposal Resolution'
        WHEN p.ProposalId IS NULL THEN '3. Unresolved (No Proposal)'
        ELSE '4. Unknown'
    END as ResolutionPath,
    COUNT(*) as PolicyCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) as Percentage
FROM [dbo].[Policies] p
GROUP BY 
    CASE 
        WHEN p.GroupId = 'G00000' THEN '1. DTC (Direct-to-Consumer)'
        WHEN p.ProposalId IS NOT NULL THEN '2. Proposal Resolution'
        WHEN p.ProposalId IS NULL THEN '3. Unresolved (No Proposal)'
        ELSE '4. Unknown'
    END
ORDER BY ResolutionPath;

PRINT '';

-- =====================================================
-- 2. Production PHA Status
-- =====================================================
PRINT '2. PRODUCTION PHA STATUS';
PRINT '---------------------------------------------------';

DECLARE @prod_pha_total INT = (SELECT COUNT(*) FROM [dbo].[PolicyHierarchyAssignments]);
DECLARE @prod_pha_with_hierarchy INT = (
    SELECT COUNT(*) FROM [dbo].[PolicyHierarchyAssignments] WHERE HierarchyId IS NOT NULL
);
DECLARE @prod_pha_without_hierarchy INT = (
    SELECT COUNT(*) FROM [dbo].[PolicyHierarchyAssignments] WHERE HierarchyId IS NULL
);

PRINT '   Total PHA records: ' + CAST(@prod_pha_total AS VARCHAR);
PRINT '   With HierarchyId: ' + CAST(@prod_pha_with_hierarchy AS VARCHAR);
PRINT '   Without HierarchyId: ' + CAST(@prod_pha_without_hierarchy AS VARCHAR);
PRINT '';

-- =====================================================
-- 3. Staging PHA Status
-- =====================================================
PRINT '3. STAGING PHA STATUS';
PRINT '---------------------------------------------------';

DECLARE @stg_pha_total INT = (SELECT COUNT(*) FROM [etl].[stg_policy_hierarchy_assignments]);
DECLARE @stg_pha_unique_policies INT = (SELECT COUNT(DISTINCT PolicyId) FROM [etl].[stg_policy_hierarchy_assignments]);
DECLARE @stg_pha_with_hierarchy INT = (
    SELECT COUNT(*) FROM [etl].[stg_policy_hierarchy_assignments] WHERE HierarchyId IS NOT NULL
);
DECLARE @stg_pha_without_hierarchy INT = (
    SELECT COUNT(*) FROM [etl].[stg_policy_hierarchy_assignments] WHERE HierarchyId IS NULL
);

PRINT '   Total PHA records: ' + CAST(@stg_pha_total AS VARCHAR);
PRINT '   Unique policies: ' + CAST(@stg_pha_unique_policies AS VARCHAR);
PRINT '   With HierarchyId (hierarchy-based): ' + CAST(@stg_pha_with_hierarchy AS VARCHAR);
PRINT '   Without HierarchyId (direct rates): ' + CAST(@stg_pha_without_hierarchy AS VARCHAR);
PRINT '';

-- =====================================================
-- 4. Staging PHA Policy Breakdown
-- =====================================================
PRINT '4. STAGING PHA POLICY BREAKDOWN';
PRINT '---------------------------------------------------';

SELECT 
    CASE 
        WHEN p.GroupId = 'G00000' THEN 'DTC (G00000)'
        WHEN p.ProposalId IS NULL THEN 'Unresolved (No Proposal)'
        WHEN p.ProposalId IS NOT NULL THEN 'ERROR: Has Proposal (should not be in PHA!)'
        ELSE 'Unknown'
    END as Category,
    COUNT(DISTINCT pha.PolicyId) as UniquePolicies,
    COUNT(*) as TotalRecords,
    COUNT(CASE WHEN pha.HierarchyId IS NOT NULL THEN 1 END) as WithHierarchyId,
    COUNT(CASE WHEN pha.HierarchyId IS NULL THEN 1 END) as WithoutHierarchyId
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId
GROUP BY 
    CASE 
        WHEN p.GroupId = 'G00000' THEN 'DTC (G00000)'
        WHEN p.ProposalId IS NULL THEN 'Unresolved (No Proposal)'
        WHEN p.ProposalId IS NOT NULL THEN 'ERROR: Has Proposal (should not be in PHA!)'
        ELSE 'Unknown'
    END
ORDER BY UniquePolicies DESC;

PRINT '';

-- =====================================================
-- 5. Data Quality Checks
-- =====================================================
PRINT '5. DATA QUALITY CHECKS';
PRINT '---------------------------------------------------';

-- Check 1: Policies in PHA that have ProposalId (should be 0)
DECLARE @pha_with_proposal INT = (
    SELECT COUNT(DISTINCT pha.PolicyId)
    FROM [etl].[stg_policy_hierarchy_assignments] pha
    INNER JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId
    WHERE p.ProposalId IS NOT NULL
      AND p.GroupId != 'G00000'  -- DTC is exception
);

IF @pha_with_proposal > 0
    PRINT '   ❌ ISSUE: ' + CAST(@pha_with_proposal AS VARCHAR) + ' policies in PHA have ProposalId (non-DTC)';
ELSE
    PRINT '   ✅ PASS: No non-DTC policies in PHA have ProposalId';

-- Check 2: Expected PHA size
DECLARE @expected_pha INT;
SELECT @expected_pha = COUNT(*)
FROM [dbo].[Policies]
WHERE GroupId = 'G00000'  -- DTC
   OR ProposalId IS NULL;  -- Unresolved

DECLARE @actual_stg_pha INT = (SELECT COUNT(DISTINCT PolicyId) FROM [etl].[stg_policy_hierarchy_assignments]);

IF @expected_pha = @actual_stg_pha
    PRINT '   ✅ PASS: Staging PHA size matches expected (' + CAST(@expected_pha AS VARCHAR) + ' policies)';
ELSE
    PRINT '   ⚠️  WARNING: Expected ' + CAST(@expected_pha AS VARCHAR) + ' policies, got ' + CAST(@actual_stg_pha AS VARCHAR);

-- Check 3: All PHA policies should have NULL HierarchyId (use direct rates)
IF @stg_pha_with_hierarchy > 0
    PRINT '   ⚠️  INFO: ' + CAST(@stg_pha_with_hierarchy AS VARCHAR) + ' PHA records have HierarchyId (hierarchy-based)';
ELSE
    PRINT '   ✅ PASS: All PHA records use direct rates (NULL HierarchyId)';

PRINT '';

-- =====================================================
-- 6. Sample PHA Policies
-- =====================================================
PRINT '6. SAMPLE PHA POLICIES (First 5)';
PRINT '---------------------------------------------------';

SELECT TOP 5
    p.Id as PolicyId,
    p.GroupId,
    p.ProposalId,
    pha.HierarchyId,
    pha.WritingBrokerId,
    pha.SplitPercent,
    pha.IsNonConforming,
    CASE 
        WHEN p.GroupId = 'G00000' THEN 'DTC'
        WHEN p.ProposalId IS NULL THEN 'Unresolved'
        ELSE 'ERROR'
    END as Reason
FROM [etl].[stg_policy_hierarchy_assignments] pha
LEFT JOIN [etl].[stg_policies] p ON p.Id = pha.PolicyId
ORDER BY pha.Id;

PRINT '';

-- =====================================================
-- 7. Summary
-- =====================================================
PRINT '=========================================================';
PRINT 'VERIFICATION SUMMARY';
PRINT '=========================================================';

DECLARE @total_policies INT = (SELECT COUNT(*) FROM [dbo].[Policies]);
DECLARE @dtc_count INT = (SELECT COUNT(*) FROM [dbo].[Policies] WHERE GroupId = 'G00000');
DECLARE @with_proposal INT = (SELECT COUNT(*) FROM [dbo].[Policies] WHERE ProposalId IS NOT NULL);
DECLARE @without_proposal INT = (SELECT COUNT(*) FROM [dbo].[Policies] WHERE ProposalId IS NULL);

PRINT 'Total Policies: ' + CAST(@total_policies AS VARCHAR);
PRINT '  - With ProposalId: ' + CAST(@with_proposal AS VARCHAR) + ' (' + 
      CAST(CAST(@with_proposal * 100.0 / @total_policies AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '  - Without ProposalId: ' + CAST(@without_proposal AS VARCHAR) + ' (' + 
      CAST(CAST(@without_proposal * 100.0 / @total_policies AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '  - DTC (GroupId = G00000): ' + CAST(@dtc_count AS VARCHAR) + ' (' + 
      CAST(CAST(@dtc_count * 100.0 / @total_policies AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '';
PRINT 'Expected in PHA: ' + CAST(@expected_pha AS VARCHAR) + ' (DTC + Unresolved)';
PRINT 'Actual in Staging PHA: ' + CAST(@actual_stg_pha AS VARCHAR);
PRINT 'Production PHA: ' + CAST(@prod_pha_total AS VARCHAR);
PRINT '';

IF @pha_with_proposal = 0 AND @expected_pha = @actual_stg_pha
BEGIN
    PRINT '✅ SUCCESS: PHA cleanup is complete and correct!';
    PRINT '   - No policies with ProposalId in PHA (except DTC)';
    PRINT '   - Staging PHA size matches expected';
    PRINT '   - All policies correctly routed';
END
ELSE
BEGIN
    PRINT '⚠️  ISSUES FOUND: Review data quality checks above';
END

PRINT '=========================================================';
