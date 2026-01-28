-- =============================================================================
-- Verify Commission Runner Requirements
-- Checks all data requirements needed for commission calculation to succeed
-- Based on commission calculator code analysis
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COMMISSION RUNNER REQUIREMENTS VERIFICATION';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Test Case: Sample Policy Resolution
-- =============================================================================
PRINT 'TEST CASE: Sample Policy Resolution';
PRINT '─────────────────────────────────────────────────────────';

-- Pick a sample policy to test full resolution chain
DECLARE @test_policy_id NVARCHAR(100) = (
    SELECT TOP 1 Id 
    FROM [$(ETL_SCHEMA)].[stg_policies] 
    WHERE ProductCode IS NOT NULL 
      AND EffectiveDate IS NOT NULL
    ORDER BY Id
);

PRINT 'Testing with Policy: ' + ISNULL(@test_policy_id, 'NONE FOUND');

IF @test_policy_id IS NOT NULL
BEGIN
    -- Get policy details
    DECLARE @test_group_id NVARCHAR(100);
    DECLARE @test_product_code NVARCHAR(50);
    DECLARE @test_eff_date DATE;
    
    SELECT 
        @test_group_id = GroupId,
        @test_product_code = ProductCode,
        @test_eff_date = EffectiveDate
    FROM [$(ETL_SCHEMA)].[stg_policies]
    WHERE Id = @test_policy_id;
    
    PRINT '  GroupId: ' + ISNULL(@test_group_id, 'NULL');
    PRINT '  ProductCode: ' + ISNULL(@test_product_code, 'NULL');
    PRINT '  EffectiveDate: ' + ISNULL(CONVERT(VARCHAR, @test_eff_date, 120), 'NULL');
    PRINT '';
    
    -- Step 1: Can resolve to Proposal?
    DECLARE @resolved_proposal_id NVARCHAR(100) = (
        SELECT TOP 1 p.Id
        FROM [$(ETL_SCHEMA)].[stg_proposals] p
        WHERE p.GroupId = @test_group_id
          AND (@test_eff_date >= p.EffectiveDateFrom OR p.EffectiveDateFrom IS NULL)
          AND (@test_eff_date <= p.EffectiveDateTo OR p.EffectiveDateTo IS NULL)
          AND p.[Status] = 2  -- Approved
          AND p.IsDeleted = 0
        ORDER BY p.EffectiveDateFrom DESC
    );
    
    IF @resolved_proposal_id IS NOT NULL
        PRINT '  ✅ PASS: Resolved to Proposal ' + @resolved_proposal_id;
    ELSE
    BEGIN
        PRINT '  ❌ FAIL: No matching proposal found';
        GOTO TestEnd;
    END
    
    -- Step 2: Proposal has active split version?
    DECLARE @resolved_split_id NVARCHAR(100) = (
        SELECT TOP 1 psv.Id
        FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
        WHERE psv.ProposalId = @resolved_proposal_id
          AND @test_eff_date >= psv.EffectiveFrom
          AND (@test_eff_date <= psv.EffectiveTo OR psv.EffectiveTo IS NULL)
          AND psv.[Status] = 1  -- Active
          AND psv.IsDeleted = 0
        ORDER BY psv.EffectiveFrom DESC
    );
    
    IF @resolved_split_id IS NOT NULL
        PRINT '  ✅ PASS: Found PremiumSplitVersion ' + @resolved_split_id;
    ELSE
    BEGIN
        PRINT '  ❌ FAIL: No active split version for proposal';
        GOTO TestEnd;
    END
    
    -- Step 3: Split has participants with hierarchies?
    DECLARE @participant_count INT = (
        SELECT COUNT(*)
        FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
        WHERE psp.VersionId = @resolved_split_id
          AND psp.HierarchyId IS NOT NULL
          AND psp.HierarchyId <> ''
    );
    
    IF @participant_count > 0
        PRINT '  ✅ PASS: Split has ' + CAST(@participant_count AS VARCHAR) + ' participant(s) with hierarchies';
    ELSE
    BEGIN
        PRINT '  ❌ FAIL: Split has no participants with hierarchies';
        GOTO TestEnd;
    END
    
    -- Step 4: Hierarchies have active versions?
    DECLARE @hierarchy_version_count INT = (
        SELECT COUNT(DISTINCT hv.Id)
        FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
        JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv ON hv.HierarchyId = psp.HierarchyId
        WHERE psp.VersionId = @resolved_split_id
          AND @test_eff_date >= hv.EffectiveFrom
          AND (@test_eff_date <= hv.EffectiveTo OR hv.EffectiveTo IS NULL)
          AND hv.[Status] = 1  -- Active
          AND hv.IsDeleted = 0
    );
    
    IF @hierarchy_version_count > 0
        PRINT '  ✅ PASS: Found ' + CAST(@hierarchy_version_count AS VARCHAR) + ' active hierarchy version(s)';
    ELSE
    BEGIN
        PRINT '  ❌ FAIL: No active hierarchy versions found';
        GOTO TestEnd;
    END
    
    PRINT '  ✅ COMPLETE: Full resolution chain works!';
END

TestEnd:
PRINT '';

-- =============================================================================
-- Check 5: Critical Data Presence
-- =============================================================================
PRINT 'CHECK 5: CRITICAL DATA PRESENCE';
PRINT '─────────────────────────────────────────────────────────';

-- Check all critical tables have data
DECLARE @missing_data INT = 0;

DECLARE @brokers_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_brokers]);
PRINT 'Brokers: ' + CAST(@brokers_count AS VARCHAR);
IF @brokers_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No brokers'; END

DECLARE @groups_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_groups]);
PRINT 'Groups: ' + CAST(@groups_count AS VARCHAR);
IF @groups_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No groups'; END

DECLARE @proposals_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]);
PRINT 'Proposals: ' + CAST(@proposals_count AS VARCHAR);
IF @proposals_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No proposals'; END

DECLARE @hierarchies_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies]);
PRINT 'Hierarchies: ' + CAST(@hierarchies_count AS VARCHAR);
IF @hierarchies_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No hierarchies'; END

DECLARE @schedules_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_schedules]);
PRINT 'Schedules: ' + CAST(@schedules_count AS VARCHAR);
IF @schedules_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No schedules'; END

DECLARE @schedule_rates_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_schedule_rates]);
PRINT 'ScheduleRates: ' + CAST(@schedule_rates_count AS VARCHAR);
IF @schedule_rates_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No schedule rates'; END

DECLARE @policies_count INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policies]);
PRINT 'Policies: ' + CAST(@policies_count AS VARCHAR);
IF @policies_count = 0 BEGIN SET @missing_data = @missing_data + 1; PRINT '   ❌ FAIL: No policies'; END

PRINT '';

-- =============================================================================
-- Final Verdict
-- =============================================================================
PRINT '============================================================';
PRINT 'FINAL VERDICT';
PRINT '============================================================';
PRINT '';

DECLARE @total_failures INT = @total_issues + @missing_data +
    CASE WHEN @inactive_hierarchies > 0 THEN 1 ELSE 0 END +
    CASE WHEN @unapproved_proposals > 0 THEN 1 ELSE 0 END +
    CASE WHEN @inactive_splits > 0 THEN 1 ELSE 0 END +
    CASE WHEN @proposals_no_splits > 0 THEN 1 ELSE 0 END +
    CASE WHEN @hierarchies_no_versions > 0 THEN 1 ELSE 0 END +
    CASE WHEN @hierarchies_no_participants > 0 THEN 1 ELSE 0 END;

IF @total_failures = 0
BEGIN
    PRINT '✅ READY FOR PRODUCTION';
    PRINT '';
    PRINT 'All commission runner requirements are met.';
    PRINT 'Data can be safely exported to production.';
    PRINT '';
    PRINT 'Next steps:';
    PRINT '  1. Run export scripts';
    PRINT '  2. Test commission calculation with sample policies';
    PRINT '  3. Verify commission results match expectations';
END
ELSE
BEGIN
    PRINT '❌ NOT READY FOR PRODUCTION';
    PRINT '';
    PRINT 'Critical issues must be resolved before export.';
    PRINT '';
    PRINT 'Required fixes:';
    IF @inactive_hierarchies > 0 PRINT '  - Set Hierarchies Status=1 (Active)';
    IF @unapproved_proposals > 0 PRINT '  - Set Proposals Status=2 (Approved)';
    IF @inactive_splits > 0 PRINT '  - Set PremiumSplitVersions Status=1 (Active)';
    IF @proposals_no_splits > 0 PRINT '  - Create missing PremiumSplitVersions';
    IF @hierarchies_no_versions > 0 PRINT '  - Create missing HierarchyVersions';
    IF @hierarchies_no_participants > 0 PRINT '  - Create missing HierarchyParticipants';
    IF @missing_data > 0 PRINT '  - Populate missing core entities';
END

PRINT '';
PRINT '============================================================';

GO
