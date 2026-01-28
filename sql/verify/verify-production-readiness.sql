-- =============================================================================
-- Verify Production Readiness
-- Comprehensive validation of staging data before export
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PRODUCTION READINESS VERIFICATION';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Check 1: Status Fields
-- =============================================================================
PRINT 'CHECK 1: STATUS FIELDS';
PRINT '─────────────────────────────────────────────────────────';

-- Hierarchies should be Status=1 (Active)
DECLARE @inactive_hierarchies INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies] WHERE [Status] != 1);
PRINT 'Hierarchies with Status != 1 (Active): ' + CAST(@inactive_hierarchies AS VARCHAR);
IF @inactive_hierarchies > 0 PRINT '   ❌ FAIL: Hierarchies must be Active';
ELSE PRINT '   ✅ PASS';

-- Proposals should be Status=2 (Approved)
DECLARE @unapproved_proposals INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals] WHERE [Status] != 2);
PRINT 'Proposals with Status != 2 (Approved): ' + CAST(@unapproved_proposals AS VARCHAR);
IF @unapproved_proposals > 0 PRINT '   ❌ FAIL: Proposals must be Approved';
ELSE PRINT '   ✅ PASS';

-- PremiumSplitVersions should be Status=1 (Active)
DECLARE @inactive_splits INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] WHERE [Status] != 1);
PRINT 'PremiumSplitVersions with Status != 1 (Active): ' + CAST(@inactive_splits AS VARCHAR);
IF @inactive_splits > 0 PRINT '   ❌ FAIL: Split versions must be Active';
ELSE PRINT '   ✅ PASS';

-- HierarchyVersions should be Status=1 (Active)
DECLARE @inactive_hierarchy_versions INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] WHERE [Status] != 1);
PRINT 'HierarchyVersions with Status != 1 (Active): ' + CAST(@inactive_hierarchy_versions AS VARCHAR);
IF @inactive_hierarchy_versions > 0 PRINT '   ❌ FAIL: Hierarchy versions must be Active';
ELSE PRINT '   ✅ PASS';

PRINT '';

-- =============================================================================
-- Check 2: Required Foreign Keys Exist
-- =============================================================================
PRINT 'CHECK 2: FOREIGN KEY INTEGRITY';
PRINT '─────────────────────────────────────────────────────────';

-- All checks from Section 1 of audit script
DECLARE @fk_issues INT = 0;

-- PremiumSplitParticipants → Hierarchies
DECLARE @fk_split_hierarchy INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
    WHERE psp.HierarchyId IS NOT NULL AND psp.HierarchyId <> ''
      AND psp.HierarchyId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_hierarchies])
);
SET @fk_issues = @fk_issues + @fk_split_hierarchy;
PRINT 'PremiumSplitParticipants with invalid HierarchyId: ' + CAST(@fk_split_hierarchy AS VARCHAR);
IF @fk_split_hierarchy > 0 PRINT '   ❌ FAIL';
ELSE PRINT '   ✅ PASS';

-- HierarchyParticipants → Brokers
DECLARE @fk_participant_broker INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp
    WHERE hp.EntityId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_brokers])
);
SET @fk_issues = @fk_issues + @fk_participant_broker;
PRINT 'HierarchyParticipants with invalid EntityId (Broker): ' + CAST(@fk_participant_broker AS VARCHAR);
IF @fk_participant_broker > 0 PRINT '   ❌ FAIL';
ELSE PRINT '   ✅ PASS';

-- Proposals → Groups
DECLARE @fk_proposal_group INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals] p
    WHERE p.GroupId IS NOT NULL AND p.GroupId <> ''
      AND p.GroupId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_groups])
);
SET @fk_issues = @fk_issues + @fk_proposal_group;
PRINT 'Proposals with invalid GroupId: ' + CAST(@fk_proposal_group AS VARCHAR);
IF @fk_proposal_group > 0 PRINT '   ❌ FAIL';
ELSE PRINT '   ✅ PASS';

PRINT '';

-- =============================================================================
-- Check 3: Completeness
-- =============================================================================
PRINT 'CHECK 3: DATA COMPLETENESS';
PRINT '─────────────────────────────────────────────────────────';

-- Proposals must have splits
DECLARE @proposals_no_splits INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals] p
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
        WHERE psv.ProposalId = p.Id
    )
);
PRINT 'Proposals without PremiumSplitVersions: ' + CAST(@proposals_no_splits AS VARCHAR);
IF @proposals_no_splits > 0 PRINT '   ⚠️  WARNING: Cannot calculate commissions';
ELSE PRINT '   ✅ PASS';

-- Hierarchies must have versions
DECLARE @hierarchies_no_versions INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies] h
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv
        WHERE hv.HierarchyId = h.Id
    )
);
PRINT 'Hierarchies without HierarchyVersions: ' + CAST(@hierarchies_no_versions AS VARCHAR);
IF @hierarchies_no_versions > 0 PRINT '   ❌ FAIL';
ELSE PRINT '   ✅ PASS';

-- Hierarchies must have participants
DECLARE @hierarchies_no_participants INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies] h
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp
        JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
        WHERE hv.HierarchyId = h.Id
    )
);
PRINT 'Hierarchies without HierarchyParticipants: ' + CAST(@hierarchies_no_participants AS VARCHAR);
IF @hierarchies_no_participants > 0 PRINT '   ❌ FAIL';
ELSE PRINT '   ✅ PASS';

PRINT '';

-- =============================================================================
-- Check 4: Data Quality
-- =============================================================================
PRINT 'CHECK 4: DATA QUALITY';
PRINT '─────────────────────────────────────────────────────────';

-- Check generic group names
DECLARE @generic_names INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_groups]
    WHERE (Name LIKE 'Group G%' OR Name LIKE 'Group [0-9]%')
      AND Id <> 'G00000'
);
PRINT 'Groups with generic names: ' + CAST(@generic_names AS VARCHAR);
IF @generic_names > 100 PRINT '   ⚠️  WARNING: Many groups have generic names';
ELSE IF @generic_names > 0 PRINT '   ℹ️  INFO: Some groups have generic names';
ELSE PRINT '   ✅ PASS';

-- Check NULL BrokerUniquePartyId on Proposals
DECLARE @null_broker_id INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]
    WHERE BrokerUniquePartyId IS NULL OR BrokerUniquePartyId = ''
);
PRINT 'Proposals with NULL BrokerUniquePartyId: ' + CAST(@null_broker_id AS VARCHAR);
IF @null_broker_id > 1000 PRINT '   ⚠️  WARNING: Many proposals missing broker traceability';
ELSE IF @null_broker_id > 0 PRINT '   ℹ️  INFO: Some proposals missing broker ID';
ELSE PRINT '   ✅ PASS';

PRINT '';

-- =============================================================================
-- Final Assessment
-- =============================================================================
PRINT '============================================================';
PRINT 'FINAL ASSESSMENT';
PRINT '============================================================';
PRINT '';

DECLARE @total_issues INT = @blocking_issues + 
    CASE WHEN @inactive_hierarchies > 0 THEN 1 ELSE 0 END +
    CASE WHEN @unapproved_proposals > 0 THEN 1 ELSE 0 END +
    CASE WHEN @inactive_splits > 0 THEN 1 ELSE 0 END +
    CASE WHEN @proposals_no_splits > 0 THEN 1 ELSE 0 END +
    CASE WHEN @hierarchies_no_versions > 0 THEN 1 ELSE 0 END +
    CASE WHEN @hierarchies_no_participants > 0 THEN 1 ELSE 0 END;

IF @total_issues = 0
BEGIN
    PRINT '🎉 ALL CHECKS PASSED';
    PRINT '';
    PRINT 'Data is READY FOR EXPORT to production.';
END
ELSE
BEGIN
    PRINT '❌ FOUND ' + CAST(@total_issues AS VARCHAR) + ' CRITICAL ISSUES';
    PRINT '';
    PRINT 'DO NOT EXPORT until issues are resolved.';
    PRINT 'Review the checks above and fix failing items.';
END

PRINT '';
PRINT '============================================================';

GO
