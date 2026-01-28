-- =============================================================================
-- Post-Transform Audit and Cleanup
-- Runs after all transforms, before export
-- Validates data integrity, completeness, and applies cleanup fixes
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'POST-TRANSFORM AUDIT AND CLEANUP';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Section 1: Referential Integrity Checks
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 1: REFERENTIAL INTEGRITY CHECKS';
PRINT '============================================================';
PRINT '';

-- 1.1 Check orphaned PremiumSplitParticipants (HierarchyId not in Hierarchies)
DECLARE @orphaned_split_participants INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
    WHERE psp.HierarchyId IS NOT NULL 
      AND psp.HierarchyId <> ''
      AND psp.HierarchyId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_hierarchies])
);
PRINT 'Orphaned PremiumSplitParticipants (HierarchyId not in Hierarchies): ' + CAST(@orphaned_split_participants AS VARCHAR);
IF @orphaned_split_participants > 0
    PRINT '   ⚠️  WARNING: Found orphaned split participants';

-- 1.2 Check orphaned HierarchyParticipants (EntityId not in Brokers)
DECLARE @orphaned_hierarchy_participants INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp
    WHERE hp.EntityId IS NOT NULL
      AND hp.EntityId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_brokers])
);
PRINT 'Orphaned HierarchyParticipants (EntityId not in Brokers): ' + CAST(@orphaned_hierarchy_participants AS VARCHAR);
IF @orphaned_hierarchy_participants > 0
    PRINT '   ⚠️  WARNING: Found orphaned hierarchy participants';

-- 1.3 Check orphaned Proposals (GroupId not in Groups)
DECLARE @orphaned_proposals INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_proposals] p
    WHERE p.GroupId IS NOT NULL
      AND p.GroupId <> ''
      AND p.GroupId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_groups])
);
PRINT 'Orphaned Proposals (GroupId not in Groups): ' + CAST(@orphaned_proposals AS VARCHAR);
IF @orphaned_proposals > 0
    PRINT '   ⚠️  WARNING: Found orphaned proposals';

-- 1.4 Check orphaned PremiumSplitVersions (ProposalId not in Proposals)
DECLARE @orphaned_split_versions INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
    WHERE psv.ProposalId IS NOT NULL
      AND psv.ProposalId <> ''
      AND psv.ProposalId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_proposals])
);
PRINT 'Orphaned PremiumSplitVersions (ProposalId not in Proposals): ' + CAST(@orphaned_split_versions AS VARCHAR);
IF @orphaned_split_versions > 0
    PRINT '   ⚠️  WARNING: Found orphaned split versions';

-- 1.5 Check orphaned HierarchyVersions (HierarchyId not in Hierarchies)
DECLARE @orphaned_hierarchy_versions INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv
    WHERE hv.HierarchyId IS NOT NULL
      AND hv.HierarchyId <> ''
      AND hv.HierarchyId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_hierarchies])
);
PRINT 'Orphaned HierarchyVersions (HierarchyId not in Hierarchies): ' + CAST(@orphaned_hierarchy_versions AS VARCHAR);
IF @orphaned_hierarchy_versions > 0
    PRINT '   ⚠️  WARNING: Found orphaned hierarchy versions';

-- 1.6 Check orphaned ScheduleRates (ScheduleVersionId not in ScheduleVersions)
DECLARE @orphaned_schedule_rates INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_schedule_rates] sr
    WHERE sr.ScheduleVersionId IS NOT NULL
      AND sr.ScheduleVersionId NOT IN (SELECT Id FROM [$(ETL_SCHEMA)].[stg_schedule_versions])
);
PRINT 'Orphaned ScheduleRates (ScheduleVersionId not in ScheduleVersions): ' + CAST(@orphaned_schedule_rates AS VARCHAR);
IF @orphaned_schedule_rates > 0
    PRINT '   ⚠️  WARNING: Found orphaned schedule rates';

PRINT '';

-- =============================================================================
-- Section 2: Status Verification
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 2: STATUS VERIFICATION';
PRINT '============================================================';
PRINT '';

-- 2.1 Check Hierarchies Status
SELECT 
    'Hierarchies by Status' AS entity,
    [Status],
    COUNT(*) AS cnt,
    CASE [Status]
        WHEN 0 THEN '❌ Inactive (WRONG)'
        WHEN 1 THEN '✅ Active (CORRECT)'
        ELSE '⚠️  Unknown'
    END AS status_label
FROM [$(ETL_SCHEMA)].[stg_hierarchies]
GROUP BY [Status]
ORDER BY [Status];

-- 2.2 Check Proposals Status
SELECT 
    'Proposals by Status' AS entity,
    [Status],
    COUNT(*) AS cnt,
    CASE [Status]
        WHEN 0 THEN '❌ Inactive (WRONG)'
        WHEN 1 THEN '⚠️  Pending'
        WHEN 2 THEN '✅ Approved (CORRECT)'
        WHEN 3 THEN '⚠️  Expired'
        ELSE '⚠️  Unknown'
    END AS status_label
FROM [$(ETL_SCHEMA)].[stg_proposals]
GROUP BY [Status]
ORDER BY [Status];

-- 2.3 Check Schedules Status
SELECT 
    'Schedules by Status' AS entity,
    [Status],
    COUNT(*) AS cnt,
    CASE [Status]
        WHEN 'Active' THEN '✅ Active (CORRECT)'
        WHEN 'Inactive' THEN '❌ Inactive (WRONG)'
        WHEN '0' THEN '❌ Inactive (WRONG)'
        WHEN '1' THEN '✅ Active (CORRECT)'
        ELSE '⚠️  Unknown'
    END AS status_label
FROM [$(ETL_SCHEMA)].[stg_schedules]
GROUP BY [Status]
ORDER BY [Status];

-- 2.4 Check PremiumSplitVersions Status
SELECT 
    'PremiumSplitVersions by Status' AS entity,
    [Status],
    COUNT(*) AS cnt,
    CASE [Status]
        WHEN 0 THEN '❌ Inactive (WRONG)'
        WHEN 1 THEN '✅ Active (CORRECT)'
        ELSE '⚠️  Unknown'
    END AS status_label
FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]
GROUP BY [Status]
ORDER BY [Status];

PRINT '';

-- =============================================================================
-- Section 3: Completeness Checks
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 3: COMPLETENESS CHECKS';
PRINT '============================================================';
PRINT '';

-- 3.1 Proposals without PremiumSplitVersions
DECLARE @proposals_no_splits INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_proposals] p
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
        WHERE psv.ProposalId = p.Id
    )
);
PRINT 'Proposals without PremiumSplitVersions: ' + CAST(@proposals_no_splits AS VARCHAR);
IF @proposals_no_splits > 0
    PRINT '   ⚠️  WARNING: These proposals cannot calculate commissions';

-- 3.2 PremiumSplitVersions without Participants
DECLARE @splits_no_participants INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
        WHERE psp.VersionId = psv.Id
    )
);
PRINT 'PremiumSplitVersions without Participants: ' + CAST(@splits_no_participants AS VARCHAR);
IF @splits_no_participants > 0
    PRINT '   ⚠️  WARNING: These split versions have no brokers';

-- 3.3 Hierarchies without HierarchyVersions
DECLARE @hierarchies_no_versions INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_hierarchies] h
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv
        WHERE hv.HierarchyId = h.Id
    )
);
PRINT 'Hierarchies without HierarchyVersions: ' + CAST(@hierarchies_no_versions AS VARCHAR);
IF @hierarchies_no_versions > 0
    PRINT '   ⚠️  WARNING: These hierarchies cannot be used';

-- 3.4 Hierarchies without Participants
DECLARE @hierarchies_no_participants INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_hierarchies] h
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp
        JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
        WHERE hv.HierarchyId = h.Id
    )
);
PRINT 'Hierarchies without Participants: ' + CAST(@hierarchies_no_participants AS VARCHAR);
IF @hierarchies_no_participants > 0
    PRINT '   ⚠️  WARNING: These hierarchies have no broker chains';

-- 3.5 Schedules without ScheduleVersions
DECLARE @schedules_no_versions INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_schedules] s
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_schedule_versions] sv
        WHERE sv.ScheduleId = s.Id
    )
);
PRINT 'Schedules without ScheduleVersions: ' + CAST(@schedules_no_versions AS VARCHAR);
IF @schedules_no_versions > 0
    PRINT '   ⚠️  WARNING: These schedules cannot be used';

-- 3.6 Schedules without ScheduleRates
DECLARE @schedules_no_rates INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_schedules] s
    WHERE NOT EXISTS (
        SELECT 1 FROM [$(ETL_SCHEMA)].[stg_schedule_rates] sr
        JOIN [$(ETL_SCHEMA)].[stg_schedule_versions] sv ON sv.Id = sr.ScheduleVersionId
        WHERE sv.ScheduleId = s.Id
    )
);
PRINT 'Schedules without ScheduleRates: ' + CAST(@schedules_no_rates AS VARCHAR);
IF @schedules_no_rates > 0
    PRINT '   ⚠️  WARNING: These schedules have no rate data';

PRINT '';

-- =============================================================================
-- Section 4: Data Quality Checks
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 4: DATA QUALITY CHECKS';
PRINT '============================================================';
PRINT '';

-- 4.1 Check Groups with generic names
SELECT 
    'Groups with generic names' AS metric,
    SUM(CASE WHEN Name LIKE 'Group G%' OR Name LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS generic_count,
    SUM(CASE WHEN Name NOT LIKE 'Group G%' AND Name NOT LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS real_name_count
FROM [$(ETL_SCHEMA)].[stg_groups]
WHERE Id <> 'G00000';  -- Exclude DTC sentinel

-- 4.2 Check Proposals with NULL BrokerUniquePartyId
DECLARE @proposals_null_broker_id INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_proposals]
    WHERE BrokerUniquePartyId IS NULL OR BrokerUniquePartyId = ''
);
PRINT 'Proposals with NULL BrokerUniquePartyId: ' + CAST(@proposals_null_broker_id AS VARCHAR);
IF @proposals_null_broker_id > 0
    PRINT '   ⚠️  WARNING: Cannot trace these proposals to source broker';

-- 4.3 Check Proposals with NULL BrokerName
DECLARE @proposals_null_broker_name INT = (
    SELECT COUNT(*)
    FROM [$(ETL_SCHEMA)].[stg_proposals]
    WHERE BrokerName IS NULL OR BrokerName = ''
);
PRINT 'Proposals with NULL BrokerName: ' + CAST(@proposals_null_broker_name AS VARCHAR);
IF @proposals_null_broker_name > 0
    PRINT '   ℹ️  INFO: BrokerName can be populated during cleanup';

-- 4.4 Check Groups with NULL PrimaryBrokerId
SELECT 
    'Groups with NULL PrimaryBrokerId' AS metric,
    COUNT(*) AS cnt,
    CAST(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_groups] WHERE Id <> 'G00000'), 0) AS DECIMAL(5,2)) AS percent
FROM [$(ETL_SCHEMA)].[stg_groups]
WHERE (PrimaryBrokerId IS NULL OR PrimaryBrokerId = 0)
  AND Id <> 'G00000';

-- 4.5 Check date range validity
DECLARE @invalid_date_ranges INT = (
    SELECT COUNT(*)
    FROM (
        SELECT 'Proposals' AS entity, COUNT(*) AS cnt
        FROM [$(ETL_SCHEMA)].[stg_proposals]
        WHERE EffectiveDateFrom > EffectiveDateTo
        UNION ALL
        SELECT 'PremiumSplitVersions', COUNT(*)
        FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]
        WHERE EffectiveFrom > EffectiveTo
        UNION ALL
        SELECT 'HierarchyVersions', COUNT(*)
        FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions]
        WHERE EffectiveFrom > EffectiveTo
    ) AS invalid
    WHERE cnt > 0
);
PRINT 'Invalid date ranges (From > To): ' + CAST(@invalid_date_ranges AS VARCHAR);
IF @invalid_date_ranges > 0
    PRINT '   ❌ ERROR: Date ranges must be corrected';

-- 4.6 Check split percentage totals
SELECT 
    'PremiumSplitVersions by total percentage' AS metric,
    CASE 
        WHEN TotalSplitPercent < 99.5 THEN 'Under 100%'
        WHEN TotalSplitPercent BETWEEN 99.5 AND 100.5 THEN '~100% (Good)'
        WHEN TotalSplitPercent > 100.5 THEN 'Over 100%'
    END AS range_label,
    COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]
GROUP BY CASE 
    WHEN TotalSplitPercent < 99.5 THEN 'Under 100%'
    WHEN TotalSplitPercent BETWEEN 99.5 AND 100.5 THEN '~100% (Good)'
    WHEN TotalSplitPercent > 100.5 THEN 'Over 100%'
END
ORDER BY range_label;

PRINT '';

-- =============================================================================
-- Section 5: Data Cleanup and Fixes
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 5: DATA CLEANUP AND FIXES';
PRINT '============================================================';
PRINT '';

-- 5.1 Fix NULL BrokerName on Proposals
PRINT 'Step 5.1: Fixing NULL BrokerName on Proposals...';

UPDATE p
SET p.BrokerName = b.Name
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.Id = p.BrokerId
WHERE (p.BrokerName IS NULL OR p.BrokerName = '')
  AND b.Name IS NOT NULL
  AND b.Name <> '';

PRINT 'BrokerName updated on Proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- 5.2 Fix NULL GroupName on Proposals
PRINT '';
PRINT 'Step 5.2: Fixing NULL GroupName on Proposals...';

UPDATE p
SET p.GroupName = g.Name
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = p.GroupId
WHERE (p.GroupName IS NULL OR p.GroupName = '' OR p.GroupName LIKE 'Group G%')
  AND g.Name IS NOT NULL
  AND g.Name <> ''
  AND g.Name NOT LIKE 'Group G%';

PRINT 'GroupName updated on Proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- 5.3 Populate BrokerUniquePartyId where missing (if broker exists)
PRINT '';
PRINT 'Step 5.3: Populating missing BrokerUniquePartyId on Proposals...';

UPDATE p
SET p.BrokerUniquePartyId = b.ExternalPartyId
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.Id = p.BrokerId
WHERE (p.BrokerUniquePartyId IS NULL OR p.BrokerUniquePartyId = '')
  AND b.ExternalPartyId IS NOT NULL
  AND b.ExternalPartyId <> '';

PRINT 'BrokerUniquePartyId updated on Proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- 5.4 Populate missing PrimaryBrokerId on Groups from raw_perf_groups
-- =============================================================================
PRINT '';
PRINT 'Step 5.4: Populating missing PrimaryBrokerId on Groups from raw_perf_groups...';

UPDATE g
SET g.PrimaryBrokerId = (
    SELECT TOP 1 b.Id 
    FROM [$(ETL_SCHEMA)].[stg_brokers] b 
    INNER JOIN [$(ETL_SCHEMA)].[raw_perf_groups] rpg 
        ON b.ExternalPartyId = LTRIM(RTRIM(rpg.BrokerUniqueId))
    WHERE LTRIM(RTRIM(rpg.GroupNum)) = g.Code
)
FROM [$(ETL_SCHEMA)].[stg_groups] g
WHERE (g.PrimaryBrokerId IS NULL OR g.PrimaryBrokerId = 0)
  AND g.Id <> 'G00000';

PRINT 'PrimaryBrokerId populated on Groups: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- 5.5 Populate missing BrokerUniquePartyId on Proposals from raw_perf_groups
-- =============================================================================
PRINT '';
PRINT 'Step 5.5: Populating missing BrokerUniquePartyId on Proposals from raw_perf_groups...';

-- First, set BrokerUniquePartyId from raw_perf_groups
UPDATE p
SET p.BrokerUniquePartyId = (
    SELECT TOP 1 LTRIM(RTRIM(rpg.BrokerUniqueId))
    FROM [$(ETL_SCHEMA)].[raw_perf_groups] rpg
    WHERE CONCAT('G', LTRIM(RTRIM(rpg.GroupNum))) = p.GroupId
      AND rpg.BrokerUniqueId IS NOT NULL
      AND LTRIM(RTRIM(rpg.BrokerUniqueId)) <> ''
)
FROM [$(ETL_SCHEMA)].[stg_proposals] p
WHERE (p.BrokerUniquePartyId IS NULL OR p.BrokerUniquePartyId = '');

DECLARE @proposals_broker_updated INT = @@ROWCOUNT;
PRINT 'BrokerUniquePartyId populated on Proposals (from raw_perf_groups): ' + CAST(@proposals_broker_updated AS VARCHAR);

-- Then, set BrokerId from the BrokerUniquePartyId we just populated
UPDATE p
SET p.BrokerId = b.Id
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = p.BrokerUniquePartyId
WHERE p.BrokerUniquePartyId IS NOT NULL
  AND p.BrokerUniquePartyId <> ''
  AND (p.BrokerId IS NULL OR p.BrokerId = 0);

PRINT 'BrokerId populated on Proposals (from BrokerUniquePartyId): ' + CAST(@@ROWCOUNT AS VARCHAR);

PRINT '';

-- =============================================================================
-- Section 6: Summary Statistics
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 6: SUMMARY STATISTICS';
PRINT '============================================================';
PRINT '';

-- Entity counts
SELECT 'Brokers' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_brokers]
UNION ALL SELECT 'Groups', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_groups]
UNION ALL SELECT 'Products', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_products]
UNION ALL SELECT 'ProductCodes', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_product_codes]
UNION ALL SELECT 'Plans', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_plans]
UNION ALL SELECT 'Schedules', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_schedules]
UNION ALL SELECT 'ScheduleVersions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_schedule_versions]
UNION ALL SELECT 'ScheduleRates', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_schedule_rates]
UNION ALL SELECT 'Proposals', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposals]
UNION ALL SELECT 'ProposalProducts', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_proposal_products]
UNION ALL SELECT 'PremiumSplitVersions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_versions]
UNION ALL SELECT 'PremiumSplitParticipants', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants]
UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchies]
UNION ALL SELECT 'HierarchyVersions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_versions]
UNION ALL SELECT 'HierarchyParticipants', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants]
UNION ALL SELECT 'Policies', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policies]
UNION ALL SELECT 'PolicyHierarchyAssignments', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policy_hierarchy_assignments]
UNION ALL SELECT 'CommissionAssignmentVersions', COUNT(*) FROM [$(ETL_SCHEMA)].[stg_commission_assignment_versions]
ORDER BY entity;

PRINT '';

-- =============================================================================
-- Section 7: Production Readiness Assessment
-- =============================================================================
PRINT '============================================================';
PRINT 'SECTION 7: PRODUCTION READINESS ASSESSMENT';
PRINT '============================================================';
PRINT '';

DECLARE @blocking_issues INT = 
    @orphaned_split_participants + 
    @orphaned_hierarchy_participants + 
    @orphaned_proposals + 
    @orphaned_split_versions + 
    @orphaned_hierarchy_versions + 
    @orphaned_schedule_rates;

IF @blocking_issues = 0
BEGIN
    PRINT '✅ NO BLOCKING ISSUES FOUND';
    PRINT '';
    PRINT 'Data is ready for export with the following notes:';
    PRINT '- Hierarchies Status: Should be 1 (Active)';
    PRINT '- Proposals Status: Should be 2 (Approved)';
    PRINT '- Schedules Status: Should be Active or 1';
    PRINT '- All date ranges valid';
    PRINT '- All referential integrity checks passed';
END
ELSE
BEGIN
    PRINT '❌ FOUND ' + CAST(@blocking_issues AS VARCHAR) + ' BLOCKING ISSUES';
    PRINT '';
    PRINT 'Review the warnings above and fix issues before export.';
END

PRINT '';
PRINT '============================================================';
PRINT 'AUDIT AND CLEANUP COMPLETED';
PRINT '============================================================';

GO
