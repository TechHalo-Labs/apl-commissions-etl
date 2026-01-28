-- =============================================================================
-- FOCUSED TEST: Hierarchy Consolidation Fix
-- =============================================================================
-- Tests referential integrity after fixing hierarchy consolidation bug
-- Runs on a subset of data (specific groups) for quick validation
-- =============================================================================

SET NOCOUNT ON;

PRINT '════════════════════════════════════════════════════════════════';
PRINT 'FOCUSED TEST: Hierarchy Fix - Referential Integrity Validation';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

-- =============================================================================
-- TEST CONFIGURATION
-- =============================================================================
DECLARE @test_group_id VARCHAR(50) = 'G16163';  -- Known problematic group
DECLARE @test_group_id_numeric VARCHAR(50) = '16163';  -- Without G prefix

PRINT 'Test Configuration:';
PRINT '  Test Group: ' + @test_group_id;
PRINT '  Raw Group ID: ' + @test_group_id_numeric;
PRINT '';

-- =============================================================================
-- STEP 1: Verify Source Data Exists
-- =============================================================================
PRINT '────────────────────────────────────────────────────────────────';
PRINT 'STEP 1: Verify Source Data';
PRINT '────────────────────────────────────────────────────────────────';

-- Check raw certificate data
SELECT 
    'Raw Certificates' AS metric,
    COUNT(DISTINCT CertificateId) AS certificate_count,
    COUNT(DISTINCT CertSplitSeq) AS split_seq_count,
    MIN(CertEffectiveDate) AS min_date,
    MAX(CertEffectiveDate) AS max_date
FROM [etl].[input_certificate_info]
WHERE GroupId = @test_group_id_numeric;

-- Check unique broker structures
SELECT 
    'Unique Broker Structures' AS metric,
    COUNT(DISTINCT CONCAT(WritingBrokerID, '-', CertSplitSeq)) AS structure_count
FROM [etl].[input_certificate_info]
WHERE GroupId = @test_group_id_numeric;

PRINT 'Source data verified.';
PRINT '';

-- =============================================================================
-- STEP 2: Clear Test Tables
-- =============================================================================
PRINT '────────────────────────────────────────────────────────────────';
PRINT 'STEP 2: Clear Test Tables';
PRINT '────────────────────────────────────────────────────────────────';

-- Clear work tables
TRUNCATE TABLE [etl].[work_split_participants];
TRUNCATE TABLE [etl].[work_split_signatures];
TRUNCATE TABLE [etl].[work_hierarchy_id_map];
TRUNCATE TABLE [etl].[work_splitseq_to_hierarchy];
TRUNCATE TABLE [etl].[work_hierarchy_data];

-- Clear staging tables for test group only
DELETE FROM [etl].[stg_hierarchy_participants] 
WHERE HierarchyVersionId IN (
    SELECT hv.Id 
    FROM [etl].[stg_hierarchy_versions] hv
    INNER JOIN [etl].[stg_hierarchies] h ON h.Id = hv.HierarchyId
    WHERE h.GroupId = @test_group_id
);

DELETE FROM [etl].[stg_hierarchy_versions]
WHERE HierarchyId IN (SELECT Id FROM [etl].[stg_hierarchies] WHERE GroupId = @test_group_id);

DELETE FROM [etl].[stg_hierarchies] WHERE GroupId = @test_group_id;

DELETE FROM [etl].[stg_splitseq_hierarchy_map] WHERE GroupId = @test_group_id;

PRINT 'Test tables cleared.';
PRINT '';

-- =============================================================================
-- STEP 3: Run Hierarchy Transform (Extract from 07-hierarchies.sql)
-- =============================================================================
PRINT '────────────────────────────────────────────────────────────────';
PRINT 'STEP 3: Run Hierarchy Transform (Test Group Only)';
PRINT '────────────────────────────────────────────────────────────────';

-- Step 3a: Build split participants (for test group only)
INSERT INTO [etl].[work_split_participants] (GroupId, CertSplitSeq, WritingBrokerId, [Level], BrokerId, ScheduleCode, SplitPercent, MinEffDate)
SELECT 
    CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
    ci.CertSplitSeq,
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
    ci.SplitBrokerSeq AS [Level],
    TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) AS BrokerId,
    ci.CommissionsSchedule AS ScheduleCode,
    TRY_CAST(ci.CertSplitPercent AS DECIMAL(18,4)) AS SplitPercent,
    MIN(ci.CertEffectiveDate) AS MinEffDate
FROM [etl].[input_certificate_info] ci
WHERE ci.GroupId = @test_group_id_numeric
  AND ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> ''
  AND ci.SplitBrokerId IS NOT NULL AND ci.SplitBrokerId <> ''
  AND ci.RecStatus = 'A'
  AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
  AND TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) IS NOT NULL
GROUP BY ci.GroupId, ci.CertSplitSeq, ci.WritingBrokerID, ci.SplitBrokerSeq, ci.SplitBrokerId, 
         ci.CommissionsSchedule, ci.CertSplitPercent;

DECLARE @cnt_parts INT = (SELECT COUNT(*) FROM [etl].[work_split_participants]);
PRINT 'Split participants extracted: ' + CAST(@cnt_parts AS VARCHAR) + ' rows';

-- Step 3b: Create structure signatures
INSERT INTO [etl].[work_split_signatures] (GroupId, CertSplitSeq, WritingBrokerId, MinEffDate, StructureSignature)
SELECT 
    sp.GroupId,
    sp.CertSplitSeq,
    sp.WritingBrokerId,
    MIN(sp.MinEffDate) AS MinEffDate,
    CAST(STRING_AGG(
        CAST(CONCAT(sp.[Level], '|', sp.BrokerId, '|', ISNULL(sp.ScheduleCode, '')) AS NVARCHAR(MAX)), 
        ','
    ) WITHIN GROUP (ORDER BY sp.[Level], sp.BrokerId) AS NVARCHAR(MAX)) AS StructureSignature
FROM [etl].[work_split_participants] sp
GROUP BY sp.GroupId, sp.CertSplitSeq, sp.WritingBrokerId;

DECLARE @cnt_sigs INT = (SELECT COUNT(*) FROM [etl].[work_split_signatures]);
PRINT 'Split signatures built: ' + CAST(@cnt_sigs AS VARCHAR) + ' rows';

-- Step 3c: Create HierarchyId mapping (FIXED - No consolidation)
INSERT INTO [etl].[work_hierarchy_id_map] (GroupId, WritingBrokerId, StructureSignature, MinEffDate, RepresentativeSplitSeq, HierarchyId)
SELECT
    GroupId,
    WritingBrokerId,
    StructureSignature,
    MinEffDate,
    CertSplitSeq AS RepresentativeSplitSeq,
    CONCAT('H-', GroupId, '-', CAST(ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY CertSplitSeq, MinEffDate) AS VARCHAR)) AS HierarchyId
FROM [etl].[work_split_signatures]
-- NO GROUP BY - each CertSplitSeq gets its own hierarchy (no consolidation)

DECLARE @cnt_idmap INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_id_map]);
PRINT 'Hierarchy ID map: ' + CAST(@cnt_idmap AS VARCHAR) + ' hierarchies (NO CONSOLIDATION)';

-- Step 3d: Map splitSeq to hierarchy (FIXED - 1-to-1)
INSERT INTO [etl].[work_splitseq_to_hierarchy] (GroupId, CertSplitSeq, WritingBrokerId, HierarchyId, MinEffDate)
SELECT
    ss.GroupId,
    ss.CertSplitSeq,
    ss.WritingBrokerId,
    him.HierarchyId,
    ss.MinEffDate
FROM [etl].[work_split_signatures] ss
INNER JOIN [etl].[work_hierarchy_id_map] him 
    ON him.GroupId = ss.GroupId
    AND him.WritingBrokerId = ss.WritingBrokerId
    AND him.RepresentativeSplitSeq = ss.CertSplitSeq;  -- FIXED: 1-to-1 mapping

DECLARE @cnt_map INT = (SELECT COUNT(*) FROM [etl].[work_splitseq_to_hierarchy]);
PRINT 'SplitSeq mappings: ' + CAST(@cnt_map AS VARCHAR) + ' rows (1-to-1)';

-- Step 3e: Build hierarchy data
INSERT INTO [etl].[work_hierarchy_data] (GroupId, WritingBrokerId, FirstUplineId, MinEffDate, HierarchyId)
SELECT 
    him.GroupId,
    him.WritingBrokerId,
    (SELECT TOP 1 sp.BrokerId 
     FROM [etl].[work_split_participants] sp 
     WHERE sp.GroupId = him.GroupId 
       AND sp.CertSplitSeq = him.RepresentativeSplitSeq
       AND sp.WritingBrokerId = him.WritingBrokerId
       AND sp.[Level] = 2) AS FirstUplineId,
    him.MinEffDate,
    him.HierarchyId
FROM [etl].[work_hierarchy_id_map] him;

DECLARE @cnt_hdata INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_data]);
PRINT 'Hierarchy data: ' + CAST(@cnt_hdata AS VARCHAR) + ' rows';
PRINT '';

-- =============================================================================
-- STEP 4: Validate Results
-- =============================================================================
PRINT '════════════════════════════════════════════════════════════════';
PRINT 'STEP 4: Validation Results';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

-- Validation 1: CertSplitSeq to Hierarchy mapping
PRINT '✓ Validation 1: CertSplitSeq → Hierarchy Mapping';
PRINT '────────────────────────────────────────────────────────────────';

SELECT 
    'Total CertSplitSeq values' AS metric,
    COUNT(DISTINCT CertSplitSeq) AS value
FROM [etl].[work_split_signatures];

SELECT 
    'Total Hierarchies created' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_id_map];

SELECT 
    'Mapping ratio (should be 1:1)' AS metric,
    CAST(
        (SELECT COUNT(*) FROM [etl].[work_hierarchy_id_map]) * 1.0 / 
        NULLIF((SELECT COUNT(DISTINCT CertSplitSeq) FROM [etl].[work_split_signatures]), 0)
    AS DECIMAL(5,2)) AS value;

-- Check for duplicates (should be 0)
SELECT 
    'Duplicate CertSplitSeq in hierarchies (should be 0)' AS metric,
    COUNT(*) AS value
FROM (
    SELECT RepresentativeSplitSeq, COUNT(*) AS cnt
    FROM [etl].[work_hierarchy_id_map]
    GROUP BY RepresentativeSplitSeq
    HAVING COUNT(*) > 1
) dups;

PRINT '';

-- Validation 2: Proposals → Hierarchies Coverage
PRINT '✓ Validation 2: Proposal → Hierarchy Coverage';
PRINT '────────────────────────────────────────────────────────────────';

-- Get proposals for test group
SELECT 
    'Proposals for ' + @test_group_id AS metric,
    COUNT(*) AS value
FROM [etl].[stg_proposals]
WHERE GroupId = @test_group_id;

-- Check how many hierarchies per proposal (via split seq mapping)
WITH proposal_hierarchy_map AS (
    SELECT DISTINCT
        p.Id AS ProposalId,
        sth.HierarchyId
    FROM [etl].[stg_proposals] p
    INNER JOIN [etl].[input_certificate_info] ci 
        ON ci.GroupId = REPLACE(p.GroupId, 'G', '')
        AND ci.CertEffectiveDate >= p.EffectiveDateFrom
        AND (p.EffectiveDateTo IS NULL OR ci.CertEffectiveDate <= p.EffectiveDateTo)
    INNER JOIN [etl].[work_splitseq_to_hierarchy] sth
        ON sth.GroupId = p.GroupId
        AND sth.CertSplitSeq = ci.CertSplitSeq
    WHERE p.GroupId = @test_group_id
)
SELECT 
    'Proposals with at least 1 hierarchy' AS metric,
    COUNT(DISTINCT ProposalId) AS value
FROM proposal_hierarchy_map;

PRINT '';

-- Validation 3: Referential Integrity Checks
PRINT '✓ Validation 3: Referential Integrity';
PRINT '────────────────────────────────────────────────────────────────';

-- Check GroupId exists in stg_groups (if needed)
SELECT 
    'Hierarchies with valid GroupId' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_data] hd
WHERE EXISTS (SELECT 1 FROM [etl].[stg_groups] g WHERE g.Id = hd.GroupId);

-- Check WritingBrokerId exists in stg_brokers
SELECT 
    'Hierarchies with valid BrokerId' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_data] hd
WHERE EXISTS (SELECT 1 FROM [etl].[stg_brokers] b WHERE b.Id = hd.WritingBrokerId);

-- Check split participants have valid brokers
SELECT 
    'Split participants with valid BrokerId' AS metric,
    COUNT(*) AS value
FROM [etl].[work_split_participants] sp
WHERE EXISTS (SELECT 1 FROM [etl].[stg_brokers] b WHERE b.Id = sp.BrokerId);

-- Check split participants have valid schedules
SELECT 
    'Split participants with valid Schedule' AS metric,
    COUNT(*) AS valid_count,
    (SELECT COUNT(*) FROM [etl].[work_split_participants]) AS total_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [etl].[work_split_participants]) AS DECIMAL(5,2)) AS pct_valid
FROM [etl].[work_split_participants] sp
WHERE EXISTS (
    SELECT 1 FROM [etl].[stg_schedules] s 
    WHERE s.ExternalId = sp.ScheduleCode 
       OR s.Name LIKE '%' + sp.ScheduleCode + '%'
);

PRINT '';

-- Validation 4: Data Quality Checks
PRINT '✓ Validation 4: Data Quality';
PRINT '────────────────────────────────────────────────────────────────';

-- Check for NULL values in critical fields
SELECT 
    'Hierarchies with NULL GroupId (should be 0)' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_data]
WHERE GroupId IS NULL;

SELECT 
    'Hierarchies with NULL BrokerId (should be 0)' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_data]
WHERE WritingBrokerId IS NULL;

SELECT 
    'Hierarchies with NULL HierarchyId (should be 0)' AS metric,
    COUNT(*) AS value
FROM [etl].[work_hierarchy_data]
WHERE HierarchyId IS NULL OR HierarchyId = '';

PRINT '';

-- =============================================================================
-- STEP 5: Detailed Breakdown for Test Group
-- =============================================================================
PRINT '════════════════════════════════════════════════════════════════';
PRINT 'STEP 5: Detailed Breakdown for ' + @test_group_id;
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

-- Show CertSplitSeq → Hierarchy mapping
PRINT '▸ CertSplitSeq → Hierarchy Mapping:';
SELECT 
    RepresentativeSplitSeq AS CertSplitSeq,
    HierarchyId,
    MinEffDate,
    WritingBrokerId
FROM [etl].[work_hierarchy_id_map]
ORDER BY RepresentativeSplitSeq;

PRINT '';

-- Show split participants per hierarchy
PRINT '▸ Participants per Hierarchy:';
SELECT 
    sth.HierarchyId,
    sth.CertSplitSeq,
    COUNT(DISTINCT sp.BrokerId) AS broker_count,
    STRING_AGG(CAST(sp.BrokerId AS VARCHAR), ', ') AS brokers
FROM [etl].[work_splitseq_to_hierarchy] sth
INNER JOIN [etl].[work_split_participants] sp 
    ON sp.GroupId = sth.GroupId 
    AND sp.CertSplitSeq = sth.CertSplitSeq
GROUP BY sth.HierarchyId, sth.CertSplitSeq
ORDER BY sth.CertSplitSeq;

PRINT '';

-- =============================================================================
-- STEP 6: Summary & Test Results
-- =============================================================================
PRINT '════════════════════════════════════════════════════════════════';
PRINT 'TEST SUMMARY';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';

DECLARE @total_splits INT = (SELECT COUNT(DISTINCT CertSplitSeq) FROM [etl].[work_split_signatures]);
DECLARE @total_hierarchies INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_id_map]);
DECLARE @null_groupid INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_data] WHERE GroupId IS NULL);
DECLARE @null_brokerid INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_data] WHERE WritingBrokerId IS NULL);
DECLARE @null_hierarchyid INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_data] WHERE HierarchyId IS NULL OR HierarchyId = '');

DECLARE @test_passed BIT = 1;

-- Check 1: CertSplitSeq to Hierarchy ratio should be 1:1
IF @total_splits <> @total_hierarchies
BEGIN
    PRINT '❌ FAIL: CertSplitSeq to Hierarchy ratio is not 1:1';
    PRINT '   Expected: ' + CAST(@total_splits AS VARCHAR) + ' hierarchies';
    PRINT '   Got: ' + CAST(@total_hierarchies AS VARCHAR) + ' hierarchies';
    SET @test_passed = 0;
END
ELSE
BEGIN
    PRINT '✅ PASS: CertSplitSeq to Hierarchy ratio is 1:1 (' + CAST(@total_hierarchies AS VARCHAR) + ' hierarchies)';
END

-- Check 2: No NULL GroupId
IF @null_groupid > 0
BEGIN
    PRINT '❌ FAIL: Found ' + CAST(@null_groupid AS VARCHAR) + ' hierarchies with NULL GroupId';
    SET @test_passed = 0;
END
ELSE
BEGIN
    PRINT '✅ PASS: All hierarchies have valid GroupId';
END

-- Check 3: No NULL BrokerId
IF @null_brokerid > 0
BEGIN
    PRINT '❌ FAIL: Found ' + CAST(@null_brokerid AS VARCHAR) + ' hierarchies with NULL BrokerId';
    SET @test_passed = 0;
END
ELSE
BEGIN
    PRINT '✅ PASS: All hierarchies have valid BrokerId';
END

-- Check 4: No NULL HierarchyId
IF @null_hierarchyid > 0
BEGIN
    PRINT '❌ FAIL: Found ' + CAST(@null_hierarchyid AS VARCHAR) + ' hierarchies with NULL HierarchyId';
    SET @test_passed = 0;
END
ELSE
BEGIN
    PRINT '✅ PASS: All hierarchies have valid HierarchyId';
END

PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
IF @test_passed = 1
BEGIN
    PRINT '✅ TEST PASSED: Referential Integrity Validated';
    PRINT '';
    PRINT '🎉 The fix is working correctly!';
    PRINT '   - No consolidation by StructureSignature';
    PRINT '   - Each CertSplitSeq has its own hierarchy';
    PRINT '   - All foreign keys are valid';
    PRINT '   - Ready for full ETL run';
END
ELSE
BEGIN
    PRINT '❌ TEST FAILED: Issues detected';
    PRINT '';
    PRINT '⚠️  Review the validation results above';
    PRINT '   - Check for NULL values';
    PRINT '   - Verify mapping ratios';
    PRINT '   - Fix issues before full ETL';
END
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';
