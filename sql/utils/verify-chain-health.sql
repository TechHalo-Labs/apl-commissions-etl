-- =====================================================
-- Quick Chain Health Verification
-- Run this to verify all fixes are working
-- =====================================================

SET NOCOUNT ON;

PRINT '════════════════════════════════════════════════════════════════════';
PRINT '🎯 CHAIN HEALTH VERIFICATION - QUICK CHECK';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '';

-- Test 1: Count all key tables
PRINT '━━━ Test 1: Table Counts ━━━';
PRINT '';

SELECT 
    'PolicyHierarchyAssignments' as [Table],
    FORMAT(COUNT(*), 'N0') as [Rows],
    'Expected: ~464K' as [Expected],
    CASE WHEN COUNT(*) > 400000 THEN '✓' ELSE '✗' END as [Status]
FROM dbo.PolicyHierarchyAssignments
UNION ALL
SELECT 'Hierarchies', FORMAT(COUNT(*), 'N0'), 'Expected: ~15K',
    CASE WHEN COUNT(*) > 14000 THEN '✓' ELSE '✗' END
FROM dbo.Hierarchies
UNION ALL
SELECT 'HierarchyVersions', FORMAT(COUNT(*), 'N0'), 'Expected: ~15K',
    CASE WHEN COUNT(*) > 14000 THEN '✓' ELSE '✗' END
FROM dbo.HierarchyVersions
UNION ALL
SELECT 'HierarchyParticipants', FORMAT(COUNT(*), 'N0'), 'Expected: ~32K',
    CASE WHEN COUNT(*) > 30000 THEN '✓' ELSE '✗' END
FROM dbo.HierarchyParticipants
UNION ALL
SELECT 'SpecialScheduleRates', FORMAT(COUNT(*), 'N0'), 'Expected: ~9.5K',
    CASE WHEN COUNT(*) > 9000 THEN '✓' ELSE '✗' END
FROM dbo.SpecialScheduleRates;

PRINT '';
PRINT '━━━ Test 2: Chain Integrity (Should ALL be 0) ━━━';
PRINT '';

-- Test 2a: PHA with invalid Hierarchy
SELECT 
    'PHA → Hierarchies' as [Chain],
    COUNT(*) as [Broken Links],
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as [Status]
FROM dbo.PolicyHierarchyAssignments pha
LEFT JOIN dbo.Hierarchies h ON h.Id = pha.HierarchyId
WHERE h.Id IS NULL;

-- Test 2b: Hierarchies without Versions
SELECT 
    'Hierarchies → Versions' as [Chain],
    COUNT(*) as [Broken Links],
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as [Status]
FROM dbo.Hierarchies h
LEFT JOIN dbo.HierarchyVersions hv ON hv.HierarchyId = h.Id
WHERE hv.Id IS NULL;

-- Test 2c: Versions without Participants
SELECT 
    'Versions → Participants' as [Chain],
    COUNT(*) as [Broken Links],
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as [Status]
FROM dbo.HierarchyVersions hv
LEFT JOIN dbo.HierarchyParticipants hp ON hp.HierarchyVersionId = hv.Id
WHERE hp.Id IS NULL;

-- Test 2d: Schedules without Rates (only those referenced by participants)
SELECT 
    'Schedules → Rates' as [Chain],
    COUNT(DISTINCT s.Id) as [Broken Links],
    CASE WHEN COUNT(DISTINCT s.Id) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as [Status]
FROM dbo.HierarchyParticipants hp
INNER JOIN dbo.Schedules s ON s.Id = hp.ScheduleId
LEFT JOIN dbo.ScheduleVersions sv ON sv.ScheduleId = s.Id
LEFT JOIN dbo.ScheduleRates sr ON sr.ScheduleVersionId = sv.Id
WHERE sr.Id IS NULL;

PRINT '';
PRINT '━━━ Test 3: Commission Calculation Readiness ━━━';
PRINT '';

-- Test 3: Sample policies can traverse complete chain
SELECT TOP 5
    p.PolicyNumber as [Policy],
    p.ProductCode as [Product],
    pha.HierarchyId as [Hierarchy],
    hp.EntityId as [BrokerId],
    hp.ScheduleCode as [Schedule],
    CASE 
        WHEN sr.FirstYearRate IS NOT NULL THEN CAST(sr.FirstYearRate AS VARCHAR) + '%'
        WHEN hp.CommissionRate IS NOT NULL THEN CAST(hp.CommissionRate AS VARCHAR) + '%'
        ELSE 'NO RATE'
    END as [Rate],
    CASE 
        WHEN sr.Id IS NOT NULL OR hp.CommissionRate IS NOT NULL 
        THEN '✓ READY' 
        ELSE '✗ NOT READY' 
    END as [Status]
FROM dbo.Policies p
INNER JOIN dbo.PolicyHierarchyAssignments pha ON pha.PolicyId = p.Id
INNER JOIN dbo.Hierarchies h ON h.Id = pha.HierarchyId
INNER JOIN dbo.HierarchyVersions hv ON hv.HierarchyId = h.Id
INNER JOIN dbo.HierarchyParticipants hp ON hp.HierarchyVersionId = hv.Id
LEFT JOIN dbo.Schedules s ON s.Id = hp.ScheduleId
LEFT JOIN dbo.ScheduleVersions sv ON sv.ScheduleId = s.Id
LEFT JOIN dbo.ScheduleRates sr ON sr.ScheduleVersionId = sv.Id AND sr.ProductCode = p.ProductCode
WHERE p.ProposalId IS NOT NULL
  AND p.Premium > 0
ORDER BY NEWID();

PRINT '';
PRINT '━━━ Test 4: Original Issue (G25565) Verification ━━━';
PRINT '';

SELECT 
    'G25565 policies' as [Metric],
    COUNT(*) as [Count]
FROM dbo.Policies
WHERE GroupId = 'G25565';

SELECT 
    'G25565 with PHA' as [Metric],
    COUNT(DISTINCT pha.PolicyId) as [Count]
FROM dbo.PolicyHierarchyAssignments pha
INNER JOIN dbo.Policies p ON p.Id = pha.PolicyId
WHERE p.GroupId = 'G25565';

SELECT 
    'G25565 can calc commissions' as [Metric],
    COUNT(DISTINCT p.Id) as [Count]
FROM dbo.Policies p
INNER JOIN dbo.PolicyHierarchyAssignments pha ON pha.PolicyId = p.Id
INNER JOIN dbo.Hierarchies h ON h.Id = pha.HierarchyId
INNER JOIN dbo.HierarchyVersions hv ON hv.HierarchyId = h.Id
INNER JOIN dbo.HierarchyParticipants hp ON hp.HierarchyVersionId = hv.Id
WHERE p.GroupId = 'G25565';

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '✅ VERIFICATION COMPLETE';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '';
PRINT 'Expected Results:';
PRINT '  - All counts > 0';
PRINT '  - All "Broken Links" = 0';
PRINT '  - All Status = ✓ PASS or ✓ READY';
PRINT '  - G25565 policies = 45,634';
PRINT '';
PRINT 'If all tests pass: Database is READY for commission calculations!';
PRINT '';
