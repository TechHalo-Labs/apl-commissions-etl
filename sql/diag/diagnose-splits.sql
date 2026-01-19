-- =====================================================
-- Diagnose PremiumSplitVersions Export Issue
-- Run this to identify why split versions aren't being exported
-- =====================================================

PRINT '============================================================';
PRINT 'DIAGNOSING PREMIUM SPLIT EXPORT ISSUE';
PRINT '============================================================';
PRINT '';

-- =====================================================
-- Step 1: Check record counts in all related tables
-- =====================================================
PRINT 'STEP 1: Record Counts';
PRINT '------------------------------------------------------------';

SELECT 'etl.stg_premium_split_versions' AS [Table], COUNT(*) AS [Count] 
FROM [etl].[stg_premium_split_versions]
UNION ALL
SELECT 'etl.stg_premium_split_participants', COUNT(*) 
FROM [etl].[stg_premium_split_participants]
UNION ALL
SELECT 'dbo.PremiumSplitVersions (production)', COUNT(*) 
FROM [dbo].[PremiumSplitVersions]
UNION ALL
SELECT 'dbo.PremiumSplitParticipants (production)', COUNT(*) 
FROM [dbo].[PremiumSplitParticipants]
UNION ALL
SELECT 'etl.stg_proposals', COUNT(*) 
FROM [etl].[stg_proposals]
UNION ALL
SELECT 'dbo.Proposals (production)', COUNT(*) 
FROM [dbo].[Proposals];

-- =====================================================
-- Step 2: Sample staging data
-- =====================================================
PRINT '';
PRINT 'STEP 2: Sample Staging Split Versions (first 10)';
PRINT '------------------------------------------------------------';

SELECT TOP 10 
    Id, 
    GroupId, 
    ProposalId, 
    TotalSplitPercent,
    EffectiveFrom,
    [Status]
FROM [etl].[stg_premium_split_versions]
ORDER BY Id;

-- =====================================================
-- Step 3: Test GroupId conversion logic
-- =====================================================
PRINT '';
PRINT 'STEP 3: GroupId Conversion Test (first 20)';
PRINT '------------------------------------------------------------';

SELECT TOP 20 
    GroupId AS OriginalGroupId,
    -- Step 1: Strip all letters
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        GroupId,
        'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
        'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
        'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
        'V',''),'W',''),'X',''),'Y',''),'Z','') AS StrippedGroupId,
    -- Step 2: Try to cast to BIGINT
    TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
    ) AS ConvertedBigint,
    CASE 
        WHEN TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                GroupId,
                'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
                'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
                'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
                'V',''),'W',''),'X',''),'Y',''),'Z','')
            AS BIGINT
        ) IS NOT NULL THEN 'PASS' ELSE 'FAIL'
    END AS ConversionResult
FROM [etl].[stg_premium_split_versions];

-- =====================================================
-- Step 4: Count conversion failures
-- =====================================================
PRINT '';
PRINT 'STEP 4: Conversion Success/Failure Counts';
PRINT '------------------------------------------------------------';

SELECT 
    CASE 
        WHEN TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                GroupId,
                'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
                'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
                'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
                'V',''),'W',''),'X',''),'Y',''),'Z','')
            AS BIGINT
        ) IS NOT NULL THEN 'Would Export' ELSE 'Would NOT Export (NULL GroupId)'
    END AS ExportStatus,
    COUNT(*) AS [Count]
FROM [etl].[stg_premium_split_versions]
GROUP BY 
    CASE 
        WHEN TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                GroupId,
                'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
                'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
                'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
                'V',''),'W',''),'X',''),'Y',''),'Z','')
            AS BIGINT
        ) IS NOT NULL THEN 'Would Export' ELSE 'Would NOT Export (NULL GroupId)'
    END;

-- =====================================================
-- Step 5: Check for records that would be filtered out
-- =====================================================
PRINT '';
PRINT 'STEP 5: Records Already in Production (should be 0 if empty)';
PRINT '------------------------------------------------------------';

SELECT COUNT(*) AS RecordsAlreadyExist
FROM [etl].[stg_premium_split_versions] spsv
WHERE spsv.Id IN (SELECT Id FROM [dbo].[PremiumSplitVersions]);

-- =====================================================
-- Step 6: Sample participants
-- =====================================================
PRINT '';
PRINT 'STEP 6: Sample Staging Split Participants (first 10)';
PRINT '------------------------------------------------------------';

SELECT TOP 10 
    Id, 
    VersionId, 
    BrokerId, 
    BrokerName,
    SplitPercent,
    HierarchyId
FROM [etl].[stg_premium_split_participants]
ORDER BY Id;

-- =====================================================
-- Step 7: Simulate the export query (dry run)
-- =====================================================
PRINT '';
PRINT 'STEP 7: Simulated Export (what WOULD be inserted)';
PRINT '------------------------------------------------------------';

SELECT TOP 10
    spsv.Id,
    TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
    ) AS GroupId,
    spsv.GroupName,
    spsv.ProposalId,
    spsv.VersionNumber,
    spsv.EffectiveFrom,
    CASE WHEN spsv.TotalSplitPercent > 999.99 THEN 999.99 ELSE spsv.TotalSplitPercent END AS TotalSplitPercent,
    spsv.[Status]
FROM [etl].[stg_premium_split_versions] spsv
WHERE spsv.Id NOT IN (SELECT Id FROM [dbo].[PremiumSplitVersions])
  AND TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
      ) IS NOT NULL;

PRINT '';
PRINT '============================================================';
PRINT 'DIAGNOSIS COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'If staging tables are empty (Step 1):';
PRINT '  → Run the full pipeline: npx tsx scripts/run-pipeline.ts --restore-backup --skip-calc';
PRINT '';
PRINT 'If GroupId conversions fail (Step 4 shows "Would NOT Export"):';
PRINT '  → Check GroupId format in staging - may contain special characters';
PRINT '';
PRINT 'If all records would export but production is empty:';
PRINT '  → Export script may have failed silently - check pipeline logs';
GO
