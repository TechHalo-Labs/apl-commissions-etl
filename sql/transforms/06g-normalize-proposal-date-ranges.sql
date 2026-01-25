-- =============================================================================
-- Transform: Proposals - Step 7: Normalize Proposal Effective Date Ranges
-- 
-- Purpose: Ensure proposals for the same group form contiguous, non-overlapping sequences
-- Logic: If a group has multiple proposals with different EffectiveDateFrom values,
--         close out previous proposals when a new one starts
-- 
-- This step runs AFTER consolidation (06f) to normalize the final proposal set.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 7: Normalize Proposal Effective Date Ranges';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Normalize EffectiveDateTo for proposals with different EffectiveDateFrom
-- =============================================================================
PRINT 'Step 1: Normalizing proposal effective date ranges...';

WITH ProposalSequences AS (
    SELECT 
        Id,
        GroupId,
        ProposalNumber,
        EffectiveDateFrom,
        EffectiveDateTo,
        ROW_NUMBER() OVER (
            PARTITION BY GroupId 
            ORDER BY 
                EffectiveDateFrom ASC,
                CreationTime ASC
        ) AS SequenceNum,
        LEAD(EffectiveDateFrom) OVER (
            PARTITION BY GroupId 
            ORDER BY EffectiveDateFrom ASC, CreationTime ASC
        ) AS NextProposalStartDate
    FROM [etl].[stg_proposals]
    WHERE GroupId IS NOT NULL
        AND EffectiveDateFrom IS NOT NULL
        -- Only process groups that have multiple proposals with different EffectiveDateFrom values
        AND GroupId IN (
            SELECT GroupId
            FROM [etl].[stg_proposals]
            WHERE GroupId IS NOT NULL
                AND EffectiveDateFrom IS NOT NULL
            GROUP BY GroupId
            HAVING COUNT(DISTINCT EffectiveDateFrom) > 1
        )
)
UPDATE sp
SET 
    EffectiveDateTo = DATEADD(DAY, -1, ps.NextProposalStartDate)
FROM [etl].[stg_proposals] sp
INNER JOIN ProposalSequences ps ON sp.Id = ps.Id
WHERE ps.NextProposalStartDate IS NOT NULL  -- Only if there's a next proposal
    AND ps.NextProposalStartDate > ps.EffectiveDateFrom  -- Ensure next proposal starts after current one
    -- Always update to ensure contiguous sequence (will update even if EffectiveDateTo already exists)
    AND (
        ps.EffectiveDateTo IS NULL  -- Update proposals without end date
        OR ps.EffectiveDateTo != DATEADD(DAY, -1, ps.NextProposalStartDate)  -- OR proposals with incorrect end date
    )

DECLARE @normalized_count INT = @@ROWCOUNT;
PRINT 'Normalized ' + CAST(@normalized_count AS VARCHAR) + ' proposal effective date ranges.';
PRINT '';

-- =============================================================================
-- Step 2: Verification - Check for any remaining gaps or overlaps
-- =============================================================================
PRINT 'Step 2: Verifying proposal date range integrity...';

WITH ProposalSequences AS (
    SELECT 
        Id,
        GroupId,
        ProposalNumber,
        EffectiveDateFrom,
        EffectiveDateTo,
        ROW_NUMBER() OVER (
            PARTITION BY GroupId 
            ORDER BY EffectiveDateFrom ASC, CreationTime ASC
        ) AS SequenceNum,
        LAG(EffectiveDateTo) OVER (
            PARTITION BY GroupId 
            ORDER BY EffectiveDateFrom ASC, CreationTime ASC
        ) AS PreviousProposalEndDate
    FROM [etl].[stg_proposals]
    WHERE GroupId IS NOT NULL
        AND EffectiveDateFrom IS NOT NULL
        AND GroupId IN (
            SELECT GroupId
            FROM [etl].[stg_proposals]
            WHERE GroupId IS NOT NULL
                AND EffectiveDateFrom IS NOT NULL
            GROUP BY GroupId
            HAVING COUNT(DISTINCT EffectiveDateFrom) > 1
        )
)
SELECT 
    COUNT(*) AS GapCount,
    STRING_AGG(CAST(GroupId + ':' + ProposalNumber AS NVARCHAR(MAX)), ', ') AS GroupsWithGaps
INTO #gap_check
FROM ProposalSequences
WHERE SequenceNum > 1
    AND PreviousProposalEndDate IS NOT NULL
    AND EffectiveDateFrom > DATEADD(DAY, 1, PreviousProposalEndDate);  -- Gap detected

DECLARE @gap_count INT;
SELECT @gap_count = GapCount FROM #gap_check;

IF @gap_count > 0
BEGIN
    PRINT 'WARNING: Found ' + CAST(@gap_count AS VARCHAR) + ' groups with gaps in proposal date ranges.';
    SELECT * FROM #gap_check;
END
ELSE
BEGIN
    PRINT 'SUCCESS: All proposal date ranges are contiguous.';
END

DROP TABLE IF EXISTS #gap_check;

PRINT '';
PRINT '============================================================';
PRINT 'PROPOSALS STEP 7: Complete';
PRINT '============================================================';
PRINT '';

GO
