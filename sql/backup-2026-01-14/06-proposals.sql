-- =============================================================================
-- Transform: Proposals (T-SQL)
-- Creates proposals keyed by (Group, FirstUpline)
-- Also creates premium split versions and participants
-- Usage: sqlcmd -S server -d database -i sql/transforms/06-proposals.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Proposals';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build mapping of WritingBroker -> FirstUpline per Group
-- =============================================================================
PRINT 'Step 1: Building broker-upline mappings...';

DROP TABLE IF EXISTS #tmp_broker_uplines;

SELECT 
    LTRIM(RTRIM(ci.GroupId)) AS GroupId,
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
    TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT) AS FirstUplineId,
    MIN(ci.CertEffectiveDate) AS MinEffDate,
    MAX(ci.CertEffectiveDate) AS MaxEffDate,
    MAX(ci.CertSplitPercent) AS MaxSplit,
    ci.CertSplitSeq
INTO #tmp_broker_uplines
FROM [etl].[input_certificate_info] ci
LEFT JOIN [etl].[input_certificate_info] upline 
    ON upline.CertificateId = ci.CertificateId
    AND upline.SplitBrokerSeq = 2
    AND upline.CertSplitSeq = ci.CertSplitSeq
WHERE ci.SplitBrokerSeq = 1
  AND LTRIM(RTRIM(ci.WritingBrokerID)) <> ''
  AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
  AND LTRIM(RTRIM(ci.GroupId)) <> ''
GROUP BY 
    LTRIM(RTRIM(ci.GroupId)), 
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT),
    TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT),
    ci.CertSplitSeq;

PRINT 'Broker-upline mappings: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 2: Create proposal data keyed by (Group, EffectiveUpline)
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating proposal data...';

DROP TABLE IF EXISTS #tmp_proposal_data;

SELECT 
    GroupId,
    COALESCE(FirstUplineId, WritingBrokerId) AS EffectiveUplineId,
    FirstUplineId,
    MIN(MinEffDate) AS MinEffDate,
    MAX(MaxEffDate) AS MaxEffDate,
    SUM(MaxSplit) AS TotalSplit,
    COUNT(*) AS BrokerCount
INTO #tmp_proposal_data
FROM #tmp_broker_uplines
GROUP BY GroupId, COALESCE(FirstUplineId, WritingBrokerId), FirstUplineId;

PRINT 'Proposal data rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Rank proposals and assign IDs
-- =============================================================================
PRINT '';
PRINT 'Step 3: Ranking proposals...';

DROP TABLE IF EXISTS #tmp_proposal_ranked;

SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY MaxEffDate DESC) AS recency_rank,
    ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY MinEffDate, EffectiveUplineId) AS proposal_num
INTO #tmp_proposal_ranked
FROM #tmp_proposal_data;

-- =============================================================================
-- Step 4: Truncate and populate stg_proposals
-- =============================================================================
PRINT '';
PRINT 'Step 4: Populating stg_proposals...';

TRUNCATE TABLE [etl].[stg_proposals];

INSERT INTO [etl].[stg_proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState,
    BrokerId, BrokerName, GroupId, GroupName, Notes,
    EnableEffectiveDateFiltering, ConstrainingEffectiveDateFrom, ConstrainingEffectiveDateTo,
    EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, CreationTime, IsDeleted
)
SELECT
    CONCAT('P-G', pd.GroupId, '-', CAST(pd.proposal_num AS VARCHAR)) AS Id,
    CONCAT('G', pd.GroupId, '-', CAST(pd.proposal_num AS VARCHAR)) AS ProposalNumber,
    2 AS [Status],  -- Approved
    pd.MinEffDate AS SubmittedDate,
    pd.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    g.[State] AS SitusState,
    pd.EffectiveUplineId AS BrokerId,
    b.Name AS BrokerName,
    CONCAT('G', pd.GroupId) AS GroupId,
    g.Name AS GroupName,
    '' AS Notes,
    IIF(pd.BrokerCount > 1, 1, 0) AS EnableEffectiveDateFiltering,
    pd.MinEffDate AS ConstrainingEffectiveDateFrom,
    IIF(pd.recency_rank = 1, NULL, pd.MaxEffDate) AS ConstrainingEffectiveDateTo,
    pd.MinEffDate AS EffectiveDateFrom,
    IIF(pd.recency_rank = 1, NULL, pd.MaxEffDate) AS EffectiveDateTo,
    0 AS EnablePlanCodeFiltering,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_proposal_ranked pd
LEFT JOIN [etl].[stg_groups] g ON g.Id = CONCAT('G', pd.GroupId)
LEFT JOIN [etl].[stg_brokers] b ON b.Id = pd.EffectiveUplineId
WHERE pd.GroupId <> '';

PRINT 'Proposals created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 5: Create stg_proposal_products
-- =============================================================================
PRINT '';
PRINT 'Step 5: Populating stg_proposal_products...';

TRUNCATE TABLE [etl].[stg_proposal_products];

INSERT INTO [etl].[stg_proposal_products] (
    Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.Id, pp.ProductCode) AS Id,
    p.Id AS ProposalId,
    pp.ProductCode,
    CONCAT(pp.ProductCategory, ' - ', pp.ProductCode) AS ProductName,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT DISTINCT
        LTRIM(RTRIM(ci.GroupId)) AS GroupId,
        COALESCE(
            TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT),
            TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT)
        ) AS EffectiveUplineId,
        LTRIM(RTRIM(ci.Product)) AS ProductCode,
        LTRIM(RTRIM(ci.ProductCategory)) AS ProductCategory
    FROM [etl].[input_certificate_info] ci
    LEFT JOIN [etl].[input_certificate_info] upline 
        ON upline.CertificateId = ci.CertificateId
        AND upline.SplitBrokerSeq = 2
        AND upline.CertSplitSeq = ci.CertSplitSeq
    WHERE ci.SplitBrokerSeq = 1
      AND LTRIM(RTRIM(ci.Product)) <> ''
      AND LTRIM(RTRIM(ci.WritingBrokerID)) <> ''
) pp
INNER JOIN [etl].[stg_proposals] p 
    ON p.GroupId = CONCAT('G', pp.GroupId) 
    AND p.BrokerId = pp.EffectiveUplineId;

PRINT 'Proposal products created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 6: Create stg_premium_split_versions
-- =============================================================================
PRINT '';
PRINT 'Step 6: Populating stg_premium_split_versions...';

TRUNCATE TABLE [etl].[stg_premium_split_versions];

INSERT INTO [etl].[stg_premium_split_versions] (
    Id, GroupId, GroupName, ProposalId, ProposalNumber,
    VersionNumber, EffectiveFrom, EffectiveTo,
    TotalSplitPercent, [Status], [Source], CreationTime, IsDeleted
)
SELECT
    CONCAT('PSV-', p.Id) AS Id,
    p.GroupId,
    p.GroupName,
    p.Id AS ProposalId,
    p.ProposalNumber,
    '1.0' AS VersionNumber,
    p.EffectiveDateFrom AS EffectiveFrom,
    NULL AS EffectiveTo,  -- NULL = currently active
    100.0 AS TotalSplitPercent,  -- Will be updated based on actual splits
    1 AS [Status],  -- Active
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_proposals] p;

PRINT 'Premium split versions created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 7: Create stg_premium_split_participants from certificate splits
-- =============================================================================
PRINT '';
PRINT 'Step 7: Populating stg_premium_split_participants...';

TRUNCATE TABLE [etl].[stg_premium_split_participants];

-- Get unique split participants per proposal
-- FIXED: Group by (GroupId, EffectiveUplineId, WritingBrokerId, SplitPercent) to deduplicate
-- This ensures each broker+percent combination appears only once per proposal
DROP TABLE IF EXISTS #split_participants;

SELECT
    CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
    COALESCE(
        TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT),
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT)
    ) AS EffectiveUplineId,
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
    ci.CertSplitPercent AS SplitPercent,
    MIN(ci.CertSplitSeq) AS SplitSequence, -- Keep first sequence for ordering
    MAX(LTRIM(RTRIM(ci.CommissionsSchedule))) AS ScheduleCode
INTO #split_participants
FROM [etl].[input_certificate_info] ci
LEFT JOIN [etl].[input_certificate_info] upline 
    ON upline.CertificateId = ci.CertificateId
    AND upline.SplitBrokerSeq = 2
    AND upline.CertSplitSeq = ci.CertSplitSeq
WHERE ci.SplitBrokerSeq = 1
  AND LTRIM(RTRIM(ci.WritingBrokerID)) <> ''
  AND LTRIM(RTRIM(ci.GroupId)) <> ''
  AND ci.CertSplitPercent IS NOT NULL
GROUP BY 
    LTRIM(RTRIM(ci.GroupId)),
    COALESCE(
        TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT),
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT)
    ),
    TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT),
    ci.CertSplitPercent;  -- Include SplitPercent to keep distinct broker+percent combos

INSERT INTO [etl].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, Sequence, WritingBrokerId, EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY psv.Id, sp.SplitSequence, sp.WritingBrokerId) AS Id,
    psv.Id AS VersionId,
    sp.WritingBrokerId AS BrokerId,
    b.Name AS BrokerName,
    sp.SplitPercent,
    1 AS IsWritingAgent,
    -- Join to stg_hierarchies to get the actual HierarchyId
    h.Id AS HierarchyId,
    sp.SplitSequence AS Sequence,
    sp.WritingBrokerId,
    p.EffectiveDateFrom AS EffectiveFrom,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #split_participants sp
INNER JOIN [etl].[stg_proposals] p 
    ON p.GroupId = sp.GroupId 
    AND p.BrokerId = sp.EffectiveUplineId
INNER JOIN [etl].[stg_premium_split_versions] psv ON psv.ProposalId = p.Id
LEFT JOIN [etl].[stg_brokers] b ON b.Id = sp.WritingBrokerId
LEFT JOIN [etl].[stg_hierarchies] h 
    ON h.GroupId = p.GroupId
    AND h.BrokerId = sp.WritingBrokerId;

PRINT 'Premium split participants created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Update TotalSplitPercent in versions based on actual participants
UPDATE psv
SET TotalSplitPercent = COALESCE(totals.TotalSplit, 100.0)
FROM [etl].[stg_premium_split_versions] psv
LEFT JOIN (
    SELECT VersionId, SUM(SplitPercent) AS TotalSplit
    FROM [etl].[stg_premium_split_participants]
    GROUP BY VersionId
) totals ON totals.VersionId = psv.Id;

-- =============================================================================
-- Step 8: Flag non-conformant groups based on split percentage
-- =============================================================================
PRINT '';
PRINT 'Step 8: Flagging non-conformant groups...';

-- First, reset all groups to conformant
UPDATE [etl].[stg_groups]
SET IsNonConformant = 0,
    NonConformantDescription = NULL;

-- Flag groups with split percentages that don't equal 100%
UPDATE g
SET IsNonConformant = 1,
    NonConformantDescription = CONCAT(
        'Split percentage non-conformance: ',
        nc.NonConformantCount, ' proposal(s) with invalid splits. ',
        'Min: ', nc.MinSplit, '%, Max: ', nc.MaxSplit, '%, ',
        'Valid (100%): ', nc.ValidCount
    )
FROM [etl].[stg_groups] g
INNER JOIN (
    SELECT 
        p.GroupId,
        COUNT(*) AS TotalProposals,
        SUM(CASE WHEN psv.TotalSplitPercent = 100 THEN 1 ELSE 0 END) AS ValidCount,
        SUM(CASE WHEN psv.TotalSplitPercent <> 100 THEN 1 ELSE 0 END) AS NonConformantCount,
        MIN(psv.TotalSplitPercent) AS MinSplit,
        MAX(psv.TotalSplitPercent) AS MaxSplit
    FROM [etl].[stg_proposals] p
    INNER JOIN [etl].[stg_premium_split_versions] psv ON psv.ProposalId = p.Id
    GROUP BY p.GroupId
    HAVING SUM(CASE WHEN psv.TotalSplitPercent <> 100 THEN 1 ELSE 0 END) > 0
) nc ON nc.GroupId = g.Id;

DECLARE @nonconf_count INT = @@ROWCOUNT;
PRINT 'Groups flagged as non-conformant: ' + CAST(@nonconf_count AS VARCHAR);

-- Summary of non-conformance
SELECT 
    'Non-conformance summary' AS metric,
    COUNT(*) AS total_groups,
    SUM(CASE WHEN IsNonConformant = 1 THEN 1 ELSE 0 END) AS nonconformant_groups,
    SUM(CASE WHEN IsNonConformant = 0 THEN 1 ELSE 0 END) AS conformant_groups
FROM [etl].[stg_groups];

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Proposals' AS entity, COUNT(*) AS cnt FROM [etl].[stg_proposals];
SELECT 'Proposal Products' AS entity, COUNT(*) AS cnt FROM [etl].[stg_proposal_products];
SELECT 'Premium Split Versions' AS entity, COUNT(*) AS cnt FROM [etl].[stg_premium_split_versions];
SELECT 'Premium Split Participants' AS entity, COUNT(*) AS cnt FROM [etl].[stg_premium_split_participants];

-- Active vs Historical proposals
SELECT 'Proposal status' AS metric,
       SUM(CASE WHEN EffectiveDateTo IS NULL THEN 1 ELSE 0 END) AS active_proposals,
       SUM(CASE WHEN EffectiveDateTo IS NOT NULL THEN 1 ELSE 0 END) AS historical_proposals
FROM [etl].[stg_proposals];

-- Cleanup
DROP TABLE IF EXISTS #tmp_broker_uplines;
DROP TABLE IF EXISTS #tmp_proposal_data;
DROP TABLE IF EXISTS #tmp_proposal_ranked;
DROP TABLE IF EXISTS #split_participants;

PRINT '';
PRINT '============================================================';
PRINT 'PROPOSALS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

