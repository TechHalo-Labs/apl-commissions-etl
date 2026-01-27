-- =============================================================================
-- Transform: Proposals with Combined Split Participants
-- 
-- Creates ONE proposal per Group, with a split version containing ALL
-- split participants (multiple writing brokers sharing the premium).
--
-- Key insight: A certificate can have multiple CertSplitSeq values, each with
-- a different WritingBroker and SplitPercent. These should be combined into
-- ONE split version with multiple participants.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Proposals (Combined Split Participants)';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Identify unique split participants per Group
-- 
-- Key insight about CertSplitSeq:
-- - The sequence numbers can start at any value (1,2 or 74,75 or 4,5)
-- - What matters is the RELATIVE ORDER: lowest = first participant, next = second
-- - Multiple certificates may have different seq numbers but same split config
-- 
-- We deduplicate by (GroupId, WritingBrokerId, SplitPercent) to get unique participants
-- Then assign sequence based on SplitPercent DESC (highest split first)
-- =============================================================================
PRINT 'Step 1: Analyzing split participants per group...';

DROP TABLE IF EXISTS #group_split_configs;

-- Get unique (Group, WritingBroker, SplitPercent) combinations
WITH unique_splits AS (
    SELECT 
        LTRIM(RTRIM(ci.GroupId)) AS RawGroupId,
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        -- NEW: Extract BrokerUniquePartyId (strip 'P' prefix, get numeric string)
        REPLACE(REPLACE(LTRIM(RTRIM(ci.WritingBrokerID)), 'P', ''), ' ', '') AS WritingBrokerUniquePartyId,
        TRY_CAST(ci.CertSplitPercent AS DECIMAL(5,2)) AS SplitPercent,
        MIN(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS MinEffDate,
        MAX(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS MaxEffDate,
        MAX(ci.CertIssuedState) AS SitusState,
        MAX(LTRIM(RTRIM(ci.CommissionsSchedule))) AS ScheduleCode
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.SplitBrokerSeq = 1  -- Only get writing broker level records
      AND LTRIM(RTRIM(ci.WritingBrokerID)) <> ''
      AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
      AND LTRIM(RTRIM(ci.GroupId)) <> ''
      AND ci.CertSplitPercent IS NOT NULL
    GROUP BY 
        LTRIM(RTRIM(ci.GroupId)),
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT),
        REPLACE(REPLACE(LTRIM(RTRIM(ci.WritingBrokerID)), 'P', ''), ' ', ''),
        TRY_CAST(ci.CertSplitPercent AS DECIMAL(5,2))
)
SELECT 
    RawGroupId,
    GroupId,
    WritingBrokerId,
    WritingBrokerUniquePartyId,  -- NEW: BrokerUniquePartyId field
    SplitPercent,
    -- Assign sequence based on split percent (highest first) within each group
    ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY SplitPercent DESC, WritingBrokerId) AS SplitSequence,
    MinEffDate,
    MaxEffDate,
    SitusState,
    ScheduleCode
INTO #group_split_configs
FROM unique_splits;

PRINT 'Split participants found: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 2: Create ONE proposal per Group with combined split info
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating proposal data (one per group)...';

DROP TABLE IF EXISTS #group_proposals;

-- Calculate MinEffDate from ALL certificates in the group (not just SplitBrokerSeq = 1)
-- This ensures EffectiveDateFrom covers all policies, including those with earlier dates
SELECT 
    gp.GroupId,
    gp.RawGroupId,
    -- Get the TRUE minimum effective date from ALL certificates in the group
    COALESCE(
        (SELECT MIN(TRY_CAST(ci.CertEffectiveDate AS DATE))
         FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
         WHERE CONCAT('G', LTRIM(RTRIM(ci.GroupId))) = gp.GroupId
           AND LTRIM(RTRIM(ci.GroupId)) <> ''
           AND ci.CertEffectiveDate IS NOT NULL
           AND TRY_CAST(ci.CertEffectiveDate AS DATE) IS NOT NULL),
        MIN(gp.MinEffDate)  -- Fallback to minimum from split configs if no certificates found
    ) AS MinEffDate,
    MAX(gp.MaxEffDate) AS MaxEffDate,
    MAX(gp.SitusState) AS SitusState,
    SUM(gp.SplitPercent) AS TotalSplitPercent,
    COUNT(DISTINCT gp.WritingBrokerId) AS ParticipantCount,
    COUNT(DISTINCT gp.SplitSequence) AS SplitCount
INTO #group_proposals
FROM (
    SELECT DISTINCT
        GroupId,
        RawGroupId,
        MinEffDate,
        MaxEffDate,
        SitusState,
        SplitPercent,
        WritingBrokerId,
        SplitSequence
    FROM #group_split_configs
) gp
GROUP BY gp.GroupId, gp.RawGroupId;

PRINT 'Group proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Truncate and populate stg_proposals (ONE per Group)
-- =============================================================================
PRINT '';
PRINT 'Step 3: Populating stg_proposals...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposals];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState,
    BrokerUniquePartyId, BrokerName, GroupId, GroupName, Notes,
    ProductCodes, PlanCodes, SplitConfigHash, DateRangeFrom, DateRangeTo,
    EnableEffectiveDateFiltering, ConstrainingEffectiveDateFrom, ConstrainingEffectiveDateTo,
    EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, CreationTime, IsDeleted
)
SELECT
    CONCAT('P-', gp.GroupId, '-1') AS Id,
    CONCAT(gp.GroupId, '-1') AS ProposalNumber,
    2 AS [Status],  -- Approved
    gp.MinEffDate AS SubmittedDate,
    gp.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    COALESCE(gp.SitusState, g.[State]) AS SitusState,
    -- Use the first writing broker's ExternalPartyId as the "lead" broker
    -- Only populate if broker exists in stg_brokers (validation)
    (SELECT TOP 1 
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b 
                WHERE b.ExternalPartyId = sc.WritingBrokerUniquePartyId
            )
            THEN sc.WritingBrokerUniquePartyId
            ELSE NULL
        END
     FROM #group_split_configs sc 
     WHERE sc.GroupId = gp.GroupId 
     ORDER BY sc.SplitPercent DESC) AS BrokerUniquePartyId,
    (SELECT TOP 1 b.Name FROM #group_split_configs sc 
     LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = sc.WritingBrokerUniquePartyId
     WHERE sc.GroupId = gp.GroupId ORDER BY sc.SplitPercent DESC) AS BrokerName,
    gp.GroupId,
    g.Name AS GroupName,
    CONCAT('Combined split: ', gp.ParticipantCount, ' participants, ', 
           CAST(gp.TotalSplitPercent AS VARCHAR), '% total') AS Notes,
    '*' AS ProductCodes,
    '*' AS PlanCodes,
    CONCAT('SC-', gp.GroupId) AS SplitConfigHash,
    YEAR(gp.MinEffDate) AS DateRangeFrom,
    NULL AS DateRangeTo,  -- Open-ended
    0 AS EnableEffectiveDateFiltering,
    gp.MinEffDate AS ConstrainingEffectiveDateFrom,
    NULL AS ConstrainingEffectiveDateTo,
    gp.MinEffDate AS EffectiveDateFrom,
    NULL AS EffectiveDateTo,
    0 AS EnablePlanCodeFiltering,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #group_proposals gp
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = gp.GroupId
WHERE gp.GroupId IS NOT NULL AND gp.GroupId <> 'G';

DECLARE @proposal_count INT = @@ROWCOUNT;
PRINT 'Proposals created: ' + CAST(@proposal_count AS VARCHAR);

-- =============================================================================
-- Step 3.5: Update BrokerName from stg_brokers (populate if NULL/empty)
-- =============================================================================
PRINT '';
PRINT 'Step 3.5: Updating BrokerName from stg_brokers...';

UPDATE sp
SET sp.BrokerName = COALESCE(
    NULLIF(LTRIM(RTRIM(b.Name)), ''),
    CONCAT('Broker ', sp.BrokerUniquePartyId)
)
FROM [$(ETL_SCHEMA)].[stg_proposals] sp
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = sp.BrokerUniquePartyId
WHERE sp.BrokerUniquePartyId IS NOT NULL
    AND (
        sp.BrokerName IS NULL 
        OR LTRIM(RTRIM(sp.BrokerName)) = ''
        OR sp.BrokerName = CONCAT('Broker ', sp.BrokerUniquePartyId)  -- Update placeholder names too
    )
    AND b.Name IS NOT NULL
    AND LTRIM(RTRIM(b.Name)) <> '';

DECLARE @broker_name_updated INT = @@ROWCOUNT;
PRINT 'BrokerName updated for ' + CAST(@broker_name_updated AS VARCHAR) + ' proposals';

-- =============================================================================
-- Step 4: Populate stg_split_configs
-- =============================================================================
PRINT '';
PRINT 'Step 4: Populating stg_split_configs...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_split_configs];

INSERT INTO [$(ETL_SCHEMA)].[stg_split_configs] (SplitConfigHash, TotalSplitPercent, ParticipantCount, ConfigJson)
SELECT
    CONCAT('SC-', gp.GroupId) AS SplitConfigHash,
    CASE WHEN gp.TotalSplitPercent > 999.99 THEN 999.99 ELSE gp.TotalSplitPercent END AS TotalSplitPercent,
    gp.ParticipantCount,
    NULL AS ConfigJson
FROM #group_proposals gp
WHERE gp.GroupId IS NOT NULL AND gp.GroupId <> 'G';

PRINT 'Split configs created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 5: Populate stg_premium_split_versions (ONE per proposal)
-- =============================================================================
PRINT '';
PRINT 'Step 5: Populating stg_premium_split_versions...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_versions];

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_versions] (
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
    NULL AS EffectiveTo,
    gp.TotalSplitPercent,
    1 AS [Status],  -- Active
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_proposals] p
INNER JOIN #group_proposals gp ON gp.GroupId = p.GroupId;

PRINT 'Premium split versions created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 6: Populate stg_premium_split_participants (MULTIPLE per version)
-- This is the key fix - we now include ALL writing brokers for each group
-- =============================================================================
PRINT '';
PRINT 'Step 6: Populating stg_premium_split_participants (combined)...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_participants];

-- First, pick ONE hierarchy per (Group, Broker) combination
-- Note: stg_hierarchies still uses BrokerId, so we'll join by BrokerId for now
-- TODO: Update stg_hierarchies to also have BrokerUniquePartyId in future phase
DROP TABLE IF EXISTS #broker_hierarchies;

SELECT 
    h.GroupId,
    h.BrokerId,
    MIN(h.Id) AS HierarchyId,  -- Pick first hierarchy if multiple exist
    MIN(h.Name) AS HierarchyName
INTO #broker_hierarchies
FROM [$(ETL_SCHEMA)].[stg_hierarchies] h
GROUP BY h.GroupId, h.BrokerId;

INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, HierarchyName, Sequence, WritingBrokerId, EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY psv.Id, sc.SplitSequence) AS Id,
    psv.Id AS VersionId,
    -- BrokerId (required, deprecated but still needed)
    COALESCE(b.Id, sc.WritingBrokerId, 0) AS BrokerId,
    -- NEW: Populate BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b 
            WHERE b.ExternalPartyId = sc.WritingBrokerUniquePartyId
        )
        THEN sc.WritingBrokerUniquePartyId
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    sc.SplitPercent,
    1 AS IsWritingAgent,
    bh.HierarchyId,
    bh.HierarchyName,
    sc.SplitSequence AS Sequence,
    sc.WritingBrokerId,  -- Keep for hierarchy lookup (stg_hierarchies still uses BrokerId)
    p.EffectiveDateFrom AS EffectiveFrom,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #group_split_configs sc
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.GroupId = sc.GroupId
INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_versions] psv ON psv.ProposalId = p.Id
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = sc.WritingBrokerUniquePartyId
LEFT JOIN #broker_hierarchies bh 
    ON bh.GroupId = sc.GroupId
    AND bh.BrokerId = sc.WritingBrokerId;  -- Join by BrokerId (hierarchies still use BrokerId)

DROP TABLE IF EXISTS #broker_hierarchies;

DECLARE @participant_count INT = @@ROWCOUNT;
PRINT 'Premium split participants created: ' + CAST(@participant_count AS VARCHAR);

-- =============================================================================
-- Step 7: Populate stg_proposal_key_mapping
-- =============================================================================
PRINT '';
PRINT 'Step 7: Populating stg_proposal_key_mapping...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposal_key_mapping];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_key_mapping] (
    GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
)
SELECT DISTINCT
    CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
    YEAR(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS EffectiveYear,
    LTRIM(RTRIM(ci.Product)) AS ProductCode,
    COALESCE(LTRIM(RTRIM(ci.PlanCode)), '*') AS PlanCode,
    p.Id AS ProposalId,
    p.SplitConfigHash
FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.GroupId = CONCAT('G', LTRIM(RTRIM(ci.GroupId)))
WHERE LTRIM(RTRIM(ci.GroupId)) <> ''
  AND ci.Product IS NOT NULL 
  AND LTRIM(RTRIM(ci.Product)) <> ''
  AND ci.CertEffectiveDate IS NOT NULL
  AND TRY_CAST(ci.CertEffectiveDate AS DATE) IS NOT NULL;

PRINT 'Key mappings created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 8: Populate stg_proposal_products
-- =============================================================================
PRINT '';
PRINT 'Step 8: Populating stg_proposal_products...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposal_products];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_products] (
    Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.Id, pp.ProductCode) AS Id,
    p.Id AS ProposalId,
    pp.ProductCode,
    CONCAT(COALESCE(pp.ProductCategory, 'Unknown'), ' - ', pp.ProductCode) AS ProductName,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        LTRIM(RTRIM(ci.Product)) AS ProductCode,
        MAX(LTRIM(RTRIM(ci.ProductCategory))) AS ProductCategory
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.SplitBrokerSeq = 1
      AND LTRIM(RTRIM(ci.Product)) <> ''
      AND LTRIM(RTRIM(ci.GroupId)) <> ''
    GROUP BY LTRIM(RTRIM(ci.GroupId)), LTRIM(RTRIM(ci.Product))
) pp
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.GroupId = pp.GroupId;

PRINT 'Proposal products created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Proposals' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_proposals];
SELECT 'Split Configs' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_split_configs];
SELECT 'Premium Split Versions' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_premium_split_versions];
SELECT 'Premium Split Participants' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_premium_split_participants];
SELECT 'Key Mappings' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_proposal_key_mapping];
SELECT 'Proposal Products' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_proposal_products];

-- Show sample of combined split participants (groups with multiple brokers)
PRINT '';
PRINT 'Sample: Groups with multiple split participants:';
SELECT TOP 20
    psv.GroupId,
    psv.TotalSplitPercent,
    psp.BrokerUniquePartyId,
    psp.BrokerName,
    psp.SplitPercent,
    psp.Sequence
FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] psv
INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_participants] psp ON psp.VersionId = psv.Id
WHERE psv.Id IN (
    SELECT TOP 5 VersionId 
    FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] 
    GROUP BY VersionId 
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
)
ORDER BY psv.GroupId, psp.Sequence;

-- Cleanup
DROP TABLE IF EXISTS #group_split_configs;
DROP TABLE IF EXISTS #group_proposals;

PRINT '';
PRINT '============================================================';
PRINT 'PROPOSALS TRANSFORM COMPLETED';
PRINT '============================================================';

GO
