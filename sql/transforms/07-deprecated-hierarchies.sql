-- =============================================================================
-- Transform: Hierarchies (SQL Server)
-- =============================================================================
-- FIXED: Creates one hierarchy per (GroupId, CertSplitSeq, WritingBrokerId)
-- Each split sequence gets its own hierarchy (NO CONSOLIDATION)
-- This ensures proposals with different time periods have their own hierarchies
-- Uses work tables to persist data across GO batches
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: HIERARCHIES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build full hierarchy structure per (Group, CertSplitSeq)
-- =============================================================================
TRUNCATE TABLE [etl].[work_split_participants];

-- Build list of groups that have proposals (from all proposal sources)
-- This ensures hierarchies are created for ALL groups with proposals, not just ref_active_groups
WITH proposal_groups AS (
    SELECT DISTINCT GroupId FROM [etl].[simple_groups]
    UNION
    SELECT DISTINCT GroupId FROM [etl].[plan_differentiated_keys]
    UNION
    SELECT DISTINCT GroupId FROM [etl].[year_differentiated_keys]
    UNION
    -- Include all groups that have proposals in staging (e.g., consolidated proposals, non-conformant)
    SELECT DISTINCT GroupId FROM [etl].[stg_proposals]
    WHERE GroupId IS NOT NULL AND GroupId <> ''
)
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
INNER JOIN proposal_groups pg ON pg.GroupId = CONCAT('G', LTRIM(RTRIM(ci.GroupId)))
WHERE ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> ''
  AND ci.SplitBrokerId IS NOT NULL AND ci.SplitBrokerId <> ''
  AND ci.RecStatus = 'A'  -- Only active split configurations (filter out historical/deleted)
  AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
  AND TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) IS NOT NULL
GROUP BY ci.GroupId, ci.CertSplitSeq, ci.WritingBrokerID, ci.SplitBrokerSeq, ci.SplitBrokerId, 
         ci.CommissionsSchedule, ci.CertSplitPercent;

DECLARE @cnt_parts INT = (SELECT COUNT(*) FROM [etl].[work_split_participants]);
PRINT 'Split participants extracted: ' + CAST(@cnt_parts AS VARCHAR) + ' rows';

GO

-- =============================================================================
-- Step 2: Create structure signatures
-- =============================================================================
TRUNCATE TABLE [etl].[work_split_signatures];

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

GO

-- =============================================================================
-- Step 3: Create HierarchyId mapping
-- =============================================================================
TRUNCATE TABLE [etl].[work_hierarchy_id_map];

-- FIXED: Create one hierarchy per (GroupId, CertSplitSeq, WritingBrokerId)
-- Do NOT consolidate by StructureSignature to avoid orphaning proposals with different time periods
INSERT INTO [etl].[work_hierarchy_id_map] (GroupId, WritingBrokerId, StructureSignature, MinEffDate, RepresentativeSplitSeq, HierarchyId)
SELECT
    GroupId,
    WritingBrokerId,
    StructureSignature,
    MinEffDate,
    CertSplitSeq AS RepresentativeSplitSeq,
    CONCAT('H-', GroupId, '-', CAST(ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY CertSplitSeq, MinEffDate) AS VARCHAR)) AS HierarchyId
FROM [etl].[work_split_signatures]
-- REMOVED: GROUP BY GroupId, WritingBrokerId, StructureSignature
-- Each CertSplitSeq gets its own hierarchy (no consolidation)

DECLARE @cnt_idmap INT = (SELECT COUNT(*) FROM [etl].[work_hierarchy_id_map]);
PRINT 'Hierarchy ID map: ' + CAST(@cnt_idmap AS VARCHAR) + ' hierarchies';

GO

-- =============================================================================
-- Step 4: Map splitSeq to hierarchy
-- =============================================================================
TRUNCATE TABLE [etl].[work_splitseq_to_hierarchy];

-- FIXED: 1-to-1 mapping (no consolidation)
-- Each CertSplitSeq maps to exactly one hierarchy
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
    AND him.RepresentativeSplitSeq = ss.CertSplitSeq  -- FIXED: Join on RepresentativeSplitSeq=CertSplitSeq (1-to-1) instead of StructureSignature (many-to-1)

DECLARE @cnt_map INT = (SELECT COUNT(*) FROM [etl].[work_splitseq_to_hierarchy]);
PRINT 'SplitSeq mappings: ' + CAST(@cnt_map AS VARCHAR) + ' rows';

GO

-- =============================================================================
-- Step 5: Build hierarchy data with FirstUplineId
-- =============================================================================
TRUNCATE TABLE [etl].[work_hierarchy_data];

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

GO

-- =============================================================================
-- Step 6: Populate stg_splitseq_hierarchy_map
-- =============================================================================
TRUNCATE TABLE [etl].[stg_splitseq_hierarchy_map];

INSERT INTO [etl].[stg_splitseq_hierarchy_map] (GroupId, CertSplitSeq, WritingBrokerId, HierarchyId, StructureSignature, CreationTime)
SELECT
    sth.GroupId,
    sth.CertSplitSeq,
    sth.WritingBrokerId,
    sth.HierarchyId,
    ss.StructureSignature,
    GETUTCDATE()
FROM [etl].[work_splitseq_to_hierarchy] sth
INNER JOIN [etl].[work_split_signatures] ss 
    ON ss.GroupId = sth.GroupId 
    AND ss.CertSplitSeq = sth.CertSplitSeq
    AND ss.WritingBrokerId = sth.WritingBrokerId;

DECLARE @cnt_shm INT = (SELECT COUNT(*) FROM [etl].[stg_splitseq_hierarchy_map]);
PRINT 'SplitSeq-Hierarchy map staged: ' + CAST(@cnt_shm AS VARCHAR) + ' rows';

GO

-- =============================================================================
-- Step 7: Create Hierarchies
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchies];

-- FIXED: Create hierarchies - ONE per CertSplitSeq (no consolidation by structure)
-- Link to matching proposals based on date ranges
INSERT INTO [prestage].[prestage_hierarchies] (
    Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber, GroupId, GroupName,
    GroupNumber, BrokerId, BrokerName, BrokerLevel, ContractId, SourceType,
    SitusState, EffectiveDate, CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
)
SELECT
    hd.HierarchyId AS Id,
    CONCAT('Hierarchy: ', hd.GroupId, ' - ', COALESCE(b.Name, CONCAT('Broker ', CAST(hd.WritingBrokerId AS VARCHAR)))) AS Name,
    CONCAT('Commission hierarchy for ', COALESCE(b.Name, 'broker'), ' on group ', hd.GroupId) AS [Description],
    0 AS [Type],
    1 AS [Status],  -- Active (was 0=Inactive - FIXED)
    -- Link to the FIRST matching proposal based on date range
    -- Priority: 1) Proposal where hierarchy date falls within proposal range
    --           2) Open-ended proposal (no end date) where hierarchy date >= proposal start
    --           3) Most recent proposal as fallback
    COALESCE(
        -- Match 1: Hierarchy date within proposal date range
        (SELECT TOP 1 p.Id 
         FROM [etl].[stg_proposals] p 
         WHERE p.GroupId = hd.GroupId
           AND p.EffectiveDateFrom IS NOT NULL
           AND CAST(hd.MinEffDate AS DATE) >= p.EffectiveDateFrom
           AND (p.EffectiveDateTo IS NULL OR CAST(hd.MinEffDate AS DATE) <= p.EffectiveDateTo)
         ORDER BY p.EffectiveDateFrom DESC),
        -- Match 2: Open-ended proposal where hierarchy date >= proposal start
        (SELECT TOP 1 p.Id 
         FROM [etl].[stg_proposals] p 
         WHERE p.GroupId = hd.GroupId
           AND p.EffectiveDateTo IS NULL
           AND p.EffectiveDateFrom IS NOT NULL
           AND CAST(hd.MinEffDate AS DATE) >= p.EffectiveDateFrom
         ORDER BY p.EffectiveDateFrom DESC),
        -- Match 3: Fallback to most recent proposal
        (SELECT TOP 1 p.Id 
         FROM [etl].[stg_proposals] p 
         WHERE p.GroupId = hd.GroupId
         ORDER BY p.EffectiveDateFrom DESC)
    ) AS ProposalId,
    (SELECT TOP 1 p.ProposalNumber 
     FROM [etl].[stg_proposals] p 
     WHERE p.GroupId = hd.GroupId
     ORDER BY p.EffectiveDateFrom DESC) AS ProposalNumber,
    hd.GroupId AS GroupId,
    g.Name AS GroupName,
    REPLACE(hd.GroupId, 'G', '') AS GroupNumber,
    hd.WritingBrokerId AS BrokerId,
    b.Name AS BrokerName,
    1 AS BrokerLevel,
    NULL AS ContractId,
    'Migration' AS SourceType,
    g.[State] AS SitusState,
    CAST(hd.MinEffDate AS DATE) AS EffectiveDate,
    CONCAT(hd.HierarchyId, '-V1') AS CurrentVersionId,
    1 AS CurrentVersionNumber,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[work_hierarchy_data] hd
LEFT JOIN [etl].[stg_groups] g ON g.Id = hd.GroupId
LEFT JOIN [etl].[stg_brokers] b ON b.Id = hd.WritingBrokerId
WHERE hd.WritingBrokerId IS NOT NULL;

PRINT 'Hierarchies staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

GO

-- =============================================================================
-- Step 8: Create Hierarchy Versions
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchy_versions];

INSERT INTO [prestage].[prestage_hierarchy_versions] (
    Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo, ChangeReason, CreationTime, IsDeleted
)
SELECT
    CONCAT(h.Id, '-V1') AS Id,
    h.Id AS HierarchyId,
    1 AS [Version],
    1 AS [Status],
    h.EffectiveDate AS EffectiveFrom,
    CAST('2099-01-01' AS DATETIME2) AS EffectiveTo,
    'Initial migration' AS ChangeReason,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] h;

PRINT 'Hierarchy versions staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

GO

-- =============================================================================
-- Step 9: Create Hierarchy Participants
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchy_participants];

;WITH deduped_participants AS (
    SELECT 
        CONCAT(h.Id, '-V1', 'P', CAST(sp.BrokerId AS VARCHAR), 'L', CAST(sp.[Level] AS VARCHAR)) AS Id,
        CONCAT(h.Id, '-V1') AS HierarchyVersionId,
        sp.BrokerId AS EntityId,
        b.Name AS EntityName,
        sp.[Level],
        sp.[Level] AS SortOrder,
        sp.SplitPercent,
        sp.ScheduleCode,
        ROW_NUMBER() OVER (PARTITION BY h.Id, sp.BrokerId, sp.[Level] ORDER BY sp.MinEffDate DESC) AS rn
    FROM [etl].[stg_hierarchies] h
    INNER JOIN [etl].[work_hierarchy_id_map] him ON him.HierarchyId = h.Id
    INNER JOIN [etl].[work_split_participants] sp 
        ON sp.GroupId = h.GroupId
        AND sp.CertSplitSeq = him.RepresentativeSplitSeq
        AND sp.WritingBrokerId = him.WritingBrokerId
    LEFT JOIN [etl].[stg_brokers] b ON b.Id = sp.BrokerId
    WHERE sp.BrokerId IS NOT NULL
)
INSERT INTO [prestage].[prestage_hierarchy_participants] (
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder, SplitPercent,
    ScheduleCode, ScheduleId, CommissionRate, PaidBrokerId, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder, SplitPercent,
    ScheduleCode, NULL, 0, NULL, GETUTCDATE(), 0
FROM deduped_participants
WHERE rn = 1;

PRINT 'Hierarchy participants staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Link ScheduleId
UPDATE hp
SET hp.ScheduleId = s.Id
FROM [etl].[stg_hierarchy_participants] hp
INNER JOIN [etl].[stg_schedules] s ON s.ExternalId = hp.ScheduleCode
WHERE hp.ScheduleCode IS NOT NULL AND hp.ScheduleCode <> '';

PRINT 'ScheduleId linked: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' participants';

GO

-- =============================================================================
-- Step 10: Link Split Participants to Hierarchies
-- =============================================================================
PRINT '';
PRINT 'Linking split participants to hierarchies...';

UPDATE psp
SET 
    psp.HierarchyId = shm.HierarchyId,
    psp.HierarchyName = h.Name
FROM [etl].[stg_premium_split_participants] psp
INNER JOIN [etl].[stg_splitseq_hierarchy_map] shm 
    ON shm.GroupId = psp.GroupId
    AND shm.CertSplitSeq = psp.Sequence
    AND shm.WritingBrokerId = psp.WritingBrokerId
LEFT JOIN [etl].[stg_hierarchies] h ON h.Id = shm.HierarchyId
WHERE psp.HierarchyId IS NULL;

PRINT 'Split participants linked: ' + CAST(@@ROWCOUNT AS VARCHAR);

PRINT '';
PRINT '============================================================';
PRINT 'HIERARCHIES TRANSFORM COMPLETED';
PRINT '============================================================';

GO
