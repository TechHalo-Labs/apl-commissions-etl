-- =============================================================================
-- Transform: Hierarchies (SQL Server)
-- =============================================================================
-- Creates one hierarchy per (Group, WritingBroker)
-- Each hierarchy contains the commission chain for that writing broker
-- Hierarchies link to Proposals via (Group, FirstUpline)
-- 
-- CRITICAL BUG FIX: Transferee Exclusion Logic
-- =============================================
-- When BrokerId === PaidBrokerId (self-payment), the broker should NOT be 
-- excluded as a transferee. The fix ensures:
-- 1. Self-payments (PaidBrokerId = SplitBrokerId) are NEVER treated as transfers
-- 2. A broker is only excluded if they appear as PaidBrokerId but NOT as SplitBrokerId
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: HIERARCHIES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build broker -> upline mapping for linking to proposals
-- =============================================================================
DROP TABLE IF EXISTS #tmp_hierarchy_data;

SELECT 
    GroupId,
    WritingBrokerId,
    FirstUplineId,
    MIN(EffDate) AS MinEffDate
INTO #tmp_hierarchy_data
FROM (
    SELECT 
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        TRY_CAST(REPLACE(upline.SplitBrokerId, 'P', '') AS BIGINT) AS FirstUplineId,
        ci.CertEffectiveDate AS EffDate
    FROM [etl].[input_certificate_info] ci
    LEFT JOIN [etl].[input_certificate_info] upline 
        ON upline.CertificateId = ci.CertificateId
        AND upline.SplitBrokerSeq = 2
        AND upline.CertSplitSeq = ci.CertSplitSeq
    WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates])
      AND LTRIM(RTRIM(ci.GroupId)) IN (SELECT GroupNumber FROM [etl].[ref_active_groups])
      AND ci.SplitBrokerSeq = 1
      AND ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> ''
      AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
) base
WHERE WritingBrokerId IS NOT NULL
GROUP BY GroupId, WritingBrokerId, FirstUplineId;

DECLARE @cnt_hdata INT = (SELECT COUNT(*) FROM #tmp_hierarchy_data);
PRINT 'Hierarchy data built: ' + CAST(@cnt_hdata AS VARCHAR) + ' rows';

-- =============================================================================
-- Step 2: Create mapping table with canonical IDs
-- =============================================================================
DROP TABLE IF EXISTS #tmp_hierarchy_id_map;

SELECT
    GroupId,
    WritingBrokerId,
    FirstUplineId,
    CONCAT('H-', GroupId, '-', CAST(split_num AS VARCHAR)) AS HierarchyId,
    split_num
INTO #tmp_hierarchy_id_map
FROM (
    SELECT
        GroupId,
        WritingBrokerId,
        FirstUplineId,
        ROW_NUMBER() OVER (PARTITION BY GroupId ORDER BY WritingBrokerId, FirstUplineId) AS split_num
    FROM #tmp_hierarchy_data
) sub;

DECLARE @cnt_idmap INT = (SELECT COUNT(*) FROM #tmp_hierarchy_id_map);
PRINT 'Hierarchy ID map built: ' + CAST(@cnt_idmap AS VARCHAR) + ' rows';

GO

-- =============================================================================
-- Hierarchies - One per (Group, WritingBroker)
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchies];

-- Use CTE to get one row per hierarchy (may have multiple proposals per hierarchy)
;WITH hierarchy_base AS (
    SELECT 
        idmap.HierarchyId,
        hd.GroupId,
        hd.WritingBrokerId,
        hd.FirstUplineId,
        hd.MinEffDate,
        g.Name AS GroupName,
        b.Name AS BrokerName,
        g.[State] AS SitusState,
        ROW_NUMBER() OVER (PARTITION BY idmap.HierarchyId ORDER BY hd.MinEffDate DESC) AS rn
    FROM #tmp_hierarchy_data hd
    INNER JOIN #tmp_hierarchy_id_map idmap ON idmap.GroupId = hd.GroupId 
        AND idmap.WritingBrokerId = hd.WritingBrokerId
        AND (idmap.FirstUplineId = hd.FirstUplineId OR (idmap.FirstUplineId IS NULL AND hd.FirstUplineId IS NULL))
    LEFT JOIN [etl].[stg_groups] g ON g.Id = hd.GroupId
    LEFT JOIN [etl].[stg_brokers] b ON b.Id = hd.WritingBrokerId
    WHERE hd.WritingBrokerId IS NOT NULL
)
INSERT INTO [etl].[stg_hierarchies] (
    Id, Name, [Description], [Type], [Status], ProposalId, ProposalNumber, GroupId, GroupName,
    GroupNumber, BrokerId, BrokerName, BrokerLevel, ContractId, SourceType,
    SitusState, EffectiveDate, CurrentVersionId, CurrentVersionNumber, CreationTime, IsDeleted
)
SELECT
    hb.HierarchyId AS Id,
    CONCAT('Hierarchy: ', hb.GroupId, ' - ', COALESCE(hb.BrokerName, CONCAT('Broker ', CAST(hb.WritingBrokerId AS VARCHAR)))) AS Name,
    CONCAT('Commission hierarchy for ', COALESCE(hb.BrokerName, 'broker'), ' on group ', hb.GroupId) AS [Description],
    0 AS [Type],
    0 AS [Status],
    p.Id AS ProposalId,
    p.ProposalNumber AS ProposalNumber,
    hb.GroupId AS GroupId,
    hb.GroupName AS GroupName,
    REPLACE(hb.GroupId, 'G', '') AS GroupNumber,
    hb.WritingBrokerId AS BrokerId,
    hb.BrokerName AS BrokerName,
    1 AS BrokerLevel,
    NULL AS ContractId,
    'Migration' AS SourceType,
    hb.SitusState AS SitusState,
    CAST(hb.MinEffDate AS DATE) AS EffectiveDate,
    CONCAT(hb.HierarchyId, '-V1') AS CurrentVersionId,
    1 AS CurrentVersionNumber,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM hierarchy_base hb
LEFT JOIN [etl].[stg_proposals] p ON p.GroupId = hb.GroupId 
    AND p.BrokerId = COALESCE(hb.FirstUplineId, hb.WritingBrokerId)
    AND p.EffectiveDateTo IS NULL  -- Only get the active proposal
WHERE hb.rn = 1;

PRINT 'Hierarchies staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS hierarchies_staged FROM [etl].[stg_hierarchies];

GO

-- =============================================================================
-- Hierarchy Versions - One version per hierarchy
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchy_versions];

INSERT INTO [etl].[stg_hierarchy_versions] (
    Id, HierarchyId, [Version], [Status], EffectiveFrom, EffectiveTo, ChangeReason, CreationTime, IsDeleted
)
SELECT
    CONCAT(h.Id, '-V1') AS Id,
    h.Id AS HierarchyId,
    1 AS [Version],
    1 AS [Status],  -- Active
    h.EffectiveDate AS EffectiveFrom,
    NULL AS EffectiveTo,
    'Initial migration' AS ChangeReason,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[stg_hierarchies] h;

PRINT 'Hierarchy versions staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS hierarchy_versions_staged FROM [etl].[stg_hierarchy_versions];

GO

-- =============================================================================
-- Hierarchy Participants - Commission chain for each hierarchy
-- 
-- CRITICAL BUG FIX: Transferee Exclusion Logic
-- =============================================
-- A broker should ONLY be excluded as a transferee if ALL of these are true:
-- 1. They appear as PaidBrokerId (they receive the commission)
-- 2. The ReassignedType is 'Transferred' or 'Assigned'
-- 3. PaidBrokerId != SplitBrokerId (it's NOT a self-payment)
-- 4. PaidBrokerId is NOT also a SplitBrokerId for the same certificate/split
--
-- This ensures:
-- - Self-payments (BrokerId == PaidBrokerId) are NEVER treated as transfers
-- - Cross-assignments (A transfers to B, B assigns to A) include BOTH brokers
-- =============================================================================
TRUNCATE TABLE [etl].[stg_hierarchy_participants];

-- Step 1a: Get all POTENTIAL transferees (NOT self-payments)
-- CRITICAL: ci.PaidBrokerId <> ci.SplitBrokerId excludes self-payments
DROP TABLE IF EXISTS #tmp_potential_transferees;

SELECT DISTINCT
    ci.CertificateId,
    ci.CertSplitSeq,
    ci.PaidBrokerId AS TransfereeBrokerId
INTO #tmp_potential_transferees
FROM [etl].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates])
  AND ci.ReassignedType IN ('Transferred', 'Assigned')
  AND ci.PaidBrokerId IS NOT NULL AND ci.PaidBrokerId <> ''
  AND ci.PaidBrokerId <> ci.SplitBrokerId;  -- CRITICAL: Exclude self-payments

DECLARE @cnt_pot INT = (SELECT COUNT(*) FROM #tmp_potential_transferees);
PRINT 'Potential transferees (excluding self-payments): ' + CAST(@cnt_pot AS VARCHAR);

-- Step 1b: Get all earners (SplitBrokerIds) per certificate
DROP TABLE IF EXISTS #tmp_all_earners;

SELECT DISTINCT
    ci.CertificateId,
    ci.CertSplitSeq,
    ci.SplitBrokerId
INTO #tmp_all_earners
FROM [etl].[input_certificate_info] ci
WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates]);

DECLARE @cnt_earners INT = (SELECT COUNT(*) FROM #tmp_all_earners);
PRINT 'All earners: ' + CAST(@cnt_earners AS VARCHAR);

-- Step 1c: Final transferees = potential transferees WHO ARE NOT ALSO EARNERS
-- This ensures that if a broker is BOTH a transferee AND an earner, they are included
DROP TABLE IF EXISTS #tmp_transferees;

SELECT 
    pt.CertificateId,
    pt.CertSplitSeq,
    TRY_CAST(REPLACE(pt.TransfereeBrokerId, 'P', '') AS BIGINT) AS TransfereeBrokerId
INTO #tmp_transferees
FROM #tmp_potential_transferees pt
WHERE NOT EXISTS (
    -- Check if this "transferee" is also an earner (SplitBrokerId) for the same cert/split
    SELECT 1 FROM #tmp_all_earners ae
    WHERE ae.CertificateId = pt.CertificateId
      AND ae.CertSplitSeq = pt.CertSplitSeq
      AND ae.SplitBrokerId = pt.TransfereeBrokerId  -- Transferee is also an earner
);

DECLARE @cnt_trans INT = (SELECT COUNT(*) FROM #tmp_transferees);
PRINT 'TRUE transferees (not also earners): ' + CAST(@cnt_trans AS VARCHAR);

-- Step 2: Build participant data, EXCLUDING only TRUE transferees
-- Use CTE to deduplicate by participant Id
;WITH participant_data AS (
    SELECT 
        CONCAT(hv.Id, 'P', CAST(participants.SplitBrokerId AS VARCHAR), 'L', CAST(participants.[Level] AS VARCHAR)) AS Id,
        hv.Id AS HierarchyVersionId,
        participants.SplitBrokerId AS EntityId,
        b.Name AS EntityName,
        participants.[Level],
        participants.[Level] AS SortOrder,
        participants.SplitPercent,
        participants.ScheduleCode,
        participants.CommissionRate,
        participants.PaidBrokerId,
        ROW_NUMBER() OVER (PARTITION BY CONCAT(hv.Id, 'P', CAST(participants.SplitBrokerId AS VARCHAR), 'L', CAST(participants.[Level] AS VARCHAR)) ORDER BY hv.Id) AS rn
    FROM [etl].[stg_hierarchy_versions] hv
    INNER JOIN [etl].[stg_hierarchies] h ON h.Id = hv.HierarchyId
    INNER JOIN (
    -- Get participants, excluding TRUE transferees
    SELECT 
        GroupId,
        WritingBrokerId,
        SplitBrokerId,
        [Level],
        MIN(SplitPercent) AS SplitPercent,
        MIN(ScheduleCode) AS ScheduleCode,
        MIN(CommissionRate) AS CommissionRate,
        MIN(PaidBrokerId) AS PaidBrokerId
    FROM (
        SELECT 
            CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
            TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
            TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) AS SplitBrokerId,
            ci.SplitBrokerSeq AS [Level],
            TRY_CAST(ci.CertSplitPercent AS DECIMAL(18,4)) AS SplitPercent,
            ci.CommissionsSchedule AS ScheduleCode,
            COALESCE(TRY_CAST(cd.RealCommissionRate AS DECIMAL(18,4)), 0) AS CommissionRate,
            TRY_CAST(REPLACE(ci.PaidBrokerId, 'P', '') AS BIGINT) AS PaidBrokerId,
            ci.CertificateId,
            ci.CertSplitSeq
        FROM [etl].[input_certificate_info] ci
        LEFT JOIN [etl].[input_commission_details] cd ON cd.CertificateId = ci.CertificateId 
                                                AND cd.SplitBrokerId = ci.SplitBrokerId
        WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates])
          AND LTRIM(RTRIM(ci.GroupId)) IN (SELECT GroupNumber FROM [etl].[ref_active_groups])
          AND ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> ''
          AND ci.SplitBrokerId IS NOT NULL AND ci.SplitBrokerId <> ''
          AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
          AND TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) IS NOT NULL
    ) base
    -- EXCLUDE TRUE transferees: brokers who are ONLY transferees (not also earners)
    WHERE NOT EXISTS (
        SELECT 1 FROM #tmp_transferees t
        WHERE t.CertificateId = base.CertificateId
          AND t.CertSplitSeq = base.CertSplitSeq
          AND t.TransfereeBrokerId = base.SplitBrokerId
    )
    GROUP BY GroupId, WritingBrokerId, SplitBrokerId, [Level]
) participants ON participants.GroupId = h.GroupId
              AND participants.WritingBrokerId = h.BrokerId
    LEFT JOIN [etl].[stg_brokers] b ON b.Id = participants.SplitBrokerId
    WHERE participants.SplitBrokerId IS NOT NULL AND participants.[Level] IS NOT NULL
)
INSERT INTO [etl].[stg_hierarchy_participants] (
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder, SplitPercent,
    ScheduleCode, ScheduleId, CommissionRate, PaidBrokerId, CreationTime, IsDeleted
)
SELECT 
    Id, HierarchyVersionId, EntityId, EntityName, [Level], SortOrder, SplitPercent,
    ScheduleCode, NULL, CommissionRate, PaidBrokerId, GETUTCDATE(), 0
FROM participant_data
WHERE rn = 1;

PRINT 'Hierarchy participants staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Link ScheduleId to stg_hierarchy_participants
-- Match ScheduleCode to stg_schedules.ExternalId to get the actual ScheduleId
-- =============================================================================
PRINT '';
PRINT 'Linking ScheduleId to hierarchy participants...';

UPDATE hp
SET hp.ScheduleId = s.Id
FROM [etl].[stg_hierarchy_participants] hp
INNER JOIN [etl].[stg_schedules] s ON s.ExternalId = hp.ScheduleCode
WHERE hp.ScheduleCode IS NOT NULL AND hp.ScheduleCode <> '';

PRINT 'ScheduleId linked: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' participants updated';

-- Report participants without ScheduleId
DECLARE @missing_sched INT = (
    SELECT COUNT(*) FROM [etl].[stg_hierarchy_participants] 
    WHERE ScheduleCode IS NOT NULL AND ScheduleCode <> '' AND ScheduleId IS NULL
);
IF @missing_sched > 0
    PRINT 'WARNING: ' + CAST(@missing_sched AS VARCHAR) + ' participants have ScheduleCode but no matching Schedule';

SELECT COUNT(*) AS hierarchy_participants_staged FROM [etl].[stg_hierarchy_participants];

GO

-- =============================================================================
-- Commission Assignment Versions - Track assignments/transfers
-- =============================================================================
TRUNCATE TABLE [etl].[stg_commission_assignment_versions];

-- Use CTE with ROW_NUMBER to deduplicate by Id
;WITH assignment_data AS (
    SELECT
        CONCAT('CAV-', assignments.GroupId, '-', CAST(assignments.EarnerBrokerId AS VARCHAR), '-', CAST(assignments.RecipientBrokerId AS VARCHAR)) AS Id,
    assignments.EarnerBrokerId AS BrokerId,
    b.Name AS BrokerName,
    '__DEFAULT__' AS ProposalId,
    TRY_CAST(assignments.GroupId AS BIGINT) AS GroupId,
    h.Id AS HierarchyId,
    hv.Id AS HierarchyVersionId,
    hp.Id AS HierarchyParticipantId,
    '1.0' AS VersionNumber,
    MIN(assignments.EffectiveDate) AS EffectiveFrom,
    NULL AS EffectiveTo,
    0 AS [Status],
    CASE WHEN assignments.AssignmentType = 'Assigned' THEN 0 ELSE 1 END AS [Type],
    CONCAT(assignments.AssignmentType, ' from ', CAST(assignments.EarnerBrokerId AS VARCHAR), ' to ', CAST(assignments.RecipientBrokerId AS VARCHAR)) AS ChangeDescription,
    100.0 AS TotalAssignedPercent,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT DISTINCT
        LTRIM(RTRIM(ci.GroupId)) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT) AS EarnerBrokerId,
        TRY_CAST(REPLACE(ci.PaidBrokerId, 'P', '') AS BIGINT) AS RecipientBrokerId,
        ci.ReassignedType AS AssignmentType,
        ci.CertEffectiveDate AS EffectiveDate
    FROM [etl].[input_certificate_info] ci
    WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates])
      AND LTRIM(RTRIM(ci.GroupId)) IN (SELECT GroupNumber FROM [etl].[ref_active_groups])
      AND ci.ReassignedType IN ('Transferred', 'Assigned')
      AND ci.SplitBrokerId <> ci.PaidBrokerId  -- Actually transferred/assigned
      AND ci.WritingBrokerID IS NOT NULL AND ci.WritingBrokerID <> ''
      AND ci.SplitBrokerId IS NOT NULL AND ci.SplitBrokerId <> ''
      AND ci.PaidBrokerId IS NOT NULL AND ci.PaidBrokerId <> ''
) assignments
LEFT JOIN #tmp_hierarchy_data hd ON hd.GroupId = CONCAT('G', assignments.GroupId)
                                AND hd.WritingBrokerId = assignments.WritingBrokerId
LEFT JOIN #tmp_hierarchy_id_map him ON him.GroupId = CONCAT('G', assignments.GroupId)
                                   AND him.WritingBrokerId = assignments.WritingBrokerId
LEFT JOIN [etl].[stg_hierarchies] h ON h.Id = him.HierarchyId
LEFT JOIN [etl].[stg_hierarchy_versions] hv ON hv.HierarchyId = h.Id
LEFT JOIN [etl].[stg_hierarchy_participants] hp ON hp.HierarchyVersionId = hv.Id 
                                        AND hp.EntityId = assignments.EarnerBrokerId
LEFT JOIN [etl].[stg_brokers] b ON b.Id = assignments.EarnerBrokerId
WHERE assignments.EarnerBrokerId IS NOT NULL 
  AND assignments.RecipientBrokerId IS NOT NULL
  AND assignments.EarnerBrokerId <> assignments.RecipientBrokerId
GROUP BY assignments.GroupId, assignments.WritingBrokerId, assignments.EarnerBrokerId, 
         assignments.RecipientBrokerId, assignments.AssignmentType,
         h.Id, hv.Id, hp.Id, b.Name
),
assignment_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY EffectiveFrom DESC) AS rn
    FROM assignment_data
)
INSERT INTO [etl].[stg_commission_assignment_versions] (
    Id, BrokerId, BrokerName, ProposalId, GroupId, HierarchyId, HierarchyVersionId,
    HierarchyParticipantId, VersionNumber, EffectiveFrom, EffectiveTo, [Status], [Type],
    ChangeDescription, TotalAssignedPercent, CreationTime, IsDeleted
)
SELECT
    Id, BrokerId, BrokerName, ProposalId, GroupId, HierarchyId, HierarchyVersionId,
    HierarchyParticipantId, VersionNumber, EffectiveFrom, EffectiveTo, [Status], [Type],
    ChangeDescription, TotalAssignedPercent, GETUTCDATE(), 0
FROM assignment_dedup
WHERE rn = 1;

PRINT 'Commission assignment versions staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS commission_assignments_staged FROM [etl].[stg_commission_assignment_versions];

GO

-- =============================================================================
-- Commission Assignment Recipients
-- =============================================================================
TRUNCATE TABLE [etl].[stg_commission_assignment_recipients];

;WITH recipient_data AS (
    SELECT
        CONCAT(cav.Id, '-R') AS Id,
        cav.Id AS AssignmentVersionId,
        TRY_CAST(REPLACE(ci.PaidBrokerId, 'P', '') AS BIGINT) AS RecipientBrokerId,
        b.Name AS RecipientBrokerName,
        100.0 AS [Percent],
        1 AS RecipientType,
        ROW_NUMBER() OVER (PARTITION BY CONCAT(cav.Id, '-R') ORDER BY ci.CertEffectiveDate DESC) AS rn
    FROM [etl].[input_certificate_info] ci
    INNER JOIN [etl].[stg_commission_assignment_versions] cav 
        ON cav.BrokerId = TRY_CAST(REPLACE(ci.SplitBrokerId, 'P', '') AS BIGINT)
    LEFT JOIN [etl].[stg_brokers] b ON b.Id = TRY_CAST(REPLACE(ci.PaidBrokerId, 'P', '') AS BIGINT)
    WHERE CAST(ci.CertificateId AS NVARCHAR(100)) IN (SELECT CertificateId FROM [etl].[ref_active_certificates])
      AND LTRIM(RTRIM(ci.GroupId)) IN (SELECT GroupNumber FROM [etl].[ref_active_groups])
      AND ci.ReassignedType IN ('Transferred', 'Assigned')
      AND ci.SplitBrokerId <> ci.PaidBrokerId
)
INSERT INTO [etl].[stg_commission_assignment_recipients] (
    Id, AssignmentVersionId, RecipientBrokerId, RecipientBrokerName, [Percent], RecipientType,
    CreationTime, IsDeleted
)
SELECT
    Id, AssignmentVersionId, RecipientBrokerId, RecipientBrokerName, [Percent], RecipientType,
    GETUTCDATE(), 0
FROM recipient_data
WHERE rn = 1;

PRINT 'Commission assignment recipients staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS commission_assignment_recipients_staged FROM [etl].[stg_commission_assignment_recipients];

-- Cleanup temp tables (keep hierarchy_id_map for other transforms)
DROP TABLE IF EXISTS #tmp_potential_transferees;
DROP TABLE IF EXISTS #tmp_all_earners;
DROP TABLE IF EXISTS #tmp_transferees;

PRINT '';
PRINT '============================================================';
PRINT 'HIERARCHIES TRANSFORM COMPLETED';
PRINT '============================================================';

GO

