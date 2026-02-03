-- =====================================================
-- Commission Assignments Transform
-- =====================================================
-- Generates commission assignment records from certificate data
-- where SplitBrokerId != PaidBrokerId (indicates broker assignment)
-- 
-- Run after: proposal-builder (hierarchies must exist)
-- =====================================================

SET NOCOUNT ON;
PRINT '=== Commission Assignments Transform ===';
PRINT '';

-- Step 1: Clear existing staging data
PRINT 'Step 1: Clearing staging assignment tables...';
TRUNCATE TABLE [etl].[stg_commission_assignment_versions];
TRUNCATE TABLE [etl].[stg_commission_assignment_recipients];
PRINT '  ✓ Truncated staging tables';

-- Step 2: Generate and insert commission assignment versions in one shot
PRINT '';
PRINT 'Step 2: Generating commission assignment versions...';

;WITH BrokerAssignments AS (
    -- Find unique broker assignments (source -> recipient) with most recent effective date
    SELECT 
        ci.SplitBrokerId AS SourceBrokerId,
        ci.PaidBrokerId AS RecipientBrokerId,
        MAX(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS EffectiveDate
    FROM [etl].[input_certificate_info] ci
    WHERE ci.CertStatus = 'A'
      AND ci.RecStatus = 'A'
      AND ci.SplitBrokerId IS NOT NULL
      AND ci.PaidBrokerId IS NOT NULL
      AND LTRIM(RTRIM(ci.SplitBrokerId)) != ''
      AND LTRIM(RTRIM(ci.PaidBrokerId)) != ''
      AND ci.SplitBrokerId != ci.PaidBrokerId
    GROUP BY ci.SplitBrokerId, ci.PaidBrokerId
),
RankedAssignments AS (
    -- Rank to get one hierarchy link per broker (most recent)
    SELECT 
        ba.SourceBrokerId,
        ba.RecipientBrokerId,
        ba.EffectiveDate,
        psp.HierarchyId,
        hv.Id AS HierarchyVersionId,
        hp.Id AS HierarchyParticipantId,
        psv.ProposalId,
        ROW_NUMBER() OVER (
            PARTITION BY ba.SourceBrokerId 
            ORDER BY ba.EffectiveDate DESC, psp.HierarchyId
        ) AS rn
    FROM BrokerAssignments ba
    -- Find a hierarchy participant for this broker
    INNER JOIN [etl].[stg_hierarchy_participants] hp 
        ON hp.EntityId = TRY_CAST(REPLACE(ba.SourceBrokerId, 'P', '') AS BIGINT)
    INNER JOIN [etl].[stg_hierarchy_versions] hv 
        ON hv.Id = hp.HierarchyVersionId
    INNER JOIN [etl].[stg_premium_split_participants] psp 
        ON psp.HierarchyId = hv.HierarchyId
    INNER JOIN [etl].[stg_premium_split_versions] psv 
        ON psv.Id = psp.VersionId
),
NumberedAssignments AS (
    SELECT 
        ra.*,
        ROW_NUMBER() OVER (ORDER BY ra.SourceBrokerId) AS AssignmentNum
    FROM RankedAssignments ra
    WHERE ra.rn = 1
)
INSERT INTO [etl].[stg_commission_assignment_versions] (
    Id, BrokerId, BrokerName, ProposalId, GroupId,
    HierarchyId, HierarchyVersionId, HierarchyParticipantId,
    VersionNumber, EffectiveFrom, EffectiveTo,
    Status, Type, ChangeDescription, TotalAssignedPercent,
    CreationTime, IsDeleted
)
SELECT 
    CONCAT('CAV-', na.AssignmentNum) AS Id,
    TRY_CAST(REPLACE(na.SourceBrokerId, 'P', '') AS BIGINT) AS BrokerId,
    COALESCE(sb.Name, CONCAT('Broker ', na.SourceBrokerId)) AS BrokerName,
    na.ProposalId,
    NULL AS GroupId,
    na.HierarchyId,
    na.HierarchyVersionId,
    na.HierarchyParticipantId,
    'V1' AS VersionNumber,
    na.EffectiveDate AS EffectiveFrom,
    '2099-01-01' AS EffectiveTo,
    1 AS Status,
    1 AS Type,
    NULL AS ChangeDescription,
    100.00 AS TotalAssignedPercent,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM NumberedAssignments na
LEFT JOIN [dbo].[Brokers] sb ON sb.ExternalPartyId = na.SourceBrokerId;

DECLARE @versionCount INT = @@ROWCOUNT;
PRINT CONCAT('  ✓ Inserted ', @versionCount, ' commission assignment versions');

-- Step 3: Generate and insert commission assignment recipients
PRINT '';
PRINT 'Step 3: Generating commission assignment recipients...';

;WITH VersionRecipients AS (
    SELECT 
        cav.Id AS AssignmentVersionId,
        ROW_NUMBER() OVER (ORDER BY cav.Id) AS RecipientNum,
        -- Find the recipient broker from the original certificate data
        ba.RecipientBrokerId
    FROM [etl].[stg_commission_assignment_versions] cav
    -- Rejoin to get recipient info
    CROSS APPLY (
        SELECT TOP 1 ci.PaidBrokerId AS RecipientBrokerId
        FROM [etl].[input_certificate_info] ci
        WHERE ci.SplitBrokerId = 'P' + CAST(cav.BrokerId AS VARCHAR(20))
          AND ci.SplitBrokerId != ci.PaidBrokerId
          AND ci.CertStatus = 'A'
          AND ci.RecStatus = 'A'
        ORDER BY TRY_CAST(ci.CertEffectiveDate AS DATE) DESC
    ) ba
)
INSERT INTO [etl].[stg_commission_assignment_recipients] (
    Id, AssignmentVersionId, RecipientBrokerId, RecipientBrokerName,
    [Percent], RecipientType,
    CreationTime, IsDeleted
)
SELECT 
    CONCAT('CAR-', vr.RecipientNum) AS Id,
    vr.AssignmentVersionId,
    TRY_CAST(REPLACE(vr.RecipientBrokerId, 'P', '') AS BIGINT) AS RecipientBrokerId,
    COALESCE(rb.Name, CONCAT('Broker ', vr.RecipientBrokerId)) AS RecipientBrokerName,
    100.00 AS [Percent],
    1 AS RecipientType,  -- 1 = Broker
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM VersionRecipients vr
LEFT JOIN [dbo].[Brokers] rb ON rb.ExternalPartyId = vr.RecipientBrokerId;

DECLARE @recipientCount INT = @@ROWCOUNT;
PRINT CONCAT('  ✓ Inserted ', @recipientCount, ' commission assignment recipients');

-- Summary
PRINT '';
PRINT '=== Commission Assignments Complete ===';
PRINT CONCAT('  Versions:   ', @versionCount);
PRINT CONCAT('  Recipients: ', @recipientCount);
GO
