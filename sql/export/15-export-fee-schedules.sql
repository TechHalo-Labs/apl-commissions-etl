-- ============================================================================
-- 15-export-fee-schedules.sql (SQL Server)
-- Export fees to FeeSchedules, FeeScheduleVersions, and FeeScheduleItems
-- ============================================================================
-- Uses stg_fee_schedules staging table
-- Creates one FeeSchedule per Proposal (for groups with fees)
-- Creates one Active version per schedule with fee items
-- ============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: FEE SCHEDULES';
PRINT '============================================================';
PRINT '';

-- ============================================================================
-- Step 1: Identify groups with fees and get earliest effective date
-- ============================================================================
PRINT 'Step 1: Identifying groups with fees...';

IF OBJECT_ID('tempdb..#GroupFees') IS NOT NULL DROP TABLE #GroupFees;

SELECT 
    f.GroupId,
    COALESCE(g.GroupName, f.GroupName) AS GroupName,
    MIN(f.EffectiveDate) AS EarliestEffectiveDate,
    COUNT(*) AS FeeCount
INTO #GroupFees
FROM [etl].[stg_fee_schedules] f
LEFT JOIN [dbo].[Group] g ON g.Id = f.GroupId
WHERE f.GroupId IS NOT NULL
GROUP BY f.GroupId, COALESCE(g.GroupName, f.GroupName);

DECLARE @groupCount INT = @@ROWCOUNT;
PRINT '  Found ' + CAST(@groupCount AS VARCHAR) + ' groups with fees';

GO

-- ============================================================================
-- Step 2: Get proposals for groups with fees
-- ============================================================================
PRINT '';
PRINT 'Step 2: Identifying proposals for groups with fees...';

IF OBJECT_ID('tempdb..#ProposalsWithFees') IS NOT NULL DROP TABLE #ProposalsWithFees;

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    gf.GroupId,
    gf.GroupName,
    gf.EarliestEffectiveDate,
    gf.FeeCount
INTO #ProposalsWithFees
FROM [dbo].[Proposals] p
INNER JOIN #GroupFees gf ON gf.GroupId = p.GroupId
WHERE p.GroupId IS NOT NULL;

DECLARE @proposalCount INT = @@ROWCOUNT;
PRINT '  Found ' + CAST(@proposalCount AS VARCHAR) + ' proposals for groups with fees';

GO

-- ============================================================================
-- Step 3: Create FeeSchedules (one per proposal)
-- ============================================================================
PRINT '';
PRINT 'Step 3: Creating FeeSchedules...';

-- Insert new fee schedules (skip existing)
INSERT INTO [dbo].[FeeSchedules] (Id, [Name], [Description], ProposalId, CreationTime, IsDeleted)
SELECT 
    CONCAT('FS-', pwf.ProposalId) AS Id,
    CONCAT('Fee Schedule - ', pwf.ProposalNumber) AS [Name],
    CONCAT('Fee schedule for proposal ', pwf.ProposalNumber, ' (Group: ', pwf.GroupName, ')') AS [Description],
    pwf.ProposalId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #ProposalsWithFees pwf
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[FeeSchedules] fs 
    WHERE fs.Id = CONCAT('FS-', pwf.ProposalId)
);

DECLARE @schedulesCreated INT = @@ROWCOUNT;
PRINT '  Created ' + CAST(@schedulesCreated AS VARCHAR) + ' new FeeSchedules';

GO

-- ============================================================================
-- Step 4: Create FeeScheduleVersions (v1.0, Active)
-- ============================================================================
PRINT '';
PRINT 'Step 4: Creating FeeScheduleVersions...';

-- Insert new versions (skip existing)
INSERT INTO [dbo].[FeeScheduleVersions] (Id, FeeScheduleId, VersionNumber, [Status], EffectiveDate, EndDate, ChangeReason, Content, CreationTime, IsDeleted)
SELECT 
    CONCAT('FSV-', pwf.ProposalId, '-1.0') AS Id,
    CONCAT('FS-', pwf.ProposalId) AS FeeScheduleId,
    '1.0' AS VersionNumber,
    1 AS [Status],  -- Active
    pwf.EarliestEffectiveDate AS EffectiveDate,
    NULL AS EndDate,
    'Initial fee schedule from ETL migration' AS ChangeReason,
    NULL AS Content,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #ProposalsWithFees pwf
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[FeeScheduleVersions] fsv 
    WHERE fsv.Id = CONCAT('FSV-', pwf.ProposalId, '-1.0')
);

DECLARE @versionsCreated INT = @@ROWCOUNT;
PRINT '  Created ' + CAST(@versionsCreated AS VARCHAR) + ' new FeeScheduleVersions';

GO

-- ============================================================================
-- Step 5: Create FeeScheduleItems (clone fees per proposal)
-- ============================================================================
PRINT '';
PRINT 'Step 5: Creating FeeScheduleItems...';

-- stg_fee_schedules has: Id, GroupId, GroupName, FeeType, Amount, EffectiveDate, EndDate, Description, CreationTime, IsDeleted
-- Production FeeScheduleItems has: Id, FeeScheduleVersionId, FeeTypeId, FeeTypeCode, FeeTypeName, Name, Frequency, Basis, Amount, Percent, Notes, DisplayOrder, EffectiveFrom, EffectiveTo, RecipientBrokerId, RecipientBrokerName, CreationTime, IsDeleted

INSERT INTO [dbo].[FeeScheduleItems] (
    Id, FeeScheduleVersionId, FeeTypeId, FeeTypeCode, FeeTypeName, 
    [Name], Frequency, Basis, Amount, [Percent], Notes, DisplayOrder,
    EffectiveFrom, EffectiveTo, RecipientBrokerId, RecipientBrokerName,
    CreationTime, IsDeleted
)
SELECT 
    CONCAT(f.Id, '-P-', pwf.ProposalId) AS Id,
    CONCAT('FSV-', pwf.ProposalId, '-1.0') AS FeeScheduleVersionId,
    NULL AS FeeTypeId,
    f.FeeType AS FeeTypeCode,
    f.FeeType AS FeeTypeName,
    COALESCE(f.[Description], f.FeeType) AS [Name],
    'Monthly' AS Frequency,
    'Flat' AS Basis,
    f.Amount,
    NULL AS [Percent],
    f.[Description] AS Notes,
    ROW_NUMBER() OVER (
        PARTITION BY pwf.ProposalId 
        ORDER BY f.EffectiveDate, f.FeeType, f.Id
    ) AS DisplayOrder,
    f.EffectiveDate AS EffectiveFrom,
    f.EndDate AS EffectiveTo,
    NULL AS RecipientBrokerId,
    NULL AS RecipientBrokerName,
    COALESCE(f.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(f.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_fee_schedules] f
INNER JOIN #ProposalsWithFees pwf ON pwf.GroupId = f.GroupId
WHERE f.GroupId IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM [dbo].[FeeScheduleItems] fsi 
      WHERE fsi.Id = CONCAT(f.Id, '-P-', pwf.ProposalId)
  );

DECLARE @itemsCreated INT = @@ROWCOUNT;
PRINT '  Created ' + CAST(@itemsCreated AS VARCHAR) + ' new FeeScheduleItems';

GO

-- ============================================================================
-- Step 6: Summary
-- ============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'FEE SCHEDULES EXPORT SUMMARY';
PRINT '============================================================';
PRINT '';

SELECT 
    'FeeSchedules (total)' AS Entity,
    COUNT(*) AS [Count]
FROM [dbo].[FeeSchedules]

UNION ALL

SELECT 
    'FeeScheduleVersions (total)' AS Entity,
    COUNT(*) AS [Count]
FROM [dbo].[FeeScheduleVersions]

UNION ALL

SELECT 
    'FeeScheduleVersions (Active)' AS Entity,
    COUNT(*) AS [Count]
FROM [dbo].[FeeScheduleVersions]
WHERE [Status] = 1

UNION ALL

SELECT 
    'FeeScheduleItems (total)' AS Entity,
    COUNT(*) AS [Count]
FROM [dbo].[FeeScheduleItems]

UNION ALL

SELECT 
    'Proposals with FeeSchedules' AS Entity,
    COUNT(DISTINCT p.Id) AS [Count]
FROM [dbo].[Proposals] p
INNER JOIN [dbo].[FeeSchedules] fs ON fs.ProposalId = p.Id;

PRINT '';
PRINT '============================================================';
PRINT 'FEE SCHEDULES EXPORT COMPLETE';
PRINT '============================================================';

GO
