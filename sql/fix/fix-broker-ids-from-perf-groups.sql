-- =============================================================================
-- One-Time Fix: Populate Broker IDs from poc_etl.raw_perf_groups
-- Fixes PrimaryBrokerId on Groups and BrokerUniquePartyId on Proposals
-- Run once to fix current production, then fixed by ETL for future data
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '============================================================';
PRINT 'ONE-TIME FIX: BROKER IDS FROM raw_perf_groups';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Pre-fix verification
-- =============================================================================
PRINT 'Current state:';

SELECT 
    'Groups PrimaryBrokerId' AS metric,
    COUNT(*) AS total,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL AND PrimaryBrokerId != 0 THEN 1 ELSE 0 END) AS with_broker,
    SUM(CASE WHEN PrimaryBrokerId IS NULL OR PrimaryBrokerId = 0 THEN 1 ELSE 0 END) AS without_broker
FROM [dbo].[EmployerGroups]
WHERE Id <> 'G00000';

SELECT 
    'Proposals BrokerUniquePartyId' AS metric,
    COUNT(*) AS total,
    SUM(CASE WHEN BrokerUniquePartyId IS NOT NULL AND BrokerUniquePartyId <> '' THEN 1 ELSE 0 END) AS with_id,
    SUM(CASE WHEN BrokerUniquePartyId IS NULL OR BrokerUniquePartyId = '' THEN 1 ELSE 0 END) AS without_id
FROM [dbo].[Proposals];

PRINT '';

-- =============================================================================
-- Backup current state
-- =============================================================================
PRINT 'Backing up current state...';

IF OBJECT_ID('new_data.EmployerGroups_perf_backup_20260128', 'U') IS NOT NULL
    DROP TABLE [new_data].[EmployerGroups_perf_backup_20260128];

SELECT * 
INTO [new_data].[EmployerGroups_perf_backup_20260128]
FROM [dbo].[EmployerGroups];

PRINT 'EmployerGroups backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);

IF OBJECT_ID('new_data.Proposals_perf_backup_20260128', 'U') IS NOT NULL
    DROP TABLE [new_data].[Proposals_perf_backup_20260128];

SELECT * 
INTO [new_data].[Proposals_perf_backup_20260128]
FROM [dbo].[Proposals];

PRINT 'Proposals backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

-- =============================================================================
-- Step 1: Populate PrimaryBrokerId on Groups from poc_etl.raw_perf_groups
-- =============================================================================
PRINT 'Step 1: Populating PrimaryBrokerId on Groups...';

UPDATE g
SET g.PrimaryBrokerId = (
    SELECT TOP 1 b.Id 
    FROM [dbo].[Brokers] b 
    INNER JOIN [poc_etl].[raw_perf_groups] rpg 
        ON b.ExternalPartyId = LTRIM(RTRIM(rpg.BrokerUniqueId))
    WHERE LTRIM(RTRIM(rpg.GroupNum)) = g.GroupNumber
      AND rpg.BrokerUniqueId IS NOT NULL
      AND LTRIM(RTRIM(rpg.BrokerUniqueId)) <> ''
),
    g.LastModificationTime = GETUTCDATE()
FROM [dbo].[EmployerGroups] g
WHERE (g.PrimaryBrokerId IS NULL OR g.PrimaryBrokerId = 0)
  AND g.Id <> 'G00000';

PRINT 'Groups updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 2: Populate BrokerUniquePartyId on Proposals from poc_etl.raw_perf_groups
-- =============================================================================
PRINT '';
PRINT 'Step 2: Populating BrokerUniquePartyId on Proposals...';

UPDATE p
SET p.BrokerUniquePartyId = (
    SELECT TOP 1 LTRIM(RTRIM(rpg.BrokerUniqueId))
    FROM [poc_etl].[raw_perf_groups] rpg
    WHERE CONCAT('G', LTRIM(RTRIM(rpg.GroupNum))) = p.GroupId
      AND rpg.BrokerUniqueId IS NOT NULL
      AND LTRIM(RTRIM(rpg.BrokerUniqueId)) <> ''
),
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
WHERE (p.BrokerUniquePartyId IS NULL OR p.BrokerUniquePartyId = '');

PRINT 'BrokerUniquePartyId populated on Proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Populate BrokerId on Proposals from BrokerUniquePartyId
-- =============================================================================
PRINT '';
PRINT 'Step 3: Populating BrokerId on Proposals from BrokerUniquePartyId...';

UPDATE p
SET p.BrokerId = b.Id,
    p.BrokerName = b.Name,
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
INNER JOIN [dbo].[Brokers] b ON b.ExternalPartyId = p.BrokerUniquePartyId
WHERE p.BrokerUniquePartyId IS NOT NULL
  AND p.BrokerUniquePartyId <> ''
  AND (p.BrokerId IS NULL OR p.BrokerId = 0);

PRINT 'BrokerId populated on Proposals: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    'Groups PrimaryBrokerId' AS metric,
    COUNT(*) AS total,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL AND PrimaryBrokerId != 0 THEN 1 ELSE 0 END) AS with_broker,
    SUM(CASE WHEN PrimaryBrokerId IS NULL OR PrimaryBrokerId = 0 THEN 1 ELSE 0 END) AS without_broker,
    CAST(SUM(CASE WHEN PrimaryBrokerId IS NOT NULL AND PrimaryBrokerId != 0 THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct
FROM [dbo].[EmployerGroups]
WHERE Id <> 'G00000';

SELECT 
    'Proposals BrokerUniquePartyId' AS metric,
    COUNT(*) AS total,
    SUM(CASE WHEN BrokerUniquePartyId IS NOT NULL AND BrokerUniquePartyId <> '' THEN 1 ELSE 0 END) AS with_id,
    SUM(CASE WHEN BrokerUniquePartyId IS NULL OR BrokerUniquePartyId = '' THEN 1 ELSE 0 END) AS without_id,
    CAST(SUM(CASE WHEN BrokerUniquePartyId IS NOT NULL AND BrokerUniquePartyId <> '' THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct
FROM [dbo].[Proposals];

SELECT 
    'Proposals BrokerId' AS metric,
    COUNT(*) AS total,
    SUM(CASE WHEN BrokerId IS NOT NULL AND BrokerId != 0 THEN 1 ELSE 0 END) AS with_broker,
    SUM(CASE WHEN BrokerId IS NULL OR BrokerId = 0 THEN 1 ELSE 0 END) AS without_broker,
    CAST(SUM(CASE WHEN BrokerId IS NOT NULL AND BrokerId != 0 THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS pct
FROM [dbo].[Proposals];

-- Sample fixed records
PRINT '';
PRINT 'Sample Groups with PrimaryBrokerId:';
SELECT TOP 5
    g.Id,
    g.GroupNumber,
    g.GroupName,
    g.PrimaryBrokerId,
    b.Name AS PrimaryBrokerName,
    b.ExternalPartyId
FROM [dbo].[EmployerGroups] g
INNER JOIN [dbo].[Brokers] b ON b.Id = g.PrimaryBrokerId
WHERE g.PrimaryBrokerId IS NOT NULL
ORDER BY g.Id;

PRINT '';
PRINT 'Sample Proposals with BrokerUniquePartyId:';
SELECT TOP 5
    p.Id,
    p.ProposalNumber,
    p.GroupName,
    p.BrokerId,
    p.BrokerUniquePartyId,
    p.BrokerName
FROM [dbo].[Proposals] p
WHERE p.BrokerUniquePartyId IS NOT NULL AND p.BrokerUniquePartyId <> ''
ORDER BY p.Id;

PRINT '';
PRINT '============================================================';
PRINT 'BROKER IDS FROM PERF GROUPS FIX COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Backups created:';
PRINT '  - new_data.EmployerGroups_perf_backup_20260128';
PRINT '  - new_data.Proposals_perf_backup_20260128';

GO
