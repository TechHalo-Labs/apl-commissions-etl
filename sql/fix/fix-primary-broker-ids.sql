-- =============================================================================
-- One-Time Fix: Populate PrimaryBrokerId on EmployerGroups
-- Sets PrimaryBrokerId from Proposals.BrokerId for existing groups
-- Run once to fix current production, then fixed by ETL for future data
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '============================================================';
PRINT 'ONE-TIME FIX: PRIMARY BROKER IDS ON GROUPS';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Backup current state
-- =============================================================================
PRINT 'Step 1: Backing up current state...';

IF OBJECT_ID('new_data.EmployerGroups_broker_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[EmployerGroups_broker_backup_20260127];

SELECT * 
INTO [new_data].[EmployerGroups_broker_backup_20260127]
FROM [dbo].[EmployerGroups];

PRINT 'EmployerGroups backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

-- =============================================================================
-- Populate PrimaryBrokerId from first proposal for each group
-- =============================================================================
PRINT 'Step 2: Setting PrimaryBrokerId from Proposals...';

UPDATE g
SET g.PrimaryBrokerId = p.BrokerId,
    g.LastModificationTime = GETUTCDATE()
FROM [dbo].[EmployerGroups] g
INNER JOIN (
    SELECT 
        GroupId,
        MIN(Id) AS FirstProposalId
    FROM [dbo].[Proposals]
    WHERE BrokerId IS NOT NULL AND BrokerId != 0
    GROUP BY GroupId
) first_prop ON first_prop.GroupId = g.Id
INNER JOIN [dbo].[Proposals] p ON p.Id = first_prop.FirstProposalId
WHERE (g.PrimaryBrokerId IS NULL OR g.PrimaryBrokerId = 0);

PRINT 'Groups updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    'PrimaryBrokerId coverage' AS metric,
    COUNT(*) AS total_groups,
    SUM(CASE WHEN PrimaryBrokerId IS NOT NULL AND PrimaryBrokerId != 0 THEN 1 ELSE 0 END) AS with_broker,
    SUM(CASE WHEN PrimaryBrokerId IS NULL OR PrimaryBrokerId = 0 THEN 1 ELSE 0 END) AS without_broker,
    CAST(SUM(CASE WHEN PrimaryBrokerId IS NOT NULL AND PrimaryBrokerId != 0 THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS percent_coverage
FROM [dbo].[EmployerGroups]
WHERE Id <> 'G00000';  -- Exclude DTC sentinel

-- Sample groups with PrimaryBrokerId
SELECT TOP 10
    g.Id,
    g.GroupNumber,
    g.GroupName,
    g.PrimaryBrokerId,
    b.Name AS PrimaryBrokerName
FROM [dbo].[EmployerGroups] g
LEFT JOIN [dbo].[Brokers] b ON b.Id = g.PrimaryBrokerId
WHERE g.PrimaryBrokerId IS NOT NULL
ORDER BY g.Id;

PRINT '';
PRINT '============================================================';
PRINT 'PRIMARY BROKER ID FIX COMPLETED';
PRINT '============================================================';

GO
