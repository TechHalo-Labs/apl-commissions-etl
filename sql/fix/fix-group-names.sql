-- =============================================================================
-- One-Time Fix: Group Names from Source Data
-- Replaces generic "Group G12345" names with real company names
-- Run once to fix current production, then fixed by ETL for future data
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '============================================================';
PRINT 'ONE-TIME FIX: GROUP NAMES FROM SOURCE';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Check current state
-- =============================================================================
PRINT 'Current state:';

SELECT 
    COUNT(*) AS total_groups,
    SUM(CASE WHEN GroupName LIKE 'Group G%' OR GroupName LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS generic_names,
    SUM(CASE WHEN GroupName NOT LIKE 'Group G%' AND GroupName NOT LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS real_names
FROM [dbo].[EmployerGroups];

PRINT '';

-- =============================================================================
-- Backup current state
-- =============================================================================
PRINT 'Backing up current state...';

IF OBJECT_ID('new_data.EmployerGroups_names_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[EmployerGroups_names_backup_20260127];

SELECT * 
INTO [new_data].[EmployerGroups_names_backup_20260127]
FROM [dbo].[EmployerGroups];

PRINT 'EmployerGroups backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

-- =============================================================================
-- Update from new_data.stg_groups_backup (if exists)
-- =============================================================================
IF OBJECT_ID('new_data.stg_groups_backup', 'U') IS NOT NULL
BEGIN
    PRINT 'Updating GroupName from new_data.stg_groups_backup...';
    
    UPDATE g
    SET g.GroupName = b.Name,
        g.LastModificationTime = GETUTCDATE()
    FROM [dbo].[EmployerGroups] g
    INNER JOIN [new_data].[stg_groups_backup] b ON b.Id = g.Id
    WHERE g.GroupName != b.Name
      AND b.Name IS NOT NULL
      AND b.Name <> ''
      AND b.Name NOT LIKE 'Group G%'
      AND b.Name NOT LIKE 'Group [0-9]%';
    
    PRINT 'Groups updated from backup: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
ELSE
BEGIN
    PRINT 'new_data.stg_groups_backup not found, skipping backup update';
END

-- =============================================================================
-- Update from new_data.PerfGroupModel (primary source)
-- =============================================================================
PRINT '';
PRINT 'Updating GroupName from new_data.PerfGroupModel...';

UPDATE g
SET g.GroupName = LTRIM(RTRIM(pgm.GroupName)),
    g.LastModificationTime = GETUTCDATE()
FROM [dbo].[EmployerGroups] g
INNER JOIN [new_data].[PerfGroupModel] pgm 
    ON LTRIM(RTRIM(pgm.GroupNum)) = g.GroupNumber
WHERE (g.GroupName LIKE 'Group G%' OR g.GroupName LIKE 'Group [0-9]%' OR g.GroupName != pgm.GroupName)
  AND pgm.GroupName IS NOT NULL
  AND LTRIM(RTRIM(pgm.GroupName)) <> ''
  AND LTRIM(RTRIM(pgm.GroupName)) <> g.GroupNumber;  -- Don't use GroupNum as name

PRINT 'Groups updated from PerfGroupModel: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Update Proposals.GroupName to match
-- =============================================================================
PRINT '';
PRINT 'Updating Proposals.GroupName from EmployerGroups...';

UPDATE p
SET p.GroupName = g.GroupName,
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
INNER JOIN [dbo].[EmployerGroups] g ON g.Id = p.GroupId
WHERE p.GroupName != g.GroupName
  AND g.GroupName IS NOT NULL
  AND g.GroupName <> '';

PRINT 'Proposals updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    'After fix' AS status,
    COUNT(*) AS total_groups,
    SUM(CASE WHEN GroupName LIKE 'Group G%' OR GroupName LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS generic_names,
    SUM(CASE WHEN GroupName NOT LIKE 'Group G%' AND GroupName NOT LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) AS real_names,
    CAST(SUM(CASE WHEN GroupName NOT LIKE 'Group G%' AND GroupName NOT LIKE 'Group [0-9]%' THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS percent_real_names
FROM [dbo].[EmployerGroups];

-- Sample fixed groups
SELECT TOP 10
    Id,
    GroupNumber,
    GroupName
FROM [dbo].[EmployerGroups]
WHERE GroupName NOT LIKE 'Group G%'
  AND GroupName NOT LIKE 'Group [0-9]%'
ORDER BY Id;

PRINT '';
PRINT '============================================================';
PRINT 'GROUP NAMES FIX COMPLETED';
PRINT '============================================================';

GO
