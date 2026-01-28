-- =============================================================================
-- One-Time Fix: Production Status Values
-- Corrects Status fields on existing production data
-- Run once to fix current production, then fixed by ETL for future data
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '============================================================';
PRINT 'ONE-TIME FIX: PRODUCTION STATUS VALUES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Backup current state
-- =============================================================================
PRINT 'Step 1: Backing up current state to new_data schema...';

-- Backup Hierarchies
IF OBJECT_ID('new_data.Hierarchies_status_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[Hierarchies_status_backup_20260127];

SELECT * 
INTO [new_data].[Hierarchies_status_backup_20260127]
FROM [dbo].[Hierarchies];

PRINT 'Hierarchies backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Backup Proposals
IF OBJECT_ID('new_data.Proposals_status_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[Proposals_status_backup_20260127];

SELECT * 
INTO [new_data].[Proposals_status_backup_20260127]
FROM [dbo].[Proposals];

PRINT 'Proposals backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Backup Schedules
IF OBJECT_ID('new_data.Schedules_status_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[Schedules_status_backup_20260127];

SELECT * 
INTO [new_data].[Schedules_status_backup_20260127]
FROM [dbo].[Schedules];

PRINT 'Schedules backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);

PRINT '';

-- =============================================================================
-- Fix Hierarchies Status (0 → 1)
-- =============================================================================
PRINT 'Step 2: Fixing Hierarchies Status (0 → 1 Active)...';

UPDATE [dbo].[Hierarchies]
SET [Status] = 1,
    LastModificationTime = GETUTCDATE()
WHERE [Status] = 0;

PRINT 'Hierarchies updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Fix Proposals Status (0 → 2)
-- =============================================================================
PRINT '';
PRINT 'Step 3: Fixing Proposals Status (0 → 2 Approved)...';

UPDATE [dbo].[Proposals]
SET [Status] = 2,
    LastModificationTime = GETUTCDATE()
WHERE [Status] = 0;

PRINT 'Proposals updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Fix Schedules Status (0 → 1 or 'Active')
-- =============================================================================
PRINT '';
PRINT 'Step 4: Fixing Schedules Status...';

-- Check data type first
DECLARE @schedule_status_type NVARCHAR(50) = (
    SELECT DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
      AND TABLE_NAME = 'Schedules' 
      AND COLUMN_NAME = 'Status'
);

PRINT 'Schedule Status column type: ' + @schedule_status_type;

IF @schedule_status_type = 'int'
BEGIN
    UPDATE [dbo].[Schedules]
    SET [Status] = 1
    WHERE [Status] = 0;
    
    PRINT 'Schedules updated (int): ' + CAST(@@ROWCOUNT AS VARCHAR);
END
ELSE
BEGIN
    UPDATE [dbo].[Schedules]
    SET [Status] = 'Active'
    WHERE [Status] = '0' OR [Status] = 'Inactive' OR [Status] = 'inactive';
    
    PRINT 'Schedules updated (string): ' + CAST(@@ROWCOUNT AS VARCHAR);
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Hierarchies by Status' AS entity, [Status], COUNT(*) AS cnt
FROM [dbo].[Hierarchies]
GROUP BY [Status]
ORDER BY [Status];

SELECT 'Proposals by Status' AS entity, [Status], COUNT(*) AS cnt
FROM [dbo].[Proposals]
GROUP BY [Status]
ORDER BY [Status];

SELECT 'Schedules by Status' AS entity, [Status], COUNT(*) AS cnt
FROM [dbo].[Schedules]
GROUP BY [Status]
ORDER BY [Status];

PRINT '';
PRINT '============================================================';
PRINT 'STATUS FIX COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Backups created in new_data schema:';
PRINT '  - Hierarchies_status_backup_20260127';
PRINT '  - Proposals_status_backup_20260127';
PRINT '  - Schedules_status_backup_20260127';

GO
