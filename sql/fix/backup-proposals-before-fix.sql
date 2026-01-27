-- =============================================================================
-- Backup Proposals Data to new_data Schema (Before Fix)
-- 
-- This script creates a backup of proposal data before applying the
-- EffectiveDateFrom fix, enabling before/after analysis and verification.
--
-- Backup Strategy:
-- 1. Create backup tables in new_data schema if they don't exist
-- 2. Copy current proposal data (including EffectiveDateFrom)
-- 3. Copy related policy data for comparison
-- 4. Add timestamp for tracking
-- =============================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

DECLARE @backup_timestamp NVARCHAR(50) = FORMAT(GETUTCDATE(), 'yyyyMMdd_HHmmss');
DECLARE @backup_suffix NVARCHAR(50) = '_before_fix_' + @backup_timestamp;

PRINT '============================================================';
PRINT 'BACKUP: Proposals Data Before EffectiveDateFrom Fix';
PRINT '============================================================';
PRINT 'Backup Timestamp: ' + @backup_timestamp;
PRINT '';

-- =============================================================================
-- Step 1: Create backup tables in new_data schema
-- =============================================================================
PRINT 'Step 1: Creating backup tables in new_data schema...';

-- Backup Proposals table
DECLARE @proposals_backup_table NVARCHAR(200) = '[new_data].[Proposals' + @backup_suffix + ']';
DECLARE @sql NVARCHAR(MAX);

IF OBJECT_ID(@proposals_backup_table, 'U') IS NOT NULL
BEGIN
    SET @sql = 'DROP TABLE ' + @proposals_backup_table;
    EXEC sp_executesql @sql;
END

SET @sql = '
SELECT 
    p.*,
    GETUTCDATE() AS BackupTimestamp,
    ''Before EffectiveDateFrom Fix'' AS BackupReason
INTO ' + @proposals_backup_table + '
FROM [dbo].[Proposals] p';

EXEC sp_executesql @sql;

DECLARE @proposal_backup_count INT = @@ROWCOUNT;
PRINT 'Backed up ' + CAST(@proposal_backup_count AS VARCHAR) + ' proposals';
PRINT '';

-- Backup Policies table (for before/after comparison)
DECLARE @policies_backup_table NVARCHAR(200) = '[new_data].[Policies' + @backup_suffix + ']';

IF OBJECT_ID(@policies_backup_table, 'U') IS NOT NULL
BEGIN
    SET @sql = 'DROP TABLE ' + @policies_backup_table;
    EXEC sp_executesql @sql;
END

SET @sql = '
SELECT 
    pol.*,
    GETUTCDATE() AS BackupTimestamp,
    ''Before EffectiveDateFrom Fix'' AS BackupReason
INTO ' + @policies_backup_table + '
FROM [dbo].[Policies] pol
WHERE pol.ProposalId IS NOT NULL';

EXEC sp_executesql @sql;

DECLARE @policy_backup_count INT = @@ROWCOUNT;
PRINT 'Backed up ' + CAST(@policy_backup_count AS VARCHAR) + ' policies';
PRINT '';

-- Backup ProposalProducts (for completeness)
DECLARE @products_backup_table NVARCHAR(200) = '[new_data].[ProposalProducts' + @backup_suffix + ']';

IF OBJECT_ID(@products_backup_table, 'U') IS NOT NULL
BEGIN
    SET @sql = 'DROP TABLE ' + @products_backup_table;
    EXEC sp_executesql @sql;
END

SET @sql = '
SELECT 
    pp.*,
    GETUTCDATE() AS BackupTimestamp,
    ''Before EffectiveDateFrom Fix'' AS BackupReason
INTO ' + @products_backup_table + '
FROM [dbo].[ProposalProducts] pp';

EXEC sp_executesql @sql;

DECLARE @product_backup_count INT = @@ROWCOUNT;
PRINT 'Backed up ' + CAST(@product_backup_count AS VARCHAR) + ' proposal products';
PRINT '';

-- =============================================================================
-- Step 2: Create a summary table for easy comparison
-- =============================================================================
PRINT 'Step 2: Creating summary table for comparison...';

DECLARE @summary_backup_table NVARCHAR(200) = '[new_data].[ProposalFixSummary' + @backup_suffix + ']';

IF OBJECT_ID(@summary_backup_table, 'U') IS NOT NULL
BEGIN
    SET @sql = 'DROP TABLE ' + @summary_backup_table;
    EXEC sp_executesql @sql;
END

-- First create temp table with summary data
DROP TABLE IF EXISTS #proposal_summary;

SELECT 
    p.Id AS ProposalId,
    p.ProposalNumber,
    p.GroupId,
    p.GroupName,
    p.EffectiveDateFrom AS CurrentEffectiveDateFrom,
    p.EffectiveDateTo AS CurrentEffectiveDateTo,
    p.ProposedEffectiveDate,
    -- Calculate true minimum from policies
    (SELECT MIN(pol.EffectiveDate)
     FROM [dbo].[Policies] pol
     WHERE pol.GroupId = p.GroupId
       AND pol.EffectiveDate IS NOT NULL) AS TrueMinEffectiveDate,
    -- Count policies that will be fixed
    (SELECT COUNT(*)
     FROM [dbo].[Policies] pol
     WHERE pol.GroupId = p.GroupId
       AND pol.EffectiveDate < p.EffectiveDateFrom
       AND pol.ProposalId = p.Id) AS AffectedPolicyCount,
    -- Count total policies for this proposal
    (SELECT COUNT(*)
     FROM [dbo].[Policies] pol
     WHERE pol.ProposalId = p.Id) AS TotalPolicyCount,
    GETUTCDATE() AS BackupTimestamp,
    'Before EffectiveDateFrom Fix' AS BackupReason
INTO #proposal_summary
FROM [dbo].[Proposals] p
WHERE p.EffectiveDateFrom IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM [dbo].[Policies] pol
      WHERE pol.GroupId = p.GroupId
        AND pol.EffectiveDate < p.EffectiveDateFrom
        AND pol.ProposalId = p.Id
  );

SET @sql = '
SELECT * INTO ' + @summary_backup_table + ' FROM #proposal_summary';

EXEC sp_executesql @sql;

DECLARE @summary_count INT = @@ROWCOUNT;
PRINT 'Created summary for ' + CAST(@summary_count AS VARCHAR) + ' proposals that need fixing';
PRINT '';

-- =============================================================================
-- Step 3: Create indexes on backup tables for efficient querying
-- =============================================================================
PRINT 'Step 3: Creating indexes on backup tables...';

-- Index on ProposalId for quick lookups
SET @sql = '
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = ''IX_Proposals_Backup_Id'' AND object_id = OBJECT_ID(''' + @proposals_backup_table + '''))
BEGIN
    CREATE INDEX IX_Proposals_Backup_Id ON ' + @proposals_backup_table + ' (Id);
END';
EXEC sp_executesql @sql;
PRINT 'Created index on Proposals backup table';

SET @sql = '
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = ''IX_Policies_Backup_ProposalId'' AND object_id = OBJECT_ID(''' + @policies_backup_table + '''))
BEGIN
    CREATE INDEX IX_Policies_Backup_ProposalId ON ' + @policies_backup_table + ' (ProposalId);
END';
EXEC sp_executesql @sql;
PRINT 'Created index on Policies backup table';

SET @sql = '
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = ''IX_Summary_Backup_ProposalId'' AND object_id = OBJECT_ID(''' + @summary_backup_table + '''))
BEGIN
    CREATE INDEX IX_Summary_Backup_ProposalId ON ' + @summary_backup_table + ' (ProposalId);
END';
EXEC sp_executesql @sql;
PRINT 'Created index on Summary backup table';

PRINT '';

-- =============================================================================
-- Step 4: Verification and Summary
-- =============================================================================
PRINT 'Step 4: Verification...';
PRINT '';

SET @sql = '
SELECT 
    ''Proposals'' AS TableName,
    COUNT(*) AS RecordCount,
    ''' + @proposals_backup_table + ''' AS BackupTableName
FROM ' + @proposals_backup_table + '

UNION ALL

SELECT 
    ''Policies'' AS TableName,
    COUNT(*) AS RecordCount,
    ''' + @policies_backup_table + ''' AS BackupTableName
FROM ' + @policies_backup_table + '

UNION ALL

SELECT 
    ''ProposalProducts'' AS TableName,
    COUNT(*) AS RecordCount,
    ''' + @products_backup_table + ''' AS BackupTableName
FROM ' + @products_backup_table + '

UNION ALL

SELECT 
    ''ProposalFixSummary'' AS TableName,
    COUNT(*) AS RecordCount,
    ''' + @summary_backup_table + ''' AS BackupTableName
FROM ' + @summary_backup_table;

EXEC sp_executesql @sql;

PRINT '';
PRINT '============================================================';
PRINT 'BACKUP COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'Backup Tables Created:';
PRINT '  - ' + @proposals_backup_table;
PRINT '  - ' + @policies_backup_table;
PRINT '  - ' + @products_backup_table;
PRINT '  - ' + @summary_backup_table;
PRINT '';
PRINT 'To compare before/after:';
PRINT '  SELECT * FROM ' + @summary_backup_table;
PRINT '  WHERE ProposalId IN (''P-GLA2003-C2'', ''P-GLA4033-C2'')';
PRINT '';
PRINT 'To restore (if needed):';
PRINT '  UPDATE p SET p.EffectiveDateFrom = b.EffectiveDateFrom';
PRINT '  FROM [dbo].[Proposals] p';
PRINT '  INNER JOIN ' + @proposals_backup_table + ' b ON b.Id = p.Id';
PRINT '';
PRINT 'Backup timestamp: ' + @backup_timestamp;
PRINT '';

GO
