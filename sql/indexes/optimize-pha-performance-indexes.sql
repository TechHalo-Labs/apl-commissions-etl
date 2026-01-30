-- ============================================================================
-- Index Optimization for PHA Script Performance
-- ============================================================================
-- These indexes will significantly speed up the PHA script operations
-- Run before executing fix-policy-hierarchy-assignments.ts
-- ============================================================================

USE $(PRODUCTION_DB);
GO

PRINT 'Creating performance indexes for PHA operations...';
GO

-- ============================================================================
-- 1. Schedules Table - External ID lookups
-- ============================================================================
-- Query: SELECT Id, ExternalId FROM dbo.Schedules WHERE ExternalId IS NOT NULL
-- Current: Table scan (51K rows)
-- Benefit: ~100x faster lookups
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Schedules') AND name = 'IX_Schedules_ExternalId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Schedules_ExternalId
    ON dbo.Schedules (ExternalId)
    WHERE ExternalId IS NOT NULL;  -- Filtered index for non-null values only

    PRINT '✓ Created IX_Schedules_ExternalId (filtered)';
END
ELSE
BEGIN
    PRINT '✓ IX_Schedules_ExternalId already exists';
END
GO

-- ============================================================================
-- 2. Policies Table - Bulk ID lookups
-- ============================================================================
-- Query: SELECT ... FROM dbo.Policies WHERE Id IN (large list)
-- Current: Uses PK but IN with many values is slow
-- Benefit: Better for large IN clauses
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Policies') AND name = 'IX_Policies_Id_Includes')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Policies_Id_Includes
    ON dbo.Policies (Id)
    INCLUDE (GroupId, ProductCode, State, EffectiveDate, Premium);  -- Cover all PHA-needed columns

    PRINT '✓ Created IX_Policies_Id_Includes (covering index)';
END
ELSE
BEGIN
    PRINT '✓ IX_Policies_Id_Includes already exists';
END
GO

-- ============================================================================
-- 3. Production Tables - INSERT Performance
-- ============================================================================
-- For INSERT operations, we may want to temporarily drop some indexes
-- during bulk operations and recreate them after.
-- These are the indexes that would slow down INSERTs:

-- Check for indexes that might slow down bulk inserts
SELECT
    t.name AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
        ORDER BY ic.key_ordinal
        FOR XML PATH('')
    ), 1, 2, '') AS IndexColumns
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE t.name IN ('Hierarchies', 'HierarchyVersions', 'HierarchyParticipants', 'StateRules', 'HierarchySplits', 'SplitDistributions')
    AND i.type_desc != 'CLUSTERED'
    AND i.is_primary_key = 0
ORDER BY t.name, i.name;

PRINT 'Note: Consider dropping non-essential indexes during bulk INSERT operations';
PRINT 'and recreating them after the script completes.';
GO

-- ============================================================================
-- Performance Monitoring Queries
-- ============================================================================

PRINT 'Performance monitoring - run these before/after adding indexes:';
GO

-- Check Schedules query performance
PRINT 'Schedules lookup performance:';
SELECT COUNT(*) as TotalSchedules
FROM dbo.Schedules;

SELECT COUNT(*) as SchedulesWithExternalId
FROM dbo.Schedules
WHERE ExternalId IS NOT NULL;

-- Check Policies query performance
PRINT 'Policies lookup performance:';
SELECT COUNT(*) as TotalPolicies
FROM dbo.Policies;

-- Check current execution plan for Schedules query (run manually)
PRINT 'To check execution plans, run:';
PRINT 'SET SHOWPLAN_ALL ON;';
PRINT 'SELECT Id, ExternalId FROM dbo.Schedules WHERE ExternalId IS NOT NULL;';
PRINT 'SET SHOWPLAN_ALL OFF;';

PRINT 'Index optimization complete!';
GO