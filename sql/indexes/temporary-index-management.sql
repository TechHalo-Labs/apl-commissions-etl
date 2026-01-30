-- ============================================================================
-- Temporary Index Management for Bulk Operations
-- ============================================================================
-- Drop non-essential indexes before bulk INSERT operations
-- Recreate them after operations complete
-- This can significantly speed up bulk INSERTs
-- ============================================================================

USE $(PRODUCTION_DB);
GO

-- ============================================================================
-- DROP INDEXES (Run BEFORE bulk operations)
-- ============================================================================

PRINT 'Dropping indexes for bulk INSERT performance...';
GO

-- Hierarchies table - drop non-essential indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_GroupId')
BEGIN
    DROP INDEX IX_Hierarchies_GroupId ON dbo.Hierarchies;
    PRINT '✓ Dropped IX_Hierarchies_GroupId';
END

IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_BrokerId')
BEGIN
    DROP INDEX IX_Hierarchies_BrokerId ON dbo.Hierarchies;
    PRINT '✓ Dropped IX_Hierarchies_BrokerId';
END

IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_ProposalId')
BEGIN
    DROP INDEX IX_Hierarchies_ProposalId ON dbo.Hierarchies;
    PRINT '✓ Dropped IX_Hierarchies_ProposalId';
END

-- HierarchyParticipants table - drop non-essential indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.HierarchyParticipants') AND name = 'IX_HierarchyParticipants_EntityId')
BEGIN
    DROP INDEX IX_HierarchyParticipants_EntityId ON dbo.HierarchyParticipants;
    PRINT '✓ Dropped IX_HierarchyParticipants_EntityId';
END

-- StateRules table - drop non-essential indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.StateRules') AND name = 'IX_StateRules_HierarchyVersionId')
BEGIN
    DROP INDEX IX_StateRules_HierarchyVersionId ON dbo.StateRules;
    PRINT '✓ Dropped IX_StateRules_HierarchyVersionId';
END

-- HierarchySplits table - drop non-essential indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.HierarchySplits') AND name = 'IX_HierarchySplits_ProductCode')
BEGIN
    DROP INDEX IX_HierarchySplits_ProductCode ON dbo.HierarchySplits;
    PRINT '✓ Dropped IX_HierarchySplits_ProductCode';
END

-- SplitDistributions table - drop non-essential indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.SplitDistributions') AND name = 'IX_SplitDistributions_HierarchyParticipantId')
BEGIN
    DROP INDEX IX_SplitDistributions_HierarchyParticipantId ON dbo.SplitDistributions;
    PRINT '✓ Dropped IX_SplitDistributions_HierarchyParticipantId';
END

PRINT 'Indexes dropped. Ready for bulk operations.';
GO

-- ============================================================================
-- RECREATE INDEXES (Run AFTER bulk operations complete)
-- ============================================================================

PRINT 'Recreating indexes...';
GO

-- Hierarchies table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_GroupId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Hierarchies_GroupId ON dbo.Hierarchies (GroupId);
    PRINT '✓ Recreated IX_Hierarchies_GroupId';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_BrokerId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Hierarchies_BrokerId ON dbo.Hierarchies (BrokerId);
    PRINT '✓ Recreated IX_Hierarchies_BrokerId';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.Hierarchies') AND name = 'IX_Hierarchies_ProposalId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Hierarchies_ProposalId ON dbo.Hierarchies (ProposalId);
    PRINT '✓ Recreated IX_Hierarchies_ProposalId';
END

-- HierarchyParticipants table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.HierarchyParticipants') AND name = 'IX_HierarchyParticipants_EntityId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_HierarchyParticipants_EntityId ON dbo.HierarchyParticipants (EntityId);
    PRINT '✓ Recreated IX_HierarchyParticipants_EntityId';
END

-- StateRules table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.StateRules') AND name = 'IX_StateRules_HierarchyVersionId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_Hierarchies_GroupId ON dbo.StateRules (HierarchyVersionId);
    PRINT '✓ Recreated IX_StateRules_HierarchyVersionId';
END

-- HierarchySplits table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.HierarchySplits') AND name = 'IX_HierarchySplits_ProductCode')
BEGIN
    CREATE NONCLUSTERED INDEX IX_HierarchySplits_ProductCode ON dbo.HierarchySplits (ProductCode);
    PRINT '✓ Recreated IX_HierarchySplits_ProductCode';
END

-- SplitDistributions table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.SplitDistributions') AND name = 'IX_SplitDistributions_HierarchyParticipantId')
BEGIN
    CREATE NONCLUSTERED INDEX IX_SplitDistributions_HierarchyParticipantId ON dbo.SplitDistributions (HierarchyParticipantId);
    PRINT '✓ Recreated IX_SplitDistributions_HierarchyParticipantId';
END

PRINT 'Indexes recreated. Bulk operations complete.';
GO