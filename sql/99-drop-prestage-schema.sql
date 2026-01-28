-- =============================================================================
-- Drop Pre-Stage Schema
-- =============================================================================
-- Use this script to drop the prestage schema after ETL has been fully audited
-- and you're ready to move to production.
--
-- IMPORTANT: This is a DESTRUCTIVE operation. The prestage schema contains
-- the complete audit trail of proposal consolidation. Once dropped, you cannot
-- recover the unconsolidated proposal data.
--
-- Usage: sqlcmd -S server -d database -i 99-drop-prestage-schema.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'DROPPING PRE-STAGE SCHEMA';
PRINT '============================================================';
PRINT '';
PRINT '⚠️  WARNING: This will permanently delete consolidation audit data';
PRINT '';

-- =============================================================================
-- Verification: Show what will be deleted
-- =============================================================================
PRINT 'Current pre-stage data counts:';
PRINT '';

IF OBJECT_ID('prestage.prestage_proposals', 'U') IS NOT NULL
BEGIN
    DECLARE @proposals INT = (SELECT COUNT(*) FROM prestage.prestage_proposals);
    DECLARE @retained INT = (SELECT COUNT(*) FROM prestage.prestage_proposals WHERE IsRetained = 1);
    DECLARE @consumed INT = (SELECT COUNT(*) FROM prestage.prestage_proposals WHERE ConsumedByProposalId IS NOT NULL);
    
    PRINT '  prestage_proposals: ' + CAST(@proposals AS VARCHAR);
    PRINT '    - Retained: ' + CAST(@retained AS VARCHAR);
    PRINT '    - Consumed: ' + CAST(@consumed AS VARCHAR);
END

IF OBJECT_ID('prestage.prestage_hierarchies', 'U') IS NOT NULL
BEGIN
    DECLARE @hierarchies INT = (SELECT COUNT(*) FROM prestage.prestage_hierarchies);
    PRINT '  prestage_hierarchies: ' + CAST(@hierarchies AS VARCHAR);
END

IF OBJECT_ID('prestage.prestage_premium_split_versions', 'U') IS NOT NULL
BEGIN
    DECLARE @split_versions INT = (SELECT COUNT(*) FROM prestage.prestage_premium_split_versions);
    PRINT '  prestage_premium_split_versions: ' + CAST(@split_versions AS VARCHAR);
END

IF OBJECT_ID('prestage.prestage_premium_split_participants', 'U') IS NOT NULL
BEGIN
    DECLARE @split_participants INT = (SELECT COUNT(*) FROM prestage.prestage_premium_split_participants);
    PRINT '  prestage_premium_split_participants: ' + CAST(@split_participants AS VARCHAR);
END

IF OBJECT_ID('prestage.prestage_hierarchy_versions', 'U') IS NOT NULL
BEGIN
    DECLARE @hierarchy_versions INT = (SELECT COUNT(*) FROM prestage.prestage_hierarchy_versions);
    PRINT '  prestage_hierarchy_versions: ' + CAST(@hierarchy_versions AS VARCHAR);
END

IF OBJECT_ID('prestage.prestage_hierarchy_participants', 'U') IS NOT NULL
BEGIN
    DECLARE @hierarchy_participants INT = (SELECT COUNT(*) FROM prestage.prestage_hierarchy_participants);
    PRINT '  prestage_hierarchy_participants: ' + CAST(@hierarchy_participants AS VARCHAR);
END

-- =============================================================================
-- Drop Tables (in reverse dependency order)
-- =============================================================================
PRINT '';
PRINT 'Dropping prestage tables...';

DROP TABLE IF EXISTS [prestage].[prestage_premium_split_participants];
PRINT '  ✓ Dropped prestage_premium_split_participants';

DROP TABLE IF EXISTS [prestage].[prestage_premium_split_versions];
PRINT '  ✓ Dropped prestage_premium_split_versions';

DROP TABLE IF EXISTS [prestage].[prestage_hierarchy_participants];
PRINT '  ✓ Dropped prestage_hierarchy_participants';

DROP TABLE IF EXISTS [prestage].[prestage_hierarchy_versions];
PRINT '  ✓ Dropped prestage_hierarchy_versions';

DROP TABLE IF EXISTS [prestage].[prestage_hierarchies];
PRINT '  ✓ Dropped prestage_hierarchies';

DROP TABLE IF EXISTS [prestage].[prestage_proposals];
PRINT '  ✓ Dropped prestage_proposals';

-- =============================================================================
-- Drop Schema
-- =============================================================================
PRINT '';
PRINT 'Dropping prestage schema...';

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'prestage')
BEGIN
    DROP SCHEMA [prestage];
    PRINT '  ✓ Dropped prestage schema';
END
ELSE
BEGIN
    PRINT '  ℹ️  prestage schema does not exist';
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'PRE-STAGE SCHEMA DROPPED SUCCESSFULLY';
PRINT '============================================================';
PRINT '';
PRINT 'The consolidation audit trail has been permanently removed.';
PRINT 'All data is now in the staging (etl schema) and production (dbo schema) tables.';
PRINT '';

GO
