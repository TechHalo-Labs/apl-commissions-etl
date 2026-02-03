-- =============================================================================
-- Schema Cleanup Script
-- =============================================================================
-- Cleans up database schemas after ETL completion.
--
-- KEEP INTACT (no changes):
--   dbo, hangfire, etl, reporting, backup
--
-- KEEP AS EMPTY (drop objects, retain schema):
--   raw_data, trace
--
-- DROP COMPLETELY:
--   All other schemas (prestage, poc_*, old_*, new_data, prod_shadow, etl_backup_*, etc.)
--
-- Usage: 
--   1. Run with @DryRun = 1 (default) to preview changes
--   2. Review output carefully
--   3. Set @DryRun = 0 and run again to execute
--
-- =============================================================================

SET NOCOUNT ON;

-- =============================================================================
-- CONFIGURATION
-- =============================================================================
DECLARE @DryRun BIT = 1;  -- Set to 0 to actually execute drops

-- Schemas to keep intact (no changes at all)
DECLARE @KeepIntact TABLE (SchemaName NVARCHAR(128));
INSERT INTO @KeepIntact VALUES 
    ('dbo'), ('hangfire'), ('etl'), ('reporting'), ('backup');

-- Schemas to empty (drop objects but keep schema)
DECLARE @KeepEmpty TABLE (SchemaName NVARCHAR(128));
INSERT INTO @KeepEmpty VALUES 
    ('raw_data'), ('trace');

-- System schemas to never touch
DECLARE @SystemSchemas TABLE (SchemaName NVARCHAR(128));
INSERT INTO @SystemSchemas VALUES 
    ('guest'), ('INFORMATION_SCHEMA'), ('sys'),
    ('db_owner'), ('db_accessadmin'), ('db_securityadmin'),
    ('db_ddladmin'), ('db_backupoperator'), ('db_datareader'),
    ('db_datawriter'), ('db_denydatareader'), ('db_denydatawriter');

-- =============================================================================
-- HEADER
-- =============================================================================
PRINT '============================================================';
PRINT 'SCHEMA CLEANUP SCRIPT';
PRINT '============================================================';
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '';
IF @DryRun = 1
BEGIN
    PRINT '>>> DRY RUN MODE - No changes will be made <<<';
    PRINT '>>> Set @DryRun = 0 to execute cleanup <<<';
END
ELSE
BEGIN
    PRINT '>>> EXECUTION MODE - Changes WILL be made <<<';
END
PRINT '';

-- =============================================================================
-- STEP 1: Discovery - Show all schemas and their categorization
-- =============================================================================
PRINT '------------------------------------------------------------';
PRINT 'STEP 1: Schema Discovery';
PRINT '------------------------------------------------------------';
PRINT '';

-- Show schemas to keep intact
PRINT 'Schemas to KEEP INTACT (no changes):';
SELECT '  - ' + s.name AS [Schema]
FROM sys.schemas s
INNER JOIN @KeepIntact ki ON s.name = ki.SchemaName
ORDER BY s.name;

PRINT '';

-- Show schemas to empty
PRINT 'Schemas to EMPTY (drop objects, keep schema):';
SELECT '  - ' + s.name + ' (' + CAST(
    (SELECT COUNT(*) FROM sys.tables t WHERE t.schema_id = s.schema_id) +
    (SELECT COUNT(*) FROM sys.views v WHERE v.schema_id = s.schema_id) +
    (SELECT COUNT(*) FROM sys.procedures p WHERE p.schema_id = s.schema_id) +
    (SELECT COUNT(*) FROM sys.objects o WHERE o.schema_id = s.schema_id AND o.type IN ('FN', 'IF', 'TF'))
    AS VARCHAR) + ' objects)' AS [Schema]
FROM sys.schemas s
INNER JOIN @KeepEmpty ke ON s.name = ke.SchemaName
ORDER BY s.name;

PRINT '';

-- Show schemas to drop completely
PRINT 'Schemas to DROP COMPLETELY:';
;WITH SchemasToProcess AS (
    SELECT s.schema_id, s.name
    FROM sys.schemas s
    WHERE s.name NOT IN (SELECT SchemaName FROM @KeepIntact)
      AND s.name NOT IN (SELECT SchemaName FROM @KeepEmpty)
      AND s.name NOT IN (SELECT SchemaName FROM @SystemSchemas)
      AND s.principal_id = 1  -- Only schemas owned by dbo
)
SELECT '  - ' + stp.name + ' (' + CAST(
    (SELECT COUNT(*) FROM sys.tables t WHERE t.schema_id = stp.schema_id) +
    (SELECT COUNT(*) FROM sys.views v WHERE v.schema_id = stp.schema_id) +
    (SELECT COUNT(*) FROM sys.procedures p WHERE p.schema_id = stp.schema_id) +
    (SELECT COUNT(*) FROM sys.objects o WHERE o.schema_id = stp.schema_id AND o.type IN ('FN', 'IF', 'TF'))
    AS VARCHAR) + ' objects)' AS [Schema]
FROM SchemasToProcess stp
ORDER BY stp.name;

PRINT '';

-- =============================================================================
-- STEP 2: Empty schemas that should be kept but cleared
-- =============================================================================
PRINT '------------------------------------------------------------';
PRINT 'STEP 2: Empty Schemas (raw_data, trace)';
PRINT '------------------------------------------------------------';
PRINT '';

DECLARE @SchemaToEmpty NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);
DECLARE @objectCount INT;

DECLARE empty_cursor CURSOR FOR
    SELECT SchemaName FROM @KeepEmpty;

OPEN empty_cursor;
FETCH NEXT FROM empty_cursor INTO @SchemaToEmpty;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = @SchemaToEmpty)
    BEGIN
        PRINT 'Processing schema [' + @SchemaToEmpty + ']...';
        
        -- Drop foreign key constraints referencing tables in this schema
        SET @sql = N'';
        SELECT @sql = @sql + 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(fk.parent_object_id) + '].[' + 
            OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.referenced_object_id = t.object_id
        WHERE t.schema_id = SCHEMA_ID(@SchemaToEmpty);
        
        IF LEN(@sql) > 0
        BEGIN
            PRINT '  Dropping foreign key constraints referencing this schema...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would execute: ' + LEFT(@sql, 200) + '...';
        END
        
        -- Drop foreign key constraints in this schema
        SET @sql = N'';
        SELECT @sql = @sql + 'ALTER TABLE [' + @SchemaToEmpty + '].[' + 
            OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        WHERE t.schema_id = SCHEMA_ID(@SchemaToEmpty);
        
        IF LEN(@sql) > 0
        BEGIN
            PRINT '  Dropping foreign key constraints in this schema...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would execute: ' + LEFT(@sql, 200) + '...';
        END
        
        -- Drop tables
        SET @sql = N'';
        SELECT @sql = @sql + 'DROP TABLE [' + @SchemaToEmpty + '].[' + name + ']; '
        FROM sys.tables
        WHERE schema_id = SCHEMA_ID(@SchemaToEmpty);
        
        SET @objectCount = (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(@SchemaToEmpty));
        IF @objectCount > 0
        BEGIN
            PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' table(s)...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would drop tables';
        END
        
        -- Drop views
        SET @sql = N'';
        SELECT @sql = @sql + 'DROP VIEW [' + @SchemaToEmpty + '].[' + name + ']; '
        FROM sys.views
        WHERE schema_id = SCHEMA_ID(@SchemaToEmpty);
        
        SET @objectCount = (SELECT COUNT(*) FROM sys.views WHERE schema_id = SCHEMA_ID(@SchemaToEmpty));
        IF @objectCount > 0
        BEGIN
            PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' view(s)...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would drop views';
        END
        
        -- Drop procedures
        SET @sql = N'';
        SELECT @sql = @sql + 'DROP PROCEDURE [' + @SchemaToEmpty + '].[' + name + ']; '
        FROM sys.procedures
        WHERE schema_id = SCHEMA_ID(@SchemaToEmpty);
        
        SET @objectCount = (SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID(@SchemaToEmpty));
        IF @objectCount > 0
        BEGIN
            PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' procedure(s)...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would drop procedures';
        END
        
        -- Drop functions
        SET @sql = N'';
        SELECT @sql = @sql + 'DROP FUNCTION [' + @SchemaToEmpty + '].[' + name + ']; '
        FROM sys.objects
        WHERE schema_id = SCHEMA_ID(@SchemaToEmpty)
          AND type IN ('FN', 'IF', 'TF');  -- Scalar, Inline Table, Multi-statement Table functions
        
        SET @objectCount = (SELECT COUNT(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(@SchemaToEmpty) AND type IN ('FN', 'IF', 'TF'));
        IF @objectCount > 0
        BEGIN
            PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' function(s)...';
            IF @DryRun = 0 EXEC sp_executesql @sql;
            ELSE PRINT '    [DRY RUN] Would drop functions';
        END
        
        PRINT '  Schema [' + @SchemaToEmpty + '] emptied (schema retained).';
    END
    ELSE
    BEGIN
        PRINT 'Schema [' + @SchemaToEmpty + '] does not exist, skipping.';
    END
    
    PRINT '';
    FETCH NEXT FROM empty_cursor INTO @SchemaToEmpty;
END

CLOSE empty_cursor;
DEALLOCATE empty_cursor;

-- =============================================================================
-- STEP 3: Drop schemas completely
-- =============================================================================
PRINT '------------------------------------------------------------';
PRINT 'STEP 3: Drop Schemas Completely';
PRINT '------------------------------------------------------------';
PRINT '';

DECLARE @SchemaToDrop NVARCHAR(128);

DECLARE drop_cursor CURSOR FOR
    SELECT s.name
    FROM sys.schemas s
    WHERE s.name NOT IN (SELECT SchemaName FROM @KeepIntact)
      AND s.name NOT IN (SELECT SchemaName FROM @KeepEmpty)
      AND s.name NOT IN (SELECT SchemaName FROM @SystemSchemas)
      AND s.principal_id = 1  -- Only schemas owned by dbo
    ORDER BY s.name;

OPEN drop_cursor;
FETCH NEXT FROM drop_cursor INTO @SchemaToDrop;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing schema [' + @SchemaToDrop + '] for complete removal...';
    
    -- Drop foreign key constraints referencing tables in this schema (from other schemas)
    SET @sql = N'';
    SELECT @sql = @sql + 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(fk.parent_object_id) + '].[' + 
        OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables t ON fk.referenced_object_id = t.object_id
    WHERE t.schema_id = SCHEMA_ID(@SchemaToDrop)
      AND OBJECT_SCHEMA_NAME(fk.parent_object_id) <> @SchemaToDrop;
    
    IF LEN(@sql) > 0
    BEGIN
        PRINT '  Dropping external foreign key constraints...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would execute FK drops';
    END
    
    -- Drop foreign key constraints within this schema
    SET @sql = N'';
    SELECT @sql = @sql + 'ALTER TABLE [' + @SchemaToDrop + '].[' + 
        OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
    WHERE t.schema_id = SCHEMA_ID(@SchemaToDrop);
    
    IF LEN(@sql) > 0
    BEGIN
        PRINT '  Dropping internal foreign key constraints...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would execute FK drops';
    END
    
    -- Drop tables
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP TABLE [' + @SchemaToDrop + '].[' + name + ']; '
    FROM sys.tables
    WHERE schema_id = SCHEMA_ID(@SchemaToDrop);
    
    SET @objectCount = (SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(@SchemaToDrop));
    IF @objectCount > 0
    BEGIN
        PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' table(s)...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would drop tables';
    END
    
    -- Drop views
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP VIEW [' + @SchemaToDrop + '].[' + name + ']; '
    FROM sys.views
    WHERE schema_id = SCHEMA_ID(@SchemaToDrop);
    
    SET @objectCount = (SELECT COUNT(*) FROM sys.views WHERE schema_id = SCHEMA_ID(@SchemaToDrop));
    IF @objectCount > 0
    BEGIN
        PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' view(s)...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would drop views';
    END
    
    -- Drop procedures
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP PROCEDURE [' + @SchemaToDrop + '].[' + name + ']; '
    FROM sys.procedures
    WHERE schema_id = SCHEMA_ID(@SchemaToDrop);
    
    SET @objectCount = (SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID(@SchemaToDrop));
    IF @objectCount > 0
    BEGIN
        PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' procedure(s)...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would drop procedures';
    END
    
    -- Drop functions
    SET @sql = N'';
    SELECT @sql = @sql + 'DROP FUNCTION [' + @SchemaToDrop + '].[' + name + ']; '
    FROM sys.objects
    WHERE schema_id = SCHEMA_ID(@SchemaToDrop)
      AND type IN ('FN', 'IF', 'TF');
    
    SET @objectCount = (SELECT COUNT(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(@SchemaToDrop) AND type IN ('FN', 'IF', 'TF'));
    IF @objectCount > 0
    BEGIN
        PRINT '  Dropping ' + CAST(@objectCount AS VARCHAR) + ' function(s)...';
        IF @DryRun = 0 EXEC sp_executesql @sql;
        ELSE PRINT '    [DRY RUN] Would drop functions';
    END
    
    -- Drop the schema itself
    IF @DryRun = 0
    BEGIN
        SET @sql = 'DROP SCHEMA [' + @SchemaToDrop + '];';
        EXEC sp_executesql @sql;
        PRINT '  Schema [' + @SchemaToDrop + '] dropped.';
    END
    ELSE
    BEGIN
        PRINT '  [DRY RUN] Would drop schema [' + @SchemaToDrop + ']';
    END
    
    PRINT '';
    FETCH NEXT FROM drop_cursor INTO @SchemaToDrop;
END

CLOSE drop_cursor;
DEALLOCATE drop_cursor;

-- =============================================================================
-- STEP 4: Verification
-- =============================================================================
PRINT '------------------------------------------------------------';
PRINT 'STEP 4: Post-Cleanup Verification';
PRINT '------------------------------------------------------------';
PRINT '';

PRINT 'Remaining schemas after cleanup:';
SELECT '  - ' + s.name + 
    CASE 
        WHEN s.name IN (SELECT SchemaName FROM @KeepIntact) THEN ' [KEPT INTACT]'
        WHEN s.name IN (SELECT SchemaName FROM @KeepEmpty) THEN ' [EMPTIED]'
        WHEN s.name IN (SELECT SchemaName FROM @SystemSchemas) THEN ' [SYSTEM]'
        ELSE ' [UNEXPECTED]'
    END AS [Schema]
FROM sys.schemas s
WHERE s.principal_id = 1 
   OR s.name IN (SELECT SchemaName FROM @SystemSchemas)
ORDER BY 
    CASE 
        WHEN s.name IN (SELECT SchemaName FROM @KeepIntact) THEN 1
        WHEN s.name IN (SELECT SchemaName FROM @KeepEmpty) THEN 2
        WHEN s.name IN (SELECT SchemaName FROM @SystemSchemas) THEN 3
        ELSE 4
    END,
    s.name;

PRINT '';

-- Check for any unexpected schemas
IF EXISTS (
    SELECT 1
    FROM sys.schemas s
    WHERE s.name NOT IN (SELECT SchemaName FROM @KeepIntact)
      AND s.name NOT IN (SELECT SchemaName FROM @KeepEmpty)
      AND s.name NOT IN (SELECT SchemaName FROM @SystemSchemas)
      AND s.principal_id = 1
)
BEGIN
    IF @DryRun = 1
    BEGIN
        PRINT 'Note: Some schemas will be dropped when @DryRun = 0';
    END
    ELSE
    BEGIN
        PRINT 'WARNING: Unexpected schemas still exist!';
    END
END
ELSE
BEGIN
    IF @DryRun = 0
    BEGIN
        PRINT 'SUCCESS: Only expected schemas remain.';
    END
END

PRINT '';

-- =============================================================================
-- FOOTER
-- =============================================================================
PRINT '============================================================';
IF @DryRun = 1
BEGIN
    PRINT 'DRY RUN COMPLETE - No changes were made';
    PRINT 'To execute cleanup, set @DryRun = 0 and run again';
END
ELSE
BEGIN
    PRINT 'SCHEMA CLEANUP COMPLETE';
END
PRINT '============================================================';
PRINT 'End Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);

GO
