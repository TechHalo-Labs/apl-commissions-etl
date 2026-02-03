-- Drop the [trace] schema and all objects within it
-- Run this script to remove the trace schema from the database

PRINT 'Dropping [trace] schema...';

-- First, drop all tables in the trace schema
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql += 'DROP TABLE [trace].[' + name + '];' + CHAR(13)
FROM sys.tables
WHERE schema_id = SCHEMA_ID('trace');

IF LEN(@sql) > 0
BEGIN
    PRINT 'Dropping tables in [trace] schema:';
    PRINT @sql;
    EXEC sp_executesql @sql;
END

-- Drop any views
SET @sql = N'';
SELECT @sql += 'DROP VIEW [trace].[' + name + '];' + CHAR(13)
FROM sys.views
WHERE schema_id = SCHEMA_ID('trace');

IF LEN(@sql) > 0
BEGIN
    PRINT 'Dropping views in [trace] schema:';
    PRINT @sql;
    EXEC sp_executesql @sql;
END

-- Drop any procedures
SET @sql = N'';
SELECT @sql += 'DROP PROCEDURE [trace].[' + name + '];' + CHAR(13)
FROM sys.procedures
WHERE schema_id = SCHEMA_ID('trace');

IF LEN(@sql) > 0
BEGIN
    PRINT 'Dropping procedures in [trace] schema:';
    PRINT @sql;
    EXEC sp_executesql @sql;
END

-- Finally, drop the schema itself
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'trace')
BEGIN
    DROP SCHEMA [trace];
    PRINT 'Schema [trace] dropped successfully.';
END
ELSE
BEGIN
    PRINT 'Schema [trace] does not exist.';
END
