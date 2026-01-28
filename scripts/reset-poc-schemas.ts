/**
 * Reset POC Schemas
 * Drops all POC schemas and recreates them fresh for a clean slate
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  console.log('\n🔄 Resetting POC Schemas...\n');
  
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    console.log('Step 1/4: Dropping POC schemas and all their contents...');
    
    // Drop poc_dbo schema
    await pool.request().query(`
      IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'poc_dbo')
      BEGIN
        PRINT 'Dropping constraints and tables in [poc_dbo]...';
        
        DECLARE @sql NVARCHAR(MAX) = N'';
        
        -- Drop all foreign key constraints first
        SELECT @sql = @sql + 'ALTER TABLE [poc_dbo].[' + OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_dbo';
        
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'Foreign key constraints dropped.';
          SET @sql = N'';
        END
        
        -- Build DROP statements for all tables
        SELECT @sql = @sql + N'DROP TABLE IF EXISTS [poc_dbo].[' + t.name + N']; '
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_dbo';
        
        -- Execute the drops
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'All tables in [poc_dbo] dropped.';
        END
        
        -- Drop the schema
        DROP SCHEMA [poc_dbo];
        PRINT 'Schema [poc_dbo] dropped.';
      END
      ELSE
      BEGIN
        PRINT 'Schema [poc_dbo] does not exist.';
      END
    `);
    
    console.log('   ✅ [poc_dbo] dropped\n');
    
    // Drop poc_etl schema
    await pool.request().query(`
      IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'poc_etl')
      BEGIN
        PRINT 'Dropping procedures, constraints and tables in [poc_etl]...';
        
        DECLARE @sql NVARCHAR(MAX) = N'';
        
        -- Drop all stored procedures first
        SELECT @sql = @sql + 'DROP PROCEDURE IF EXISTS [poc_etl].[' + p.name + ']; '
        FROM sys.procedures p
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE s.name = 'poc_etl';
        
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'Stored procedures dropped.';
          SET @sql = N'';
        END
        
        -- Drop all foreign key constraints
        SELECT @sql = @sql + 'ALTER TABLE [poc_etl].[' + OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_etl';
        
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'Foreign key constraints dropped.';
          SET @sql = N'';
        END
        
        -- Build DROP statements for all tables
        SELECT @sql = @sql + N'DROP TABLE IF EXISTS [poc_etl].[' + t.name + N']; '
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_etl';
        
        -- Execute the drops
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'All tables in [poc_etl] dropped.';
        END
        
        -- Drop the schema
        DROP SCHEMA [poc_etl];
        PRINT 'Schema [poc_etl] dropped.';
      END
      ELSE
      BEGIN
        PRINT 'Schema [poc_etl] does not exist.';
      END
    `);
    
    console.log('   ✅ [poc_etl] dropped\n');
    
    // Drop poc_raw_data schema
    await pool.request().query(`
      IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'poc_raw_data')
      BEGIN
        PRINT 'Dropping constraints and tables in [poc_raw_data]...';
        
        DECLARE @sql NVARCHAR(MAX) = N'';
        
        -- Drop all foreign key constraints first
        SELECT @sql = @sql + 'ALTER TABLE [poc_raw_data].[' + OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_raw_data';
        
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'Foreign key constraints dropped.';
          SET @sql = N'';
        END
        
        -- Build DROP statements for all tables
        SELECT @sql = @sql + N'DROP TABLE IF EXISTS [poc_raw_data].[' + t.name + N']; '
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'poc_raw_data';
        
        -- Execute the drops
        IF LEN(@sql) > 0
        BEGIN
          EXEC sp_executesql @sql;
          PRINT 'All tables in [poc_raw_data] dropped.';
        END
        
        -- Drop the schema
        DROP SCHEMA [poc_raw_data];
        PRINT 'Schema [poc_raw_data] dropped.';
      END
      ELSE
      BEGIN
        PRINT 'Schema [poc_raw_data] does not exist.';
      END
    `);
    
    console.log('   ✅ [poc_raw_data] dropped\n');
    
    console.log('Step 2/4: Verifying all POC schemas are gone...');
    
    const remainingSchemas = await pool.request().query(`
      SELECT name FROM sys.schemas 
      WHERE name IN ('poc_raw_data', 'poc_etl', 'poc_dbo')
    `);
    
    if (remainingSchemas.recordset.length === 0) {
      console.log('   ✅ All POC schemas successfully removed\n');
    } else {
      console.error('   ❌ Some POC schemas still exist:', remainingSchemas.recordset);
      throw new Error('Failed to remove all POC schemas');
    }
    
    console.log('Step 3/4: Recreating empty POC schemas...');
    
    await pool.request().query(`CREATE SCHEMA [poc_raw_data]`);
    await pool.request().query(`CREATE SCHEMA [poc_etl]`);
    await pool.request().query(`CREATE SCHEMA [poc_dbo]`);
    
    console.log('   ✅ POC schemas recreated\n');
    
    console.log('Step 4/4: Verifying clean state...');
    
    const newSchemas = await pool.request().query(`
      SELECT s.name as SchemaName, COUNT(t.name) as TableCount
      FROM sys.schemas s
      LEFT JOIN sys.tables t ON s.schema_id = t.schema_id
      WHERE s.name IN ('poc_raw_data', 'poc_etl', 'poc_dbo')
      GROUP BY s.name
      ORDER BY s.name
    `);
    
    console.log('\nPOC Schema Status:');
    newSchemas.recordset.forEach(row => {
      console.log(`   [${row.SchemaName}]: ${row.TableCount} tables`);
    });
    
    const allEmpty = newSchemas.recordset.every(row => row.TableCount === 0);
    
    if (allEmpty) {
      console.log('\n✅ POC RESET COMPLETE - All schemas are clean and empty\n');
    } else {
      console.warn('\n⚠️  Warning: Some schemas are not empty after reset\n');
    }
    
  } catch (error) {
    console.error('❌ Failed to reset POC schemas:');
    console.error(error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
