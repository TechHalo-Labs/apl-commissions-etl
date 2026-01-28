/**
 * Setup POC Schemas
 * Creates isolated poc_* schemas and copies sample data from existing etl schema
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function resetSchemas(pool: sql.ConnectionPool): Promise<void> {
  console.log('🔄 Resetting POC schemas to ensure clean state...\n');
  
  // Drop and recreate all POC schemas
  const schemas = ['poc_dbo', 'poc_etl', 'poc_raw_data'];
  
  for (const schemaName of schemas) {
    // Drop schema if exists
    await pool.request().query(`
      IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '${schemaName}')
      BEGIN
        -- Drop procedures first
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql = @sql + 'DROP PROCEDURE IF EXISTS [${schemaName}].[' + p.name + ']; '
        FROM sys.procedures p
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE s.name = '${schemaName}';
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
        
        -- Drop FK constraints
        SET @sql = N'';
        SELECT @sql = @sql + 'ALTER TABLE [${schemaName}].[' + OBJECT_NAME(fk.parent_object_id) + '] DROP CONSTRAINT [' + fk.name + ']; '
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '${schemaName}';
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
        
        -- Drop tables
        SET @sql = N'';
        SELECT @sql = @sql + N'DROP TABLE IF EXISTS [${schemaName}].[' + t.name + N']; '
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '${schemaName}';
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
        
        DROP SCHEMA [${schemaName}];
      END
    `);
    
    // Create schema (must be in separate batch)
    await pool.request().query(`CREATE SCHEMA [${schemaName}]`);
    console.log(`   ✅ [${schemaName}] reset`);
  }
  
  console.log('');
}

async function main() {
  console.log('\n🔧 Setting Up POC Schemas with Sample Data...\n');
  
  // Load POC configuration
  const config = loadConfig();
  
  // Override with POC config file if it exists
  try {
    const fs = require('fs');
    const pocConfig = JSON.parse(fs.readFileSync('appsettings.poc.json', 'utf-8'));
    Object.assign(config.database.schemas, pocConfig.database.schemas);
    Object.assign(config.debugMode, pocConfig.debugMode);
  } catch (error) {
    console.error('⚠️  Could not load appsettings.poc.json, using defaults');
  }
  
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  const maxRecords = config.debugMode.maxRecords;
  
  try {
    console.log('📋 POC Configuration:');
    console.log(`   Source/Transition: ${config.database.schemas.source}`);
    console.log(`   Processing:        ${config.database.schemas.processing}`);
    console.log(`   Production:        ${config.database.schemas.production}`);
    console.log(`   Max Records:       ${maxRecords.brokers} per entity`);
    console.log('');
    
    // Step 0: Reset all POC schemas for clean slate
    await resetSchemas(pool);
    
    // Step 1: Verify clean state
    console.log('Step 1/4: Verifying clean state...');
    const tableCheck = await pool.request().query(`
      SELECT s.name as SchemaName, COUNT(t.name) as TableCount
      FROM sys.schemas s
      LEFT JOIN sys.tables t ON s.schema_id = t.schema_id
      WHERE s.name IN ('poc_raw_data', 'poc_etl', 'poc_dbo')
      GROUP BY s.name
    `);
    
    console.log('   Schema status:');
    tableCheck.recordset.forEach(row => {
      console.log(`      [${row.SchemaName}]: ${row.TableCount} tables`);
    });
    
    if (tableCheck.recordset.some(row => row.TableCount > 0)) {
      throw new Error('POC schemas are not clean! Reset failed.');
    }
    
    console.log('   ✅ All POC schemas are clean\n');
    
    // Step 2: Copy raw tables structure and sample data from etl schema
    console.log('Step 2/4: Copying sample data from [etl] to [poc_raw_data]...');
    
    const rawTables = [
      'raw_premiums',
      'raw_certificate_info',
      'raw_individual_brokers',
      'raw_org_brokers',
      'raw_licenses',
      'raw_eo_insurance',
      'raw_schedule_rates',
      'raw_perf_groups'
    ];
    
    for (const table of rawTables) {
      try {
        // Drop if exists
        await pool.request().query(`
          IF OBJECT_ID('[poc_raw_data].[${table}]', 'U') IS NOT NULL
            DROP TABLE [poc_raw_data].[${table}];
        `);
        
        // Copy structure and top N records
        await pool.request().query(`
          SELECT TOP ${maxRecords.brokers} *
          INTO [poc_raw_data].[${table}]
          FROM [etl].[${table}];
        `);
        
        const result = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [poc_raw_data].[${table}]
        `);
        
        console.log(`   ✅ ${table}: ${result.recordset[0].cnt} records`);
      } catch (error: any) {
        if (error.message.includes('Invalid object name')) {
          console.log(`   ⚠️  ${table}: Source table not found in [etl], skipping`);
        } else {
          console.log(`   ❌ ${table}: ${error.message}`);
        }
      }
    }
    
    console.log('');
    console.log('Step 3/4: Creating state management tables in [poc_etl]...');
    
    // Create state management tables in poc_etl
    await pool.request().query(`
      IF OBJECT_ID('[poc_etl].[etl_step_state]', 'U') IS NOT NULL
          DROP TABLE [poc_etl].[etl_step_state];
      
      IF OBJECT_ID('[poc_etl].[etl_run_state]', 'U') IS NOT NULL
          DROP TABLE [poc_etl].[etl_run_state];
      
      CREATE TABLE [poc_etl].[etl_run_state] (
          RunId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
          RunName NVARCHAR(200) NOT NULL,
          RunType NVARCHAR(50),
          StartTime DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
          EndTime DATETIME2 NULL,
          Status NVARCHAR(20) NOT NULL,
          CurrentPhase NVARCHAR(100),
          CurrentStep NVARCHAR(200),
          CurrentScript NVARCHAR(500),
          TotalSteps INT,
          CompletedSteps INT DEFAULT 0,
          ProgressPercent DECIMAL(5,2) DEFAULT 0.00,
          ErrorMessage NVARCHAR(MAX),
          CanResume BIT DEFAULT 1,
          ResumedFromRunId UNIQUEIDENTIFIER NULL,
          ConfigSnapshot NVARCHAR(MAX),
          CONSTRAINT FK_poc_etl_run_state_ResumedFrom FOREIGN KEY (ResumedFromRunId) 
              REFERENCES [poc_etl].[etl_run_state](RunId)
      );

      CREATE TABLE [poc_etl].[etl_step_state] (
          StepId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
          RunId UNIQUEIDENTIFIER NOT NULL,
          StepNumber INT NOT NULL,
          ScriptPath NVARCHAR(500) NOT NULL,
          ScriptName NVARCHAR(200) NOT NULL,
          Phase NVARCHAR(100),
          StartTime DATETIME2,
          EndTime DATETIME2,
          Status NVARCHAR(20),
          RecordsProcessed BIGINT,
          TotalRecords BIGINT,
          ErrorMessage NVARCHAR(MAX),
          DurationSeconds AS DATEDIFF(SECOND, StartTime, ISNULL(EndTime, GETUTCDATE())),
          CONSTRAINT FK_poc_etl_step_state_RunId FOREIGN KEY (RunId) 
              REFERENCES [poc_etl].[etl_run_state](RunId) ON DELETE CASCADE
      );

      CREATE INDEX IX_poc_etl_run_state_Status ON [poc_etl].[etl_run_state](Status);
      CREATE INDEX IX_poc_etl_run_state_StartTime ON [poc_etl].[etl_run_state](StartTime DESC);
      CREATE INDEX IX_poc_etl_step_state_RunId ON [poc_etl].[etl_step_state](RunId);
      CREATE INDEX IX_poc_etl_step_state_Status ON [poc_etl].[etl_step_state](Status);
      CREATE INDEX IX_poc_etl_step_state_StepNumber ON [poc_etl].[etl_step_state](RunId, StepNumber);
    `);
    
    console.log('   ✅ State management tables created in [poc_etl]\n');
    
    // Step 4: Create stored procedures in poc_etl
    console.log('Step 4/4: Creating stored procedures in [poc_etl]...');
    
    const procedures = [
      {
        name: 'sp_start_run',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_start_run]
          @RunName NVARCHAR(200),
          @RunType NVARCHAR(50),
          @TotalSteps INT,
          @ConfigSnapshot NVARCHAR(MAX) = NULL,
          @RunId UNIQUEIDENTIFIER OUTPUT
        AS
        BEGIN
          SET NOCOUNT ON;
          SET @RunId = NEWID();
          INSERT INTO [poc_etl].[etl_run_state] (
              RunId, RunName, RunType, StartTime, Status, 
              TotalSteps, CompletedSteps, ProgressPercent, ConfigSnapshot
          )
          VALUES (
              @RunId, @RunName, @RunType, GETUTCDATE(), 'running',
              @TotalSteps, 0, 0.00, @ConfigSnapshot
          );
        END;`
      },
      {
        name: 'sp_update_run_progress',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_update_run_progress]
          @RunId UNIQUEIDENTIFIER,
          @CurrentPhase NVARCHAR(100),
          @CurrentStep NVARCHAR(200),
          @CurrentScript NVARCHAR(500),
          @CompletedSteps INT
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_run_state]
          SET CurrentPhase = @CurrentPhase,
              CurrentStep = @CurrentStep,
              CurrentScript = @CurrentScript,
              CompletedSteps = @CompletedSteps,
              ProgressPercent = CASE 
                  WHEN TotalSteps > 0 THEN (CAST(@CompletedSteps AS DECIMAL(10,2)) / TotalSteps) * 100
                  ELSE 0
              END
          WHERE RunId = @RunId;
        END;`
      },
      {
        name: 'sp_complete_run',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_complete_run]
          @RunId UNIQUEIDENTIFIER
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_run_state]
          SET Status = 'completed',
              EndTime = GETUTCDATE(),
              ProgressPercent = 100.00
          WHERE RunId = @RunId;
        END;`
      },
      {
        name: 'sp_fail_run',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_fail_run]
          @RunId UNIQUEIDENTIFIER,
          @ErrorMessage NVARCHAR(MAX),
          @CanResume BIT = 1
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_run_state]
          SET Status = 'failed',
              EndTime = GETUTCDATE(),
              ErrorMessage = @ErrorMessage,
              CanResume = @CanResume
          WHERE RunId = @RunId;
        END;`
      },
      {
        name: 'sp_start_step',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_start_step]
          @RunId UNIQUEIDENTIFIER,
          @StepNumber INT,
          @ScriptPath NVARCHAR(500),
          @ScriptName NVARCHAR(200),
          @Phase NVARCHAR(100),
          @StepId UNIQUEIDENTIFIER OUTPUT
        AS
        BEGIN
          SET NOCOUNT ON;
          SET @StepId = NEWID();
          INSERT INTO [poc_etl].[etl_step_state] (
              StepId, RunId, StepNumber, ScriptPath, ScriptName,
              Phase, StartTime, Status
          )
          VALUES (
              @StepId, @RunId, @StepNumber, @ScriptPath, @ScriptName,
              @Phase, GETUTCDATE(), 'running'
          );
        END;`
      },
      {
        name: 'sp_complete_step',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_complete_step]
          @StepId UNIQUEIDENTIFIER,
          @RecordsProcessed BIGINT = NULL
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_step_state]
          SET Status = 'completed',
              EndTime = GETUTCDATE(),
              RecordsProcessed = ISNULL(@RecordsProcessed, RecordsProcessed)
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_fail_step',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_fail_step]
          @StepId UNIQUEIDENTIFIER,
          @ErrorMessage NVARCHAR(MAX)
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_step_state]
          SET Status = 'failed',
              EndTime = GETUTCDATE(),
              ErrorMessage = @ErrorMessage
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_update_step_progress',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_update_step_progress]
          @StepId UNIQUEIDENTIFIER,
          @RecordsProcessed BIGINT,
          @TotalRecords BIGINT = NULL
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [poc_etl].[etl_step_state]
          SET RecordsProcessed = @RecordsProcessed,
              TotalRecords = ISNULL(@TotalRecords, TotalRecords)
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_get_last_run',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_get_last_run]
        AS
        BEGIN
          SET NOCOUNT ON;
          SELECT TOP 1 *
          FROM [poc_etl].[etl_run_state]
          ORDER BY StartTime DESC;
        END;`
      },
      {
        name: 'sp_get_incomplete_steps',
        sql: `CREATE OR ALTER PROCEDURE [poc_etl].[sp_get_incomplete_steps]
          @RunId UNIQUEIDENTIFIER
        AS
        BEGIN
          SET NOCOUNT ON;
          SELECT *
          FROM [poc_etl].[etl_step_state]
          WHERE RunId = @RunId
            AND Status IN ('pending', 'failed')
          ORDER BY StepNumber;
        END;`
      }
    ];
    
    for (const proc of procedures) {
      await pool.request().query(proc.sql);
      console.log(`   ✅ ${proc.name}`);
    }
    
    console.log('');
    console.log('✅ POC Setup Complete!\n');
    console.log('════════════════════════════════════════════════════════════════');
    console.log('POC Schemas Ready:');
    console.log('  [poc_raw_data] - Sample raw data (100 records per table)');
    console.log('  [poc_etl]      - Staging/processing area');
    console.log('  [poc_dbo]      - Production simulation');
    console.log('');
    console.log('Next Step:');
    console.log('  npx tsx scripts/run-pipeline.ts --config appsettings.poc.json');
    console.log('');
    console.log('This will run the FULL pipeline in isolated POC schemas:');
    console.log('  1. Schema Setup (create staging tables in poc_etl)');
    console.log('  2. Transforms (process 100 records per entity)');
    console.log('  3. Export (write to poc_dbo)');
    console.log('');
    console.log('All existing schemas remain completely untouched!');
    console.log('════════════════════════════════════════════════════════════════\n');
    
  } catch (error) {
    console.error('❌ Failed to setup POC schemas:');
    console.error(error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
