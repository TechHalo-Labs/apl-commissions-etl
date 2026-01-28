/**
 * Initialize State Management Tables
 * One-time setup script to create state tracking infrastructure
 */

import * as sql from 'mssql';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  console.log('\n🔧 Initializing State Management Tables...\n');
  
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    console.log('📄 Creating state management tables and procedures');
    console.log('');
    
    // Step 1: Drop existing tables
    console.log('   1/3 Dropping existing tables if they exist...');
    await pool.request().query(`
      IF OBJECT_ID('[etl].[etl_step_state]', 'U') IS NOT NULL
          DROP TABLE [etl].[etl_step_state];
      
      IF OBJECT_ID('[etl].[etl_run_state]', 'U') IS NOT NULL
          DROP TABLE [etl].[etl_run_state];
    `);
    
    // Step 2: Create tables
    console.log('   2/3 Creating state tables...');
    await pool.request().query(`
      CREATE TABLE [etl].[etl_run_state] (
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
          CONSTRAINT FK_etl_run_state_ResumedFrom FOREIGN KEY (ResumedFromRunId) 
              REFERENCES [etl].[etl_run_state](RunId)
      );

      CREATE TABLE [etl].[etl_step_state] (
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
          CONSTRAINT FK_etl_step_state_RunId FOREIGN KEY (RunId) 
              REFERENCES [etl].[etl_run_state](RunId) ON DELETE CASCADE
      );

      CREATE INDEX IX_etl_run_state_Status ON [etl].[etl_run_state](Status);
      CREATE INDEX IX_etl_run_state_StartTime ON [etl].[etl_run_state](StartTime DESC);
      CREATE INDEX IX_etl_step_state_RunId ON [etl].[etl_step_state](RunId);
      CREATE INDEX IX_etl_step_state_Status ON [etl].[etl_step_state](Status);
      CREATE INDEX IX_etl_step_state_StepNumber ON [etl].[etl_step_state](RunId, StepNumber);
    `);
    
    // Step 3: Create stored procedures
    console.log('   3/3 Creating stored procedures...');
    
    const procedures = [
      {
        name: 'sp_start_run',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_start_run]
          @RunName NVARCHAR(200),
          @RunType NVARCHAR(50),
          @TotalSteps INT,
          @ConfigSnapshot NVARCHAR(MAX) = NULL,
          @RunId UNIQUEIDENTIFIER OUTPUT
        AS
        BEGIN
          SET NOCOUNT ON;
          SET @RunId = NEWID();
          INSERT INTO [etl].[etl_run_state] (
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
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_update_run_progress]
          @RunId UNIQUEIDENTIFIER,
          @CurrentPhase NVARCHAR(100),
          @CurrentStep NVARCHAR(200),
          @CurrentScript NVARCHAR(500),
          @CompletedSteps INT
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_run_state]
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
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_complete_run]
          @RunId UNIQUEIDENTIFIER
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_run_state]
          SET Status = 'completed',
              EndTime = GETUTCDATE(),
              ProgressPercent = 100.00
          WHERE RunId = @RunId;
        END;`
      },
      {
        name: 'sp_fail_run',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_fail_run]
          @RunId UNIQUEIDENTIFIER,
          @ErrorMessage NVARCHAR(MAX),
          @CanResume BIT = 1
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_run_state]
          SET Status = 'failed',
              EndTime = GETUTCDATE(),
              ErrorMessage = @ErrorMessage,
              CanResume = @CanResume
          WHERE RunId = @RunId;
        END;`
      },
      {
        name: 'sp_start_step',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_start_step]
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
          INSERT INTO [etl].[etl_step_state] (
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
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_complete_step]
          @StepId UNIQUEIDENTIFIER,
          @RecordsProcessed BIGINT = NULL
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_step_state]
          SET Status = 'completed',
              EndTime = GETUTCDATE(),
              RecordsProcessed = ISNULL(@RecordsProcessed, RecordsProcessed)
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_fail_step',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_fail_step]
          @StepId UNIQUEIDENTIFIER,
          @ErrorMessage NVARCHAR(MAX)
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_step_state]
          SET Status = 'failed',
              EndTime = GETUTCDATE(),
              ErrorMessage = @ErrorMessage
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_update_step_progress',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_update_step_progress]
          @StepId UNIQUEIDENTIFIER,
          @RecordsProcessed BIGINT,
          @TotalRecords BIGINT = NULL
        AS
        BEGIN
          SET NOCOUNT ON;
          UPDATE [etl].[etl_step_state]
          SET RecordsProcessed = @RecordsProcessed,
              TotalRecords = ISNULL(@TotalRecords, TotalRecords)
          WHERE StepId = @StepId;
        END;`
      },
      {
        name: 'sp_get_last_run',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_get_last_run]
        AS
        BEGIN
          SET NOCOUNT ON;
          SELECT TOP 1 *
          FROM [etl].[etl_run_state]
          ORDER BY StartTime DESC;
        END;`
      },
      {
        name: 'sp_get_incomplete_steps',
        sql: `CREATE OR ALTER PROCEDURE [etl].[sp_get_incomplete_steps]
          @RunId UNIQUEIDENTIFIER
        AS
        BEGIN
          SET NOCOUNT ON;
          SELECT *
          FROM [etl].[etl_step_state]
          WHERE RunId = @RunId
            AND Status IN ('pending', 'failed')
          ORDER BY StepNumber;
        END;`
      }
    ];
    
    for (const proc of procedures) {
      console.log(`      Creating ${proc.name}...`);
      await pool.request().query(proc.sql);
    }
    
    console.log('');
    console.log('✅ State management infrastructure created successfully!');
    console.log('');
    console.log('Tables created:');
    console.log('  - [etl].[etl_run_state]');
    console.log('  - [etl].[etl_step_state]');
    console.log('');
    console.log('Stored procedures created:');
    procedures.forEach(p => console.log(`  - [etl].[${p.name}]`));
    console.log('');
    console.log('🎉 Ready to run ETL pipeline!\n');
    
  } catch (error) {
    console.error('❌ Failed to create state management infrastructure:');
    console.error(error);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
