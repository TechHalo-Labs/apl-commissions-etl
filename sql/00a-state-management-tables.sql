-- =============================================
-- State Management Tables for ETL Pipeline
-- =============================================
-- Purpose: Track ETL run state, progress, and enable resume capability
-- Created: 2026-01-27

-- Drop existing tables if they exist (for clean recreation)
IF OBJECT_ID('[etl].[etl_step_state]', 'U') IS NOT NULL
    DROP TABLE [etl].[etl_step_state];

IF OBJECT_ID('[etl].[etl_run_state]', 'U') IS NOT NULL
    DROP TABLE [etl].[etl_run_state];

-- Main run state table
CREATE TABLE [etl].[etl_run_state] (
    RunId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    RunName NVARCHAR(200) NOT NULL,
    RunType NVARCHAR(50), -- 'full', 'transform-only', 'export-only'
    StartTime DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    EndTime DATETIME2 NULL,
    Status NVARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed', 'paused'
    CurrentPhase NVARCHAR(100),
    CurrentStep NVARCHAR(200),
    CurrentScript NVARCHAR(500),
    TotalSteps INT,
    CompletedSteps INT DEFAULT 0,
    ProgressPercent DECIMAL(5,2) DEFAULT 0.00,
    ErrorMessage NVARCHAR(MAX),
    CanResume BIT DEFAULT 1,
    ResumedFromRunId UNIQUEIDENTIFIER NULL,
    ConfigSnapshot NVARCHAR(MAX), -- JSON of appsettings
    CONSTRAINT FK_etl_run_state_ResumedFrom FOREIGN KEY (ResumedFromRunId) 
        REFERENCES [etl].[etl_run_state](RunId)
);

-- Individual step state table
CREATE TABLE [etl].[etl_step_state] (
    StepId UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    RunId UNIQUEIDENTIFIER NOT NULL,
    StepNumber INT NOT NULL,
    ScriptPath NVARCHAR(500) NOT NULL,
    ScriptName NVARCHAR(200) NOT NULL,
    Phase NVARCHAR(100),
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status NVARCHAR(20), -- 'pending', 'running', 'completed', 'failed', 'skipped'
    RecordsProcessed BIGINT,
    TotalRecords BIGINT,
    ErrorMessage NVARCHAR(MAX),
    DurationSeconds AS DATEDIFF(SECOND, StartTime, ISNULL(EndTime, GETUTCDATE())),
    CONSTRAINT FK_etl_step_state_RunId FOREIGN KEY (RunId) 
        REFERENCES [etl].[etl_run_state](RunId) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IX_etl_run_state_Status ON [etl].[etl_run_state](Status);
CREATE INDEX IX_etl_run_state_StartTime ON [etl].[etl_run_state](StartTime DESC);
CREATE INDEX IX_etl_step_state_RunId ON [etl].[etl_step_state](RunId);
CREATE INDEX IX_etl_step_state_Status ON [etl].[etl_step_state](Status);
CREATE INDEX IX_etl_step_state_StepNumber ON [etl].[etl_step_state](RunId, StepNumber);

-- =============================================
-- Stored Procedures for State Management
-- =============================================

-- Start a new ETL run
CREATE OR ALTER PROCEDURE [etl].[sp_start_run]
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
END;
GO

-- Update run progress
CREATE OR ALTER PROCEDURE [etl].[sp_update_run_progress]
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
END;
GO

-- Complete a run successfully
CREATE OR ALTER PROCEDURE [etl].[sp_complete_run]
    @RunId UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE [etl].[etl_run_state]
    SET Status = 'completed',
        EndTime = GETUTCDATE(),
        ProgressPercent = 100.00
    WHERE RunId = @RunId;
END;
GO

-- Fail a run
CREATE OR ALTER PROCEDURE [etl].[sp_fail_run]
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
END;
GO

-- Start a step
CREATE OR ALTER PROCEDURE [etl].[sp_start_step]
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
END;
GO

-- Complete a step successfully
CREATE OR ALTER PROCEDURE [etl].[sp_complete_step]
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
END;
GO

-- Fail a step
CREATE OR ALTER PROCEDURE [etl].[sp_fail_step]
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
END;
GO

-- Update step progress (for long-running steps)
CREATE OR ALTER PROCEDURE [etl].[sp_update_step_progress]
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
END;
GO

-- Get last run (for resume capability)
CREATE OR ALTER PROCEDURE [etl].[sp_get_last_run]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT TOP 1 *
    FROM [etl].[etl_run_state]
    ORDER BY StartTime DESC;
END;
GO

-- Get incomplete steps for a run
CREATE OR ALTER PROCEDURE [etl].[sp_get_incomplete_steps]
    @RunId UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT *
    FROM [etl].[etl_step_state]
    WHERE RunId = @RunId
      AND Status IN ('pending', 'failed')
    ORDER BY StepNumber;
END;
GO

PRINT 'State management tables and procedures created successfully';
