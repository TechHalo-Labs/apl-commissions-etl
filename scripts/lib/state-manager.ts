import * as sql from 'mssql';

export interface RunState {
  runId: string;
  runName: string;
  runType: 'full' | 'transform-only' | 'export-only';
  startTime: Date;
  endTime?: Date;
  status: 'running' | 'completed' | 'failed' | 'paused';
  currentPhase?: string;
  currentStep?: string;
  currentScript?: string;
  totalSteps: number;
  completedSteps: number;
  progressPercent: number;
  errorMessage?: string;
  canResume: boolean;
  resumedFromRunId?: string;
  configSnapshot?: string;
}

export interface StepState {
  stepId: string;
  runId: string;
  stepNumber: number;
  scriptPath: string;
  scriptName: string;
  phase: string;
  startTime?: Date;
  endTime?: Date;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'skipped';
  recordsProcessed?: number;
  totalRecords?: number;
  errorMessage?: string;
  durationSeconds?: number;
}

/**
 * ETL State Manager
 * Manages run and step state persistence for the ETL pipeline
 */
export class ETLStateManager {
  private pool: sql.ConnectionPool;
  private currentRunId: string | null = null;
  private currentStepId: string | null = null;
  private schemaName: string = 'etl';
  private stateTablesExist: boolean = false;

  constructor(pool: sql.ConnectionPool, schemaName?: string) {
    this.pool = pool;
    if (schemaName) {
      this.schemaName = schemaName;
    }
  }
  
  /**
   * Check if state management tables exist
   */
  async checkStateTablesExist(): Promise<boolean> {
    try {
      await this.pool.request().query(`
        SELECT TOP 1 1 FROM [${this.schemaName}].[etl_run_state]
      `);
      this.stateTablesExist = true;
      return true;
    } catch {
      this.stateTablesExist = false;
      return false;
    }
  }

  /**
   * Start a new ETL run
   */
  async startRun(
    runName: string,
    runType: 'full' | 'transform-only' | 'export-only',
    totalSteps: number,
    config?: any
  ): Promise<string> {
    // Check if state tables exist first
    if (!this.stateTablesExist) {
      await this.checkStateTablesExist();
    }
    
    if (!this.stateTablesExist) {
      console.log('⚠️  State tables not found - running without state tracking');
      // Generate a valid GUID for compatibility with UNIQUEIDENTIFIER parameters
      const crypto = require('crypto');
      this.currentRunId = crypto.randomUUID();
      return this.currentRunId;
    }
    
    const request = this.pool.request();
    
    const configSnapshot = config ? JSON.stringify(config) : null;
    
    const result = await request
      .input('RunName', sql.NVarChar(200), runName)
      .input('RunType', sql.NVarChar(50), runType)
      .input('TotalSteps', sql.Int, totalSteps)
      .input('ConfigSnapshot', sql.NVarChar(sql.MAX), configSnapshot)
      .output('RunId', sql.UniqueIdentifier)
      .execute(`[${this.schemaName}].[sp_start_run]`);
    
    this.currentRunId = result.output.RunId as string;
    return this.currentRunId;
  }

  /**
   * Update run progress
   */
  async updateProgress(
    phase: string,
    step: string,
    script: string,
    completedSteps: number
  ): Promise<void> {
    if (!this.currentRunId || !this.stateTablesExist) {
      return;
    }

    await this.pool.request()
      .input('RunId', sql.UniqueIdentifier, this.currentRunId)
      .input('CurrentPhase', sql.NVarChar(100), phase)
      .input('CurrentStep', sql.NVarChar(200), step)
      .input('CurrentScript', sql.NVarChar(500), script)
      .input('CompletedSteps', sql.Int, completedSteps)
      .execute(`[${this.schemaName}].[sp_update_run_progress]`);
  }

  /**
   * Complete the current run successfully
   */
  async completeRun(): Promise<void> {
    if (!this.currentRunId || !this.stateTablesExist) {
      this.currentRunId = null;
      return;
    }

    await this.pool.request()
      .input('RunId', sql.UniqueIdentifier, this.currentRunId)
      .execute(`[${this.schemaName}].[sp_complete_run]`);
    
    this.currentRunId = null;
  }

  /**
   * Fail the current run
   */
  async failRun(error: Error, canResume: boolean = true): Promise<void> {
    if (!this.currentRunId || !this.stateTablesExist) {
      this.currentRunId = null;
      return;
    }

    const errorMessage = `${error.name}: ${error.message}\n${error.stack}`;
    
    await this.pool.request()
      .input('RunId', sql.UniqueIdentifier, this.currentRunId)
      .input('ErrorMessage', sql.NVarChar(sql.MAX), errorMessage)
      .input('CanResume', sql.Bit, canResume)
      .execute(`[${this.schemaName}].[sp_fail_run]`);
    
    this.currentRunId = null;
  }

  /**
   * Start a step
   */
  async startStep(
    stepNumber: number,
    scriptPath: string,
    scriptName: string,
    phase: string
  ): Promise<string> {
    if (!this.currentRunId) {
      throw new Error('No active run. Call startRun first.');
    }
    
    if (!this.stateTablesExist) {
      const crypto = require('crypto');
      this.currentStepId = crypto.randomUUID();
      return this.currentStepId;
    }
    
    const request = this.pool.request();
    
    const result = await request
      .input('RunId', sql.UniqueIdentifier, this.currentRunId)
      .input('StepNumber', sql.Int, stepNumber)
      .input('ScriptPath', sql.NVarChar(500), scriptPath)
      .input('ScriptName', sql.NVarChar(200), scriptName)
      .input('Phase', sql.NVarChar(100), phase)
      .output('StepId', sql.UniqueIdentifier)
      .execute(`[${this.schemaName}].[sp_start_step]`);
    
    this.currentStepId = result.output.StepId as string;
    return this.currentStepId;
  }

  /**
   * Complete the current step successfully
   */
  async completeStep(stepId?: string, recordsProcessed?: number): Promise<void> {
    const id = stepId || this.currentStepId;
    if (!id || !this.stateTablesExist) {
      if (!stepId) {
        this.currentStepId = null;
      }
      return;
    }

    await this.pool.request()
      .input('StepId', sql.UniqueIdentifier, id)
      .input('RecordsProcessed', sql.BigInt, recordsProcessed)
      .execute(`[${this.schemaName}].[sp_complete_step]`);
    
    if (!stepId) {
      this.currentStepId = null;
    }
  }

  /**
   * Fail the current step
   */
  async failStep(stepId: string | undefined, error: Error): Promise<void> {
    const id = stepId || this.currentStepId;
    if (!id || !this.stateTablesExist) {
      if (!stepId) {
        this.currentStepId = null;
      }
      return;
    }

    const errorMessage = `${error.name}: ${error.message}\n${error.stack}`;
    
    await this.pool.request()
      .input('StepId', sql.UniqueIdentifier, id)
      .input('ErrorMessage', sql.NVarChar(sql.MAX), errorMessage)
      .execute(`[${this.schemaName}].[sp_fail_step]`);
    
    if (!stepId) {
      this.currentStepId = null;
    }
  }

  /**
   * Update step progress (for long-running steps)
   */
  async updateStepProgress(
    stepId: string,
    recordsProcessed: number,
    totalRecords?: number
  ): Promise<void> {
    await this.pool.request()
      .input('StepId', sql.UniqueIdentifier, stepId)
      .input('RecordsProcessed', sql.BigInt, recordsProcessed)
      .input('TotalRecords', sql.BigInt, totalRecords)
      .execute(`[${this.schemaName}].[sp_update_step_progress]`);
  }

  /**
   * Get the last run (for resume capability)
   */
  async getLastRun(): Promise<RunState | null> {
    const result = await this.pool.request()
      .execute(`[${this.schemaName}].[sp_get_last_run]`);
    
    if (result.recordset.length === 0) {
      return null;
    }

    const row = result.recordset[0];
    return {
      runId: row.RunId,
      runName: row.RunName,
      runType: row.RunType,
      startTime: row.StartTime,
      endTime: row.EndTime,
      status: row.Status,
      currentPhase: row.CurrentPhase,
      currentStep: row.CurrentStep,
      currentScript: row.CurrentScript,
      totalSteps: row.TotalSteps,
      completedSteps: row.CompletedSteps,
      progressPercent: row.ProgressPercent,
      errorMessage: row.ErrorMessage,
      canResume: row.CanResume,
      resumedFromRunId: row.ResumedFromRunId,
      configSnapshot: row.ConfigSnapshot
    };
  }

  /**
   * Check if a run can be resumed
   */
  async canResume(runId: string): Promise<boolean> {
    const result = await this.pool.request()
      .input('RunId', sql.UniqueIdentifier, runId)
      .query(`
        SELECT CanResume, Status
        FROM [${this.schemaName}].[etl_run_state]
        WHERE RunId = @RunId
      `);
    
    if (result.recordset.length === 0) {
      return false;
    }

    const row = result.recordset[0];
    return row.CanResume && row.Status === 'failed';
  }

  /**
   * Get incomplete steps for a run (for resume)
   */
  async getIncompleteSteps(runId: string): Promise<StepState[]> {
    const result = await this.pool.request()
      .input('RunId', sql.UniqueIdentifier, runId)
      .execute(`[${this.schemaName}].[sp_get_incomplete_steps]`);
    
    return result.recordset.map(row => ({
      stepId: row.StepId,
      runId: row.RunId,
      stepNumber: row.StepNumber,
      scriptPath: row.ScriptPath,
      scriptName: row.ScriptName,
      phase: row.Phase,
      startTime: row.StartTime,
      endTime: row.EndTime,
      status: row.Status,
      recordsProcessed: row.RecordsProcessed,
      totalRecords: row.TotalRecords,
      errorMessage: row.ErrorMessage,
      durationSeconds: row.DurationSeconds
    }));
  }

  /**
   * Get current run ID
   */
  getCurrentRunId(): string | null {
    return this.currentRunId;
  }

  /**
   * Set current run ID (for resume)
   */
  setCurrentRunId(runId: string): void {
    this.currentRunId = runId;
  }
}
