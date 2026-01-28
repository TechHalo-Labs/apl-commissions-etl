/**
 * Production-Ready ETL Pipeline Orchestrator
 * ==========================================
 * 
 * Features:
 * - State persistence (tracks progress in database)
 * - Resume capability (continue from failed step)
 * - Schema flexibility (configure via appsettings.json)
 * - Progress reporting (real-time step/phase tracking)
 * - Debug mode (limit records for testing)
 * - Comprehensive error handling
 * 
 * Usage:
 *   npx tsx scripts/run-pipeline.ts [options]
 * 
 * Options:
 *   --resume              Resume from last failed run
 *   --resume-from <id>    Resume from specific run ID
 *   --debug               Enable debug mode with record limits
 *   --skip-schema         Skip schema setup
 *   --skip-ingest         Skip data ingestion
 *   --skip-transform      Skip transforms
 *   --skip-export         Skip export to production
 *   --transforms-only     Run transforms only (skip ingest and export)
 *   --export-only         Run export only (skip ingest and transforms)
 */

import * as sql from 'mssql';
import * as path from 'path';
import { loadConfig, getSqlConfig, validateConfig, printConfig, ETLConfig } from './lib/config-loader';
import { ETLStateManager } from './lib/state-manager';
import { ProgressReporter } from './lib/progress-reporter';
import { executeSQLScript } from './lib/sql-executor';
import { formatError, canResumeAfterError } from './lib/error-handler';

// =============================================================================
// Parse Command Line Arguments
// =============================================================================

const args = process.argv.slice(2);
const flags = {
  resume: args.includes('--resume'),
  resumeFrom: args.includes('--resume-from') ? args[args.indexOf('--resume-from') + 1] : null,
  debug: args.includes('--debug'),
  skipSchema: args.includes('--skip-schema'),
  skipIngest: args.includes('--skip-ingest'),
  skipTransform: args.includes('--skip-transform'),
  skipExport: args.includes('--skip-export'),
  transformsOnly: args.includes('--transforms-only'),
  exportOnly: args.includes('--export-only'),
  config: args.includes('--config') ? args[args.indexOf('--config') + 1] : undefined,
};

// Apply composite flags
if (flags.transformsOnly) {
  flags.skipIngest = true;
  flags.skipExport = true;
}

if (flags.exportOnly) {
  flags.skipIngest = true;
  flags.skipTransform = true;
}

// =============================================================================
// Configuration
// =============================================================================

const configOverrides: Partial<ETLConfig> = {
  resume: {
    enabled: flags.resume || !!flags.resumeFrom,
    resumeFromRunId: flags.resumeFrom
  }
};

if (flags.debug) {
  configOverrides.debugMode = {
    enabled: true,
    maxRecords: {
      brokers: 100,
      groups: 50,
      policies: 1000,
      premiums: 5000,
      hierarchies: 100,
      proposals: 50
    }
  };
}

const config = loadConfig(configOverrides, flags.config);

// Validate configuration
const validation = validateConfig(config);
if (!validation.valid) {
  console.error('\n❌ Configuration validation failed:');
  validation.errors.forEach(err => console.error(`   - ${err}`));
  console.error('\nPlease check appsettings.json or set environment variables.\n');
  process.exit(1);
}

// Print configuration (masked)
if (!flags.resume && !flags.resumeFrom) {
  printConfig(config);
}

// =============================================================================
// SQL Script Paths
// =============================================================================

const scriptsDir = path.join(__dirname, '../sql');

const schemaScripts = [
  path.join(scriptsDir, '00a-state-management-tables.sql'),
  path.join(scriptsDir, '00-schema-setup.sql'),
  path.join(scriptsDir, '01-raw-tables.sql'),
  path.join(scriptsDir, '02-input-tables.sql'),
  path.join(scriptsDir, '03-staging-tables.sql'),
];

const transformScripts = [
  path.join(scriptsDir, 'transforms/00-references.sql'),
  path.join(scriptsDir, 'transforms/01-brokers.sql'),
  path.join(scriptsDir, 'transforms/02-groups.sql'),
  path.join(scriptsDir, 'transforms/03-products.sql'),
  path.join(scriptsDir, 'transforms/04-schedules.sql'),
  path.join(scriptsDir, 'transforms/06a-proposals-simple-groups.sql'),
  path.join(scriptsDir, 'transforms/06b-proposals-non-conformant.sql'),
  path.join(scriptsDir, 'transforms/06c-proposals-plan-differentiated.sql'),
  path.join(scriptsDir, 'transforms/06d-proposals-year-differentiated.sql'),
  path.join(scriptsDir, 'transforms/06e-proposals-granular.sql'),
  path.join(scriptsDir, 'transforms/06f-consolidate-proposals.sql'),
  path.join(scriptsDir, 'transforms/06g-normalize-proposal-date-ranges.sql'),
  path.join(scriptsDir, 'transforms/06z-update-proposal-broker-names.sql'),
  path.join(scriptsDir, 'transforms/07-hierarchies.sql'),
  path.join(scriptsDir, 'transforms/08-hierarchy-splits.sql'),
  path.join(scriptsDir, 'transforms/09-policies.sql'),
  path.join(scriptsDir, 'transforms/10-premium-transactions.sql'),
  path.join(scriptsDir, 'transforms/11-policy-hierarchy-assignments.sql'),
];

const exportScripts = [
  path.join(scriptsDir, 'export/02-export-brokers.sql'),
  path.join(scriptsDir, 'export/05-export-groups.sql'),
  path.join(scriptsDir, 'export/06-export-products.sql'),
  path.join(scriptsDir, 'export/06a-export-plans.sql'),
  path.join(scriptsDir, 'export/01-export-schedules.sql'),
  path.join(scriptsDir, 'export/07-export-proposals.sql'),
  path.join(scriptsDir, 'export/08-export-hierarchies.sql'),
  path.join(scriptsDir, 'export/11-export-splits.sql'),
  path.join(scriptsDir, 'export/09-export-policies.sql'),
  path.join(scriptsDir, 'export/10-export-premium-transactions.sql'),
  path.join(scriptsDir, 'export/14-export-policy-hierarchy-assignments.sql'),
  path.join(scriptsDir, 'export/12-export-assignments.sql'),
  path.join(scriptsDir, 'export/13-export-licenses.sql'),
  path.join(scriptsDir, 'export/15-export-fee-schedules.sql'),
  path.join(scriptsDir, 'export/16-export-broker-banking-infos.sql'),
  path.join(scriptsDir, 'export/17-export-special-schedule-rates.sql'),
  path.join(scriptsDir, 'export/18-export-schedule-rate-tiers.sql'),
  path.join(scriptsDir, 'export/19-export-hierarchy-product-rates.sql'),
];

// =============================================================================
// Main Pipeline
// =============================================================================

async function main() {
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  const stateManager = new ETLStateManager(pool, config.database.schemas.processing);
  const progress = new ProgressReporter();
  
  try {
    // Determine run type
    const runType = flags.transformsOnly ? 'transform-only' 
                  : flags.exportOnly ? 'export-only' 
                  : 'full';
    
    // Calculate total steps
    let totalSteps = 0;
    if (!flags.skipSchema) totalSteps += schemaScripts.length;
    if (!flags.skipTransform) totalSteps += transformScripts.length;
    if (!flags.skipExport) totalSteps += exportScripts.length;
    
    // Check for resume
    if (config.resume.enabled) {
      const lastRun = await stateManager.getLastRun();
      
      if (!lastRun || lastRun.status !== 'failed') {
        console.error('❌ No failed run found to resume');
        process.exit(1);
      }
      
      const canResume = await stateManager.canResume(lastRun.runId);
      if (!canResume) {
        console.error('❌ Last run cannot be resumed');
        process.exit(1);
      }
      
      const incompleteSteps = await stateManager.getIncompleteSteps(lastRun.runId);
      
      progress.logResume(lastRun.runName, incompleteSteps[0].stepNumber, totalSteps);
      
      stateManager.setCurrentRunId(lastRun.runId);
      
      // Resume execution from incomplete steps
      await resumeExecution(pool, stateManager, progress, config, incompleteSteps);
      
      await stateManager.completeRun();
      progress.logRunComplete(totalSteps, 0);
      
      return;
    }
    
    // Start new run
    const runName = `ETL-${runType}-${new Date().toISOString().replace(/[:.]/g, '-')}`;
    await stateManager.startRun(runName, runType, totalSteps, config);
    
    progress.logRunStart(runName, runType, totalSteps);
    
    let currentStep = 0;
    const startTime = Date.now();
    
    // Phase 1: Schema Setup
    if (!flags.skipSchema) {
      progress.logPhase('Schema Setup', 1, 5);
      
      for (const scriptPath of schemaScripts) {
        currentStep++;
        const scriptName = path.basename(scriptPath);
        const stepStartTime = Date.now();
        
        progress.logStep(scriptName, currentStep, totalSteps);
        
        const stepId = await stateManager.startStep(
          currentStep,
          scriptPath,
          scriptName,
          'Schema Setup'
        );
        
        try {
          const result = await executeSQLScript({
            config,
            pool,
            scriptPath,
            stepId,
            debugMode: config.debugMode.enabled
          });
          
          await stateManager.completeStep(stepId, result.recordsAffected);
          await stateManager.updateProgress('Schema Setup', scriptName, scriptPath, currentStep);
          
          const duration = (Date.now() - stepStartTime) / 1000;
          progress.logStepComplete(scriptName, duration, result.recordsAffected);
          
        } catch (error) {
          await stateManager.failStep(stepId, error as Error);
          throw error;
        }
      }
      
      const phaseTime = (Date.now() - startTime) / 1000;
      progress.logPhaseComplete('Schema Setup', phaseTime);
    }
    
    // Phase 2: Transforms
    if (!flags.skipTransform) {
      const phaseStartTime = Date.now();
      progress.logPhase('Data Transforms', 2, 5);
      
      for (const scriptPath of transformScripts) {
        currentStep++;
        const scriptName = path.basename(scriptPath);
        const stepStartTime = Date.now();
        
        progress.logStep(scriptName, currentStep, totalSteps);
        
        const stepId = await stateManager.startStep(
          currentStep,
          scriptPath,
          scriptName,
          'Transforms'
        );
        
        try {
          const result = await executeSQLScript({
            config,
            pool,
            scriptPath,
            stepId,
            debugMode: config.debugMode.enabled
          });
          
          await stateManager.completeStep(stepId, result.recordsAffected);
          await stateManager.updateProgress('Transforms', scriptName, scriptPath, currentStep);
          
          const duration = (Date.now() - stepStartTime) / 1000;
          progress.logStepComplete(scriptName, duration, result.recordsAffected);
          
        } catch (error) {
          await stateManager.failStep(stepId, error as Error);
          throw error;
        }
      }
      
      const phaseTime = (Date.now() - phaseStartTime) / 1000;
      progress.logPhaseComplete('Data Transforms', phaseTime);
    }
    
    // Phase 3: Export
    if (!flags.skipExport) {
      const phaseStartTime = Date.now();
      progress.logPhase('Export to Production', 3, 5);
      
      for (const scriptPath of exportScripts) {
        currentStep++;
        const scriptName = path.basename(scriptPath);
        const stepStartTime = Date.now();
        
        progress.logStep(scriptName, currentStep, totalSteps);
        
        const stepId = await stateManager.startStep(
          currentStep,
          scriptPath,
          scriptName,
          'Export'
        );
        
        try {
          const result = await executeSQLScript({
            config,
            pool,
            scriptPath,
            stepId,
            debugMode: config.debugMode.enabled
          });
          
          await stateManager.completeStep(stepId, result.recordsAffected);
          await stateManager.updateProgress('Export', scriptName, scriptPath, currentStep);
          
          const duration = (Date.now() - stepStartTime) / 1000;
          progress.logStepComplete(scriptName, duration, result.recordsAffected);
          
        } catch (error) {
          await stateManager.failStep(stepId, error as Error);
          throw error;
        }
      }
      
      const phaseTime = (Date.now() - phaseStartTime) / 1000;
      progress.logPhaseComplete('Export to Production', phaseTime);
    }
    
    // Complete run
    await stateManager.completeRun();
    
    const totalDuration = (Date.now() - startTime) / 1000;
    progress.logRunComplete(totalSteps, totalDuration);
    
  } catch (error) {
    const err = error as Error;
    console.error(formatError(err));
    
    const canResume = canResumeAfterError(err);
    await stateManager.failRun(err, canResume);
    progress.logRunFailure(err, canResume);
    
    process.exit(1);
  } finally {
    await pool.close();
  }
}

/**
 * Resume execution from incomplete steps
 */
async function resumeExecution(
  pool: sql.ConnectionPool,
  stateManager: ETLStateManager,
  progress: ProgressReporter,
  config: ETLConfig,
  incompleteSteps: any[]
) {
  for (const step of incompleteSteps) {
    const stepStartTime = Date.now();
    
    progress.logStep(step.scriptName, step.stepNumber, incompleteSteps.length);
    
    const stepId = await stateManager.startStep(
      step.stepNumber,
      step.scriptPath,
      step.scriptName,
      step.phase
    );
    
    try {
      const result = await executeSQLScript({
        config,
        pool,
        scriptPath: step.scriptPath,
        stepId,
        debugMode: config.debugMode.enabled
      });
      
      await stateManager.completeStep(stepId, result.recordsAffected);
      await stateManager.updateProgress(step.phase, step.scriptName, step.scriptPath, step.stepNumber);
      
      const duration = (Date.now() - stepStartTime) / 1000;
      progress.logStepComplete(step.scriptName, duration, result.recordsAffected);
      
    } catch (error) {
      await stateManager.failStep(stepId, error as Error);
      throw error;
    }
  }
}

// Run pipeline
main().catch(console.error);
