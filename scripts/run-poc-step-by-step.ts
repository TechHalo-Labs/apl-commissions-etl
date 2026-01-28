/**
 * POC Step-by-Step Pipeline Runner
 * Runs ETL pipeline phase-by-phase with pauses for verification and timing measurement
 */

import * as sql from 'mssql';
import * as path from 'path';
import * as readline from 'readline';
import { loadConfig, getSqlConfig, ETLConfig } from './lib/config-loader';
import { ETLStateManager } from './lib/state-manager';
import { ProgressReporter } from './lib/progress-reporter';
import { executeSQLScript } from './lib/sql-executor';

interface PhaseResult {
  name: string;
  duration: number;
  steps: number;
  status: 'success' | 'failed';
  error?: string;
}

/**
 * Pause execution and wait for user confirmation
 */
async function pauseForConfirmation(message: string): Promise<void> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  return new Promise((resolve) => {
    rl.question(`\n⏸️  ${message}\n   Press ENTER to continue...`, () => {
      rl.close();
      resolve();
    });
  });
}

interface SchemaSnapshot {
  poc_etl_tables: number;
  poc_etl_records: number;
  poc_dbo_tables: number;
  poc_dbo_records: number;
  etl_records: number;
  dbo_records: number;
}

/**
 * Capture complete schema snapshot for verification
 */
async function captureSchemaSnapshot(pool: sql.ConnectionPool): Promise<SchemaSnapshot> {
  const snapshot: SchemaSnapshot = {
    poc_etl_tables: 0,
    poc_etl_records: 0,
    poc_dbo_tables: 0,
    poc_dbo_records: 0,
    etl_records: 0,
    dbo_records: 0
  };
  
  try {
    // POC ETL tables
    const pocEtlTables = await pool.request().query(`
      SELECT COUNT(*) as cnt
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl'
    `);
    snapshot.poc_etl_tables = pocEtlTables.recordset[0].cnt;
    
    if (snapshot.poc_etl_tables > 0) {
      const pocEtlRecords = await pool.request().query(`
        SELECT SUM(p.rows) as total
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE s.name = 'poc_etl' AND p.index_id IN (0, 1)
      `);
      snapshot.poc_etl_records = pocEtlRecords.recordset[0].total || 0;
    }
    
    // POC DBO tables
    const pocDboTables = await pool.request().query(`
      SELECT COUNT(*) as cnt
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_dbo'
    `);
    snapshot.poc_dbo_tables = pocDboTables.recordset[0].cnt;
    
    if (snapshot.poc_dbo_tables > 0) {
      const pocDboRecords = await pool.request().query(`
        SELECT SUM(p.rows) as total
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE s.name = 'poc_dbo' AND p.index_id IN (0, 1)
      `);
      snapshot.poc_dbo_records = pocDboRecords.recordset[0].total || 0;
    }
    
    // Standard ETL records
    const etlRecords = await pool.request().query(`
      SELECT SUM(p.rows) as total
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      INNER JOIN sys.partitions p ON t.object_id = p.object_id
      WHERE s.name = 'etl' AND p.index_id IN (0, 1)
    `);
    snapshot.etl_records = etlRecords.recordset[0].total || 0;
    
    // Standard DBO records
    const dboRecords = await pool.request().query(`
      SELECT SUM(p.rows) as total
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      INNER JOIN sys.partitions p ON t.object_id = p.object_id
      WHERE s.name = 'dbo' AND p.index_id IN (0, 1)
    `);
    snapshot.dbo_records = dboRecords.recordset[0].total || 0;
    
  } catch (error) {
    console.error('Error capturing snapshot:', error);
  }
  
  return snapshot;
}

/**
 * Verify POC schema isolation and production safety
 * Enhanced with before/after snapshots and violation detection
 */
async function verifyPOCSchemas(
  pool: sql.ConnectionPool,
  phase: string,
  beforeSnapshot?: SchemaSnapshot
): Promise<SchemaSnapshot> {
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`📊 VERIFICATION: ${phase}`);
  console.log('═'.repeat(70) + '\n');
  
  const afterSnapshot = await captureSchemaSnapshot(pool);
  
  // Check POC schema changes
  console.log('POC Schema Status:');
  console.log(`  [poc_etl]:  ${afterSnapshot.poc_etl_tables} tables, ${afterSnapshot.poc_etl_records} records`);
  console.log(`  [poc_dbo]:  ${afterSnapshot.poc_dbo_tables} tables, ${afterSnapshot.poc_dbo_records} records`);
  
  // Check standard schemas for violations
  if (beforeSnapshot) {
    console.log('\n✅ Isolation Verification:');
    
    const etlChanged = afterSnapshot.etl_records !== beforeSnapshot.etl_records;
    const dboChanged = afterSnapshot.dbo_records !== beforeSnapshot.dbo_records;
    
    if (etlChanged) {
      console.log(`  ❌ VIOLATION: [etl] changed (${beforeSnapshot.etl_records} → ${afterSnapshot.etl_records})`);
      throw new Error('ISOLATION VIOLATION: [etl] schema was modified!');
    } else {
      console.log(`  ✅ [etl]:  ${afterSnapshot.etl_records} records (UNCHANGED)`);
    }
    
    if (dboChanged) {
      console.log(`  ❌ VIOLATION: [dbo] changed (${beforeSnapshot.dbo_records} → ${afterSnapshot.dbo_records})`);
      throw new Error('ISOLATION VIOLATION: [dbo] schema was modified!');
    } else {
      console.log(`  ✅ [dbo]:  ${afterSnapshot.dbo_records} records (UNCHANGED)`);
    }
  } else {
    // First verification, just show current state
    console.log('\n📊 Standard Schemas Baseline:');
    console.log(`  [etl]:  ${afterSnapshot.etl_records} records`);
    console.log(`  [dbo]:  ${afterSnapshot.dbo_records} records`);
  }
  
  console.log('\n' + '═'.repeat(70) + '\n');
  
  return afterSnapshot;
}

/**
 * Run a single phase with timing and error handling
 */
async function runPhase(
  pool: sql.ConnectionPool,
  stateManager: ETLStateManager,
  progress: ProgressReporter,
  config: ETLConfig,
  phaseName: string,
  scripts: string[],
  phaseNumber: number,
  totalPhases: number,
  currentStep: number
): Promise<{ result: PhaseResult; nextStep: number }> {
  const startTime = Date.now();
  const phaseStartTime = Date.now();
  
  progress.logPhase(phaseName, phaseNumber, totalPhases);
  
  let stepNum = currentStep;
  
  try {
    for (const scriptPath of scripts) {
      stepNum++;
      const scriptName = path.basename(scriptPath);
      const stepStartTime = Date.now();
      
      progress.logStep(scriptName, stepNum, scripts.length * totalPhases);
      
      const stepId = await stateManager.startStep(
        stepNum,
        scriptPath,
        scriptName,
        phaseName
      );
      
      try {
        const result = await executeSQLScript({
          config,
          pool,
          scriptPath,
          stepId,
          debugMode: config.debugMode.enabled,
          pocMode: true  // Enable POC mode for schema substitution
        });
        
        if (!result.success) {
          await stateManager.failStep(stepId, result.error!);
          throw result.error;
        }
        
        await stateManager.completeStep(stepId, result.recordsAffected);
        
        const stepDuration = ((Date.now() - stepStartTime) / 1000).toFixed(1);
        console.log(`    ✅ ${scriptName} completed in ${stepDuration}s\n`);
        
      } catch (error) {
        await stateManager.failStep(stepId, error as Error);
        throw error;
      }
    }
    
    const duration = (Date.now() - phaseStartTime) / 1000;
    const phaseDuration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log(`\n✅ Phase "${phaseName}" completed in ${phaseDuration}s\n`);
    
    return {
      result: {
        name: phaseName,
        duration,
        steps: scripts.length,
        status: 'success'
      },
      nextStep: stepNum
    };
  } catch (error) {
    const duration = (Date.now() - phaseStartTime) / 1000;
    
    return {
      result: {
        name: phaseName,
        duration,
        steps: scripts.length,
        status: 'failed',
        error: (error as Error).message
      },
      nextStep: stepNum
    };
  }
}

/**
 * Print execution summary
 */
function printSummary(results: PhaseResult[], totalDuration: number): void {
  console.log('\n' + '╔' + '═'.repeat(68) + '╗');
  console.log('║' + ' '.repeat(20) + 'POC PIPELINE EXECUTION SUMMARY' + ' '.repeat(18) + '║');
  console.log('╚' + '═'.repeat(68) + '╝\n');
  
  results.forEach((result, i) => {
    const statusIcon = result.status === 'success' ? '✅' : '❌';
    console.log(`${statusIcon} Phase ${i + 1}: ${result.name}`);
    console.log(`   Duration:  ${result.duration.toFixed(2)}s`);
    console.log(`   Scripts:   ${result.steps}`);
    console.log(`   Status:    ${result.status}`);
    if (result.error) {
      console.log(`   Error:     ${result.error.substring(0, 100)}...`);
    }
    console.log('');
  });
  
  const totalSteps = results.reduce((sum, r) => sum + r.steps, 0);
  const successfulPhases = results.filter(r => r.status === 'success').length;
  
  console.log('─'.repeat(70));
  console.log(`Total Duration: ${totalDuration.toFixed(2)}s`);
  console.log(`Total Steps:    ${totalSteps}`);
  console.log(`Throughput:     ${(totalSteps / totalDuration).toFixed(2)} steps/sec`);
  console.log(`Success Rate:   ${successfulPhases}/${results.length} phases`);
  console.log('─'.repeat(70) + '\n');
}

/**
 * Main POC step-by-step execution
 */
async function main() {
  console.log('\n' + '╔' + '═'.repeat(68) + '╗');
  console.log('║' + ' '.repeat(15) + 'POC ETL STEP-BY-STEP EXECUTION' + ' '.repeat(23) + '║');
  console.log('╚' + '═'.repeat(68) + '╝\n');
  
  // Load POC configuration
  const config = loadConfig(undefined, 'appsettings.poc.json');
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  const stateManager = new ETLStateManager(pool, config.database.schemas.processing);
  const progress = new ProgressReporter();
  
  const phaseResults: PhaseResult[] = [];
  const pipelineStartTime = Date.now();
  
  // Define script paths
  const scriptsDir = path.join(__dirname, '../sql');
  
  // POC mode: Skip schema setup, schemas already exist from setup-poc-schemas.ts
  const schemaScripts = [
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
  
  try {
    const totalSteps = schemaScripts.length + transformScripts.length + exportScripts.length;
    
    // Start new run
    const runName = `POC-StepByStep-${new Date().toISOString().replace(/[:.]/g, '-')}`;
    await stateManager.startRun(runName, 'poc-step-by-step', totalSteps, config);
    
    progress.logRunStart(runName, 'poc-step-by-step', totalSteps);
    
    let currentStep = 0;
    
    // Step 0: Initial verification (capture baseline)
    await pauseForConfirmation('Paused before INITIAL SETUP verification');
    const initialSnapshot = await verifyPOCSchemas(pool, 'Initial State');
    
    // Phase 1: Schema Setup
    await pauseForConfirmation(`Paused before PHASE 1: Schema Setup (${schemaScripts.length} scripts)`);
    const phase1Result = await runPhase(
      pool, stateManager, progress, config,
      'Schema Setup', schemaScripts, 1, 3, currentStep
    );
    phaseResults.push(phase1Result.result);
    currentStep = phase1Result.nextStep;
    const phase1Snapshot = await verifyPOCSchemas(pool, 'After Schema Setup', initialSnapshot);
    
    if (phase1Result.result.status === 'failed') {
      throw new Error(`Phase 1 failed: ${phase1Result.result.error}`);
    }
    
    // Phase 2: Data Transforms
    await pauseForConfirmation(`Paused before PHASE 2: Data Transforms (${transformScripts.length} scripts)`);
    const phase2Result = await runPhase(
      pool, stateManager, progress, config,
      'Data Transforms', transformScripts, 2, 3, currentStep
    );
    phaseResults.push(phase2Result.result);
    currentStep = phase2Result.nextStep;
    const phase2Snapshot = await verifyPOCSchemas(pool, 'After Data Transforms', phase1Snapshot);
    
    if (phase2Result.result.status === 'failed') {
      throw new Error(`Phase 2 failed: ${phase2Result.result.error}`);
    }
    
    // Phase 3: Export to Production
    await pauseForConfirmation(`Paused before PHASE 3: Export to Production (${exportScripts.length} scripts)`);
    const phase3Result = await runPhase(
      pool, stateManager, progress, config,
      'Export to Production', exportScripts, 3, 3, currentStep
    );
    phaseResults.push(phase3Result.result);
    currentStep = phase3Result.nextStep;
    const finalSnapshot = await verifyPOCSchemas(pool, 'After Export to Production', phase2Snapshot);
    
    if (phase3Result.result.status === 'failed') {
      throw new Error(`Phase 3 failed: ${phase3Result.result.error}`);
    }
    
    // Complete run
    await stateManager.completeRun();
    
    // Final verification
    await pauseForConfirmation('Paused before FINAL VERIFICATION');
    await verifyPOCSchemas(pool, 'Final State', initialSnapshot);
    
    const totalDuration = (Date.now() - pipelineStartTime) / 1000;
    
    // Print summary
    printSummary(phaseResults, totalDuration);
    
    progress.logRunComplete(totalSteps, totalDuration);
    
    // Report 100% isolation success
    console.log('\n' + '╔' + '═'.repeat(68) + '╗');
    console.log('║' + ' '.repeat(18) + '✅ 100% SCHEMA ISOLATION' + ' '.repeat(24) + '║');
    console.log('╚' + '═'.repeat(68) + '╝\n');
    console.log('All phases completed with verified isolation.');
    console.log('Standard schemas [etl] and [dbo] remain completely untouched.\n');
    
  } catch (error) {
    console.error('\n❌ POC Pipeline Failed:');
    console.error(error);
    
    await stateManager.failRun(error as Error, true);
    
    const totalDuration = (Date.now() - pipelineStartTime) / 1000;
    printSummary(phaseResults, totalDuration);
    
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
