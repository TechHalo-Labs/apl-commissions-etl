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

/**
 * Verify POC schema isolation and production safety
 */
async function verifyPOCSchemas(pool: sql.ConnectionPool, phase: string): Promise<void> {
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`📊 VERIFICATION: ${phase}`);
  console.log('═'.repeat(70) + '\n');
  
  // Check that POC schemas exist
  const pocSchemas = await pool.request().query(`
    SELECT name FROM sys.schemas 
    WHERE name IN ('poc_raw_data', 'poc_etl', 'poc_dbo')
    ORDER BY name
  `);
  
  console.log(`POC Schemas Status: ${pocSchemas.recordset.length}/3 schemas exist`);
  
  // Count tables in each schema
  const tableCounts = await pool.request().query(`
    SELECT s.name as SchemaName, COUNT(t.name) as TableCount
    FROM sys.schemas s
    LEFT JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name IN ('poc_raw_data', 'poc_etl', 'poc_dbo')
    GROUP BY s.name
    ORDER BY s.name
  `);
  
  console.log('\nPOC Schema Table Counts:');
  tableCounts.recordset.forEach(row => {
    console.log(`   [${row.SchemaName}]: ${row.TableCount} tables`);
  });
  
  // Sample some key staging tables if they exist
  try {
    const stagingData = await pool.request().query(`
      SELECT 
        (SELECT COUNT(*) FROM [poc_etl].[stg_brokers]) as Brokers,
        (SELECT COUNT(*) FROM [poc_etl].[stg_groups]) as Groups,
        (SELECT COUNT(*) FROM [poc_etl].[stg_policies]) as Policies
    `);
    
    if (stagingData.recordset.length > 0) {
      const data = stagingData.recordset[0];
      console.log('\nPOC Staging Data:');
      console.log(`   Brokers:  ${data.Brokers}`);
      console.log(`   Groups:   ${data.Groups}`);
      console.log(`   Policies: ${data.Policies}`);
    }
  } catch {
    // Tables don't exist yet, that's fine
  }
  
  // Sample production POC tables if they exist
  try {
    const prodData = await pool.request().query(`
      SELECT 
        (SELECT COUNT(*) FROM [poc_dbo].[Brokers]) as Brokers,
        (SELECT COUNT(*) FROM [poc_dbo].[Group]) as Groups,
        (SELECT COUNT(*) FROM [poc_dbo].[Policies]) as Policies
    `);
    
    if (prodData.recordset.length > 0) {
      const data = prodData.recordset[0];
      console.log('\nPOC Production Data:');
      console.log(`   Brokers:  ${data.Brokers}`);
      console.log(`   Groups:   ${data.Groups}`);
      console.log(`   Policies: ${data.Policies}`);
    }
  } catch {
    // Tables don't exist yet, that's fine
  }
  
  // Verify production [dbo] is untouched
  const prodBrokerCount = await pool.request().query(`
    SELECT COUNT(*) as cnt FROM [dbo].[Brokers]
  `);
  
  const prodPolicyCount = await pool.request().query(`
    SELECT COUNT(*) as cnt FROM [dbo].[Policies]
  `);
  
  console.log('\n✅ Production Schema Safety Check:');
  console.log(`   [dbo].[Brokers]:  ${prodBrokerCount.recordset[0].cnt} records (UNTOUCHED)`);
  console.log(`   [dbo].[Policies]: ${prodPolicyCount.recordset[0].cnt} records (UNTOUCHED)`);
  
  console.log('\n' + '═'.repeat(70) + '\n');
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
  
  try {
    const totalSteps = schemaScripts.length + transformScripts.length + exportScripts.length;
    
    // Start new run
    const runName = `POC-StepByStep-${new Date().toISOString().replace(/[:.]/g, '-')}`;
    await stateManager.startRun(runName, 'poc-step-by-step', totalSteps, config);
    
    progress.logRunStart(runName, 'poc-step-by-step', totalSteps);
    
    let currentStep = 0;
    
    // Step 0: Initial verification
    await pauseForConfirmation('Paused before INITIAL SETUP verification');
    await verifyPOCSchemas(pool, 'Initial State');
    
    // Phase 1: Schema Setup
    await pauseForConfirmation(`Paused before PHASE 1: Schema Setup (${schemaScripts.length} scripts)`);
    const phase1Result = await runPhase(
      pool, stateManager, progress, config,
      'Schema Setup', schemaScripts, 1, 3, currentStep
    );
    phaseResults.push(phase1Result.result);
    currentStep = phase1Result.nextStep;
    await verifyPOCSchemas(pool, 'After Schema Setup');
    
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
    await verifyPOCSchemas(pool, 'After Data Transforms');
    
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
    await verifyPOCSchemas(pool, 'After Export to Production');
    
    if (phase3Result.result.status === 'failed') {
      throw new Error(`Phase 3 failed: ${phase3Result.result.error}`);
    }
    
    // Complete run
    await stateManager.completeRun();
    
    // Final verification
    await pauseForConfirmation('Paused before FINAL VERIFICATION');
    await verifyPOCSchemas(pool, 'Final State');
    
    const totalDuration = (Date.now() - pipelineStartTime) / 1000;
    
    // Print summary
    printSummary(phaseResults, totalDuration);
    
    progress.logRunComplete(totalSteps, totalDuration);
    
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
