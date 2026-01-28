/**
 * Run Ingest Phase Step-by-Step with Verification Pauses
 * 
 * Executes each ingest script individually and waits for user confirmation
 * before proceeding to the next step. Allows verification of results between steps.
 * 
 * Usage:
 *   npx tsx scripts/run-ingest-step-by-step.ts
 *   npx tsx scripts/run-ingest-step-by-step.ts --config appsettings.poc2.json
 * 
 * Features:
 * - Executes one script at a time
 * - Shows verification results after each step
 * - Pauses for user confirmation before continuing
 * - Can abort at any step
 * - Shows detailed row counts and data samples
 */

import * as sql from 'mssql';
import * as path from 'path';
import * as readline from 'readline';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { executeSQLScript } from './lib/sql-executor';

const scriptsDir = path.join(__dirname, '../sql');

interface IngestStep {
  name: string;
  script: string;
  description: string;
  purpose?: string;
  expectedResults?: string;
  testQueries?: string[];
  verification?: string;
}

const ingestSteps = [
  {
    name: 'Step 0: Create Schema',
    script: path.join(scriptsDir, '00-create-schema.sql'),
    description: 'Creates the ETL processing schema if it doesn\'t exist',
    purpose: 'Ensures the target schema exists before any table operations',
    expectedResults: 'Schema created or already exists message',
    testQueries: [
      `-- Verify schema exists`,
      `SELECT name FROM sys.schemas WHERE name = '$(ETL_SCHEMA)';`,
      ``,
      `-- Check schema permissions`,
      `SELECT s.name AS SchemaName, dp.name AS Owner`,
      `FROM sys.schemas s`,
      `JOIN sys.database_principals dp ON s.principal_id = dp.principal_id`,
      `WHERE s.name = '$(ETL_SCHEMA)';`
    ],
    verification: `
      -- Verify schema exists
      SELECT name AS SchemaName FROM sys.schemas WHERE name = '$(ETL_SCHEMA)';
    `
  },
  {
    name: 'Step 1: Copy Raw Data',
    script: path.join(scriptsDir, 'ingest/copy-from-poc-etl.sql'),
    description: 'Copies raw data from poc_etl to etl schema',
    purpose: 'Copy raw data tables from source schema to processing schema',
    expectedResults: 'All raw_* tables populated with data from source',
    testQueries: [
      `-- Check row counts for all raw tables`,
      `SELECT 'raw_certificate_info' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_certificate_info];`,
      `SELECT 'raw_schedule_rates' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_schedule_rates];`,
      `SELECT 'raw_perf_groups' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_perf_groups];`,
      `SELECT 'raw_premiums' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_premiums];`,
      `SELECT 'raw_individual_brokers' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_individual_brokers];`,
      `SELECT 'raw_org_brokers' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_org_brokers];`,
      ``,
      `-- Verify schedule data quality`,
      `SELECT TOP 3 CertificateId, GroupId, CommissionsSchedule, WritingBrokerID`,
      `FROM [$(ETL_SCHEMA)].[raw_certificate_info]`,
      `WHERE CommissionsSchedule IS NOT NULL`,
      `ORDER BY CertificateId;`
    ],
    verification: `
      -- Verify raw data copied successfully
      SELECT 'raw_certificate_info' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_certificate_info];
      SELECT 'raw_schedule_rates' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_schedule_rates];
      SELECT 'raw_perf_groups' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_perf_groups];
      SELECT 'raw_premiums' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_premiums];
      SELECT 'raw_individual_brokers' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_individual_brokers];
      SELECT 'raw_org_brokers' AS [table], COUNT(*) AS row_count FROM [$(ETL_SCHEMA)].[raw_org_brokers];
      
      -- Sample data check
      SELECT TOP 3 CertificateId, GroupId, CommissionsSchedule, WritingBrokerID 
      FROM [$(ETL_SCHEMA)].[raw_certificate_info] 
      WHERE CommissionsSchedule IS NOT NULL
      ORDER BY CertificateId;
    `
  },
  {
    name: 'Step 2: Populate Input Tables',
    script: path.join(scriptsDir, 'ingest/populate-input-tables.sql'),
    description: 'Populates input_certificate_info from raw_certificate_info',
    purpose: 'Transform raw certificate data into structured input tables',
    expectedResults: 'input_certificate_info table populated with cleaned certificate data',
    testQueries: [
      `-- Check input table statistics`,
      `SELECT`,
      `  'input_certificate_info' AS [table],`,
      `  COUNT(*) AS total_rows,`,
      `  COUNT(DISTINCT GroupId) AS unique_groups,`,
      `  COUNT(DISTINCT CommissionsSchedule) AS unique_schedules,`,
      `  COUNT(DISTINCT WritingBrokerID) AS unique_brokers`,
      `FROM [$(ETL_SCHEMA)].[input_certificate_info];`,
      ``,
      `-- Verify schedule coverage`,
      `SELECT TOP 5`,
      `  CommissionsSchedule,`,
      `  COUNT(*) AS certificate_count`,
      `FROM [$(ETL_SCHEMA)].[input_certificate_info]`,
      `WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule != ''`,
      `GROUP BY CommissionsSchedule`,
      `ORDER BY COUNT(*) DESC;`
    ],
    verification: `
      -- Verify input tables populated
      SELECT 
        'input_certificate_info' AS [table],
        COUNT(*) AS total_rows,
        COUNT(DISTINCT GroupId) AS unique_groups,
        COUNT(DISTINCT CommissionsSchedule) AS unique_schedules,
        COUNT(DISTINCT WritingBrokerID) AS unique_brokers
      FROM [$(ETL_SCHEMA)].[input_certificate_info];
      
      -- Verify schedules are referenced
      SELECT TOP 5
        CommissionsSchedule,
        COUNT(*) AS certificate_count
      FROM [$(ETL_SCHEMA)].[input_certificate_info]
      WHERE CommissionsSchedule IS NOT NULL AND CommissionsSchedule != ''
      GROUP BY CommissionsSchedule
      ORDER BY COUNT(*) DESC;
    `
  }
];

/**
 * Prompt user for confirmation
 */
function askToContinue(): Promise<boolean> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  return new Promise((resolve) => {
    rl.question('\nContinue to next step? (y/n): ', (answer) => {
      rl.close();
      resolve(answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes');
    });
  });
}

/**
 * Execute verification query and display results
 */
async function runVerification(pool: sql.ConnectionPool, verificationSQL: string, schemaName: string) {
  console.log('\n┌─────────────────────────────────────────────┐');
  console.log('│         VERIFICATION RESULTS                │');
  console.log('└─────────────────────────────────────────────┘\n');
  
  try {
    // Replace schema variable with actual schema name
    const actualSQL = verificationSQL.replace(/\$\(ETL_SCHEMA\)/g, schemaName);
    
    const result = await pool.request().query(actualSQL);
    
    // Display each recordset
    if (Array.isArray(result.recordsets)) {
      result.recordsets.forEach((recordset, index) => {
        if (recordset.length > 0) {
          console.log(`\nResult Set ${index + 1}:`);
          console.table(recordset);
        }
      });
    }
  } catch (error: any) {
    console.error('⚠️  Verification query failed:', error.message);
  }
}

/**
 * Main step-by-step execution
 */
async function main() {
  const args = process.argv.slice(2);
  const configFile = args.includes('--config') ? args[args.indexOf('--config') + 1] : undefined;
  
  const config = loadConfig(undefined, configFile);
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════════════');
    console.log('        INGEST PHASE - STEP-BY-STEP EXECUTION          ');
    console.log('════════════════════════════════════════════════════════\n');
    console.log('This script will execute each ingest step individually.');
    console.log('After each step, verification results will be shown.');
    console.log('You can review the results before continuing.\n');
    console.log(`📚 For detailed step descriptions and test queries, see:`);
    console.log(`   STEP-BY-STEP-TEST-GUIDE.md`);
    console.log(`\nTotal steps: ${ingestSteps.length}`);
    console.log('════════════════════════════════════════════════════════\n');
    
    for (let i = 0; i < ingestSteps.length; i++) {
      const step = ingestSteps[i];
      
      console.log('\n╔════════════════════════════════════════════════════════╗');
      console.log(`║  ${step.name.padEnd(54)}║`);
      console.log('╚════════════════════════════════════════════════════════╝');
      console.log(`\n📋 Description: ${step.description}`);
      if (step.purpose) {
        console.log(`🎯 Purpose: ${step.purpose}`);
      }
      if (step.expectedResults) {
        console.log(`✅ Expected: ${step.expectedResults}`);
      }
      console.log(`📄 Script: ${path.basename(step.script)}\n`);
      
      // Execute the script
      console.log('⏳ Executing...\n');
      const startTime = Date.now();
      
      try {
        const result = await executeSQLScript({
          config,
          pool,
          scriptPath: step.script,
          stepId: `manual-step-${i + 1}`,
          debugMode: false,
          pocMode: config.database.pocMode === true
        });
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\n✅ Step completed in ${duration}s`);
        if (result.recordsAffected && result.recordsAffected > 0) {
          console.log(`📊 Records affected: ${result.recordsAffected.toLocaleString()}`);
        }
        
        // Run verification
        if (step.verification) {
          await runVerification(pool, step.verification, config.database.schemas.processing);
        }
        
        // Show test queries
        if (step.testQueries && step.testQueries.length > 0) {
          console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
          console.log('🔍 HOW TO TEST RESULTS:');
          console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
          console.log('\nCopy and run these queries to verify data quality:\n');
          
          // Replace schema variables with actual schema name
          const processedQueries = step.testQueries.map(q => 
            q.replace(/\$\(ETL_SCHEMA\)/g, config.database.schemas.processing)
             .replace(/\$\(SOURCE_SCHEMA\)/g, config.database.schemas.source)
          );
          
          console.log(processedQueries.join('\n'));
          console.log('');
        }
        
        // Ask to continue (except after last step)
        if (i < ingestSteps.length - 1) {
          const shouldContinue = await askToContinue();
          
          if (!shouldContinue) {
            console.log('\n⏸️  Execution paused by user.');
            console.log('You can resume by running this script again (it will skip completed steps).\n');
            process.exit(0);
          }
        }
        
      } catch (error: any) {
        console.error(`\n❌ Step failed: ${error.message}`);
        console.error('\nYou can:');
        console.error('  1. Fix the issue');
        console.error('  2. Re-run this script to retry from this step');
        console.error('  3. Run individual SQL script: sqlcmd -i ' + step.script);
        process.exit(1);
      }
    }
    
    console.log('\n════════════════════════════════════════════════════════');
    console.log('         ✅ ALL INGEST STEPS COMPLETED!                ');
    console.log('════════════════════════════════════════════════════════\n');
    console.log('Next steps:');
    console.log('  1. Run transforms: npx tsx scripts/run-pipeline.ts --skip-ingest');
    console.log('  2. Or continue with full pipeline: npx tsx scripts/run-pipeline.ts --skip-ingest --skip-schema\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
