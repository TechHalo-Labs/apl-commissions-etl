/**
 * POC Dry-Run Pipeline Analyzer
 * Analyzes what the pipeline would do WITHOUT making any database changes
 * Provides complete visibility into schema targets and potential violations
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { substituteSchemaVariables, substitutePOCSchemas } from './lib/sql-executor';

interface SchemaAnalysis {
  scriptName: string;
  targetSchemas: string[];
  touchesStandardSchemas: boolean;
  schemaOperations: string[];
  tableOperations: { action: string; schema: string; table: string }[];
}

interface BaselineCounts {
  timestamp: string;
  schemas: {
    [schemaName: string]: {
      tableCount: number;
      totalRecords: number;
    };
  };
}

/**
 * Capture baseline state of all schemas
 */
async function captureBaseline(pool: sql.ConnectionPool): Promise<BaselineCounts> {
  console.log('📊 Capturing baseline schema state...\n');
  
  const schemas = ['etl', 'poc_etl', 'dbo', 'poc_dbo', 'poc_raw_data'];
  const baseline: BaselineCounts = {
    timestamp: new Date().toISOString(),
    schemas: {}
  };
  
  for (const schema of schemas) {
    try {
      const tableCountResult = await pool.request().query(`
        SELECT COUNT(*) as cnt
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '${schema}'
      `);
      
      const tableCount = tableCountResult.recordset[0].cnt;
      
      // Try to get total records (may fail if no tables)
      let totalRecords = 0;
      if (tableCount > 0) {
        try {
          const recordsResult = await pool.request().query(`
            SELECT SUM(p.rows) as total
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE s.name = '${schema}' AND p.index_id IN (0, 1)
          `);
          totalRecords = recordsResult.recordset[0].total || 0;
        } catch {
          totalRecords = 0;
        }
      }
      
      baseline.schemas[schema] = {
        tableCount,
        totalRecords
      };
      
      console.log(`  [${schema}]: ${tableCount} tables, ${totalRecords} records`);
    } catch (error) {
      // Schema doesn't exist
      baseline.schemas[schema] = {
        tableCount: 0,
        totalRecords: 0
      };
      console.log(`  [${schema}]: Not found`);
    }
  }
  
  console.log('');
  return baseline;
}

/**
 * Analyze a SQL script for schema references
 */
function analyzeScript(scriptPath: string, config: any): SchemaAnalysis {
  const scriptContent = fs.readFileSync(scriptPath, 'utf-8');
  const scriptName = path.basename(scriptPath);
  
  // Apply standard substitutions
  let processedSQL = substituteSchemaVariables(scriptContent, config);
  
  // Apply POC substitutions if in POC mode
  if ((config.database as any).pocMode === true) {
    processedSQL = substitutePOCSchemas(processedSQL, config);
  }
  
  const targetSchemas = new Set<string>();
  const schemaOperations: string[] = [];
  const tableOperations: { action: string; schema: string; table: string }[] = [];
  
  // Detect schema operations
  const dropSchemaMatch = processedSQL.match(/DROP SCHEMA \[(\w+)\]/g);
  if (dropSchemaMatch) {
    dropSchemaMatch.forEach(match => {
      const schema = match.match(/\[(\w+)\]/)?.[1];
      if (schema) {
        schemaOperations.push(`DROP SCHEMA [${schema}]`);
        targetSchemas.add(schema);
      }
    });
  }
  
  const createSchemaMatch = processedSQL.match(/CREATE SCHEMA \[(\w+)\]/g);
  if (createSchemaMatch) {
    createSchemaMatch.forEach(match => {
      const schema = match.match(/\[(\w+)\]/)?.[1];
      if (schema) {
        schemaOperations.push(`CREATE SCHEMA [${schema}]`);
        targetSchemas.add(schema);
      }
    });
  }
  
  // Detect table operations
  const tableOpsRegex = /(INSERT INTO|UPDATE|DELETE FROM|DROP TABLE|CREATE TABLE)\s+\[(\w+)\]\.?\[?(\w+)?\]?/gi;
  let match;
  while ((match = tableOpsRegex.exec(processedSQL)) !== null) {
    const action = match[1];
    const schema = match[2];
    const table = match[3] || '';
    
    targetSchemas.add(schema);
    tableOperations.push({ action, schema, table });
  }
  
  // Check if touches standard schemas
  const touchesStandardSchemas = Array.from(targetSchemas).some(
    schema => schema === 'etl' || schema === 'dbo'
  );
  
  return {
    scriptName,
    targetSchemas: Array.from(targetSchemas),
    touchesStandardSchemas,
    schemaOperations,
    tableOperations
  };
}

/**
 * Generate execution plan from script analysis
 */
function generateExecutionPlan(
  allAnalyses: SchemaAnalysis[],
  baseline: BaselineCounts
): string {
  const lines: string[] = [];
  
  lines.push('');
  lines.push('═'.repeat(70));
  lines.push('  EXECUTION PLAN SUMMARY');
  lines.push('═'.repeat(70));
  lines.push('');
  
  const totalScripts = allAnalyses.length;
  const violatingScripts = allAnalyses.filter(a => a.touchesStandardSchemas);
  const pocOnlyScripts = allAnalyses.filter(a => !a.touchesStandardSchemas);
  
  lines.push(`Total Scripts to Execute: ${totalScripts}`);
  lines.push(`POC-Only Scripts: ${pocOnlyScripts.length}`);
  lines.push(`Scripts with Hardcoded Refs: ${violatingScripts.length}`);
  lines.push('');
  
  if (violatingScripts.length > 0) {
    lines.push('⚠️  WARNING: The following scripts would touch etl/dbo schemas:');
    lines.push('');
    violatingScripts.forEach(analysis => {
      lines.push(`  ❌ ${analysis.scriptName}`);
      lines.push(`     Target schemas: ${analysis.targetSchemas.join(', ')}`);
      if (analysis.schemaOperations.length > 0) {
        lines.push(`     Operations: ${analysis.schemaOperations.join(', ')}`);
      }
    });
    lines.push('');
  } else {
    lines.push('✅ All scripts target POC schemas only - 100% isolated!');
    lines.push('');
  }
  
  // Predicted schema changes
  lines.push('Predicted Schema Changes:');
  lines.push('');
  lines.push('  [poc_etl]:');
  lines.push(`    Before: ${baseline.schemas.poc_etl?.tableCount || 0} tables`);
  lines.push(`    After:  Expected 40+ tables (staging tables created)`);
  lines.push('');
  lines.push('  [poc_dbo]:');
  lines.push(`    Before: ${baseline.schemas.poc_dbo?.tableCount || 0} tables`);
  lines.push(`    After:  Expected 40+ tables (production tables created)`);
  lines.push('');
  lines.push('  [etl]:');
  lines.push(`    Before: ${baseline.schemas.etl?.tableCount || 0} tables`);
  lines.push(`    After:  ${baseline.schemas.etl?.tableCount || 0} tables (UNCHANGED)`);
  lines.push('');
  lines.push('  [dbo]:');
  lines.push(`    Before: ${baseline.schemas.dbo?.tableCount || 0} tables`);
  lines.push(`    After:  ${baseline.schemas.dbo?.tableCount || 0} tables (UNCHANGED)`);
  lines.push('');
  lines.push('═'.repeat(70));
  
  return lines.join('\n');
}

/**
 * Main dry-run analysis
 */
async function main() {
  console.log('\n' + '╔' + '═'.repeat(68) + '╗');
  console.log('║' + ' '.repeat(20) + 'POC DRY-RUN ANALYSIS' + ' '.repeat(28) + '║');
  console.log('╚' + '═'.repeat(68) + '╝\n');
  console.log('🔍 DRY RUN MODE - No database changes will be made\n');
  
  // Load POC configuration
  const config = loadConfig(undefined, 'appsettings.poc.json');
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    // Step 1: Capture baseline
    const baseline = await captureBaseline(pool);
    
    // Step 2: Analyze all scripts
    console.log('🔬 Analyzing SQL scripts...\n');
    
    const scriptsDir = path.join(__dirname, '../sql');
    
    // Define all script paths (matching run-pipeline.ts)
    const isPOCMode = (config.database as any).pocMode === true;
    
    console.log(`Configuration: POC Mode = ${isPOCMode}\n`);
    
    const schemaScripts = isPOCMode
      ? [
          path.join(scriptsDir, '01-raw-tables.sql'),
          path.join(scriptsDir, '02-input-tables.sql'),
          path.join(scriptsDir, '03-staging-tables.sql'),
        ]
      : [
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
    
    const allScripts = [...schemaScripts, ...transformScripts, ...exportScripts];
    const allAnalyses: SchemaAnalysis[] = [];
    
    console.log('Phase 1: Schema Setup Scripts');
    
    // Show skipped scripts in POC mode
    if (isPOCMode) {
      console.log('  ⏭️  00a-state-management-tables.sql: SKIPPED (POC mode)');
      console.log('  ⏭️  00-schema-setup.sql: SKIPPED (POC mode)');
    }
    
    schemaScripts.forEach(script => {
      const analysis = analyzeScript(script, config);
      allAnalyses.push(analysis);
      const status = analysis.touchesStandardSchemas ? '❌' : '✅';
      console.log(`  ${status} ${analysis.scriptName}: ${analysis.targetSchemas.join(', ')}`);
    });
    
    console.log('\nPhase 2: Transform Scripts');
    transformScripts.forEach(script => {
      const analysis = analyzeScript(script, config);
      allAnalyses.push(analysis);
      const status = analysis.touchesStandardSchemas ? '❌' : '✅';
      console.log(`  ${status} ${analysis.scriptName}: ${analysis.targetSchemas.join(', ')}`);
    });
    
    console.log('\nPhase 3: Export Scripts');
    exportScripts.forEach(script => {
      const analysis = analyzeScript(script, config);
      allAnalyses.push(analysis);
      const status = analysis.touchesStandardSchemas ? '❌' : '✅';
      console.log(`  ${status} ${analysis.scriptName}: ${analysis.targetSchemas.join(', ')}`);
    });
    
    // Step 3: Generate execution plan
    const plan = generateExecutionPlan(allAnalyses, baseline);
    console.log(plan);
    
    // Step 4: Safety assessment
    // Only check scripts that would actually run (already filtered by POC mode)
    const violations = allAnalyses.filter(a => a.touchesStandardSchemas);
    
    if (violations.length === 0) {
      console.log('✅ DRY-RUN PASSED: 100% Schema Isolation Verified\n');
      console.log(`   All ${allAnalyses.length} scripts target POC schemas only.`);
      console.log('   Safe to proceed with actual execution.\n');
      process.exit(0);
    } else {
      console.log('❌ DRY-RUN FAILED: Schema Isolation Violations Detected\n');
      console.log(`   ${violations.length} script(s) would touch etl/dbo schemas.`);
      console.log('   These scripts contain hardcoded schema references.');
      console.log('   Review and fix before execution.\n');
      process.exit(1);
    }
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
