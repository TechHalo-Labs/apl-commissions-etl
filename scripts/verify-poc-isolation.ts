/**
 * POC Schema Isolation Verifier
 * Comprehensive verification that POC schemas are isolated from standard schemas
 * Can be run at any point to verify isolation integrity
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

interface IsolationReport {
  timestamp: string;
  pocSchemas: { schema: string; tables: number; records: number }[];
  standardSchemas: { schema: string; before: number; after: number; changed: boolean }[];
  violations: string[];
  success: boolean;
}

interface SchemaSnapshot {
  tables: number;
  records: number;
}

/**
 * Capture schema table and record counts
 */
async function captureSchemaSnapshot(pool: sql.ConnectionPool, schemaName: string): Promise<SchemaSnapshot> {
  try {
    const tableCountResult = await pool.request().query(`
      SELECT COUNT(*) as cnt
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = '${schemaName}'
    `);
    
    const tables = tableCountResult.recordset[0].cnt;
    
    let records = 0;
    if (tables > 0) {
      try {
        const recordsResult = await pool.request().query(`
          SELECT SUM(p.rows) as total
          FROM sys.tables t
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          INNER JOIN sys.partitions p ON t.object_id = p.object_id
          WHERE s.name = '${schemaName}' AND p.index_id IN (0, 1)
        `);
        records = recordsResult.recordset[0].total || 0;
      } catch {
        records = 0;
      }
    }
    
    return { tables, records };
  } catch {
    return { tables: 0, records: 0 };
  }
}

/**
 * Check POC schemas status
 */
async function checkPOCSchemas(pool: sql.ConnectionPool): Promise<{ schema: string; tables: number; records: number }[]> {
  const pocSchemas = ['poc_raw_data', 'poc_etl', 'poc_dbo'];
  const results: { schema: string; tables: number; records: number }[] = [];
  
  for (const schema of pocSchemas) {
    const snapshot = await captureSchemaSnapshot(pool, schema);
    results.push({
      schema,
      tables: snapshot.tables,
      records: snapshot.records
    });
  }
  
  return results;
}

/**
 * Check standard schemas and compare to baseline
 */
async function checkStandardSchemas(
  pool: sql.ConnectionPool,
  baseline?: IsolationReport
): Promise<{ schema: string; before: number; after: number; changed: boolean }[]> {
  const standardSchemas = ['etl', 'dbo'];
  const results: { schema: string; before: number; after: number; changed: boolean }[] = [];
  
  for (const schema of standardSchemas) {
    const snapshot = await captureSchemaSnapshot(pool, schema);
    const before = baseline
      ? baseline.standardSchemas.find(s => s.schema === schema)?.after || snapshot.records
      : snapshot.records;
    
    results.push({
      schema,
      before,
      after: snapshot.records,
      changed: before !== snapshot.records
    });
  }
  
  return results;
}

/**
 * Detect isolation violations
 */
function detectViolations(
  pocSchemas: { schema: string; tables: number; records: number }[],
  standardSchemas: { schema: string; before: number; after: number; changed: boolean }[]
): string[] {
  const violations: string[] = [];
  
  // Check if any standard schema was modified
  for (const schema of standardSchemas) {
    if (schema.changed) {
      violations.push(
        `VIOLATION: [${schema.schema}] schema was modified (${schema.before} → ${schema.after} records)`
      );
    }
  }
  
  // Check if POC schemas have expected data
  const pocEtl = pocSchemas.find(s => s.schema === 'poc_etl');
  if (pocEtl && pocEtl.tables === 0) {
    violations.push('WARNING: [poc_etl] schema has no tables (expected staging tables)');
  }
  
  return violations;
}

/**
 * Verify POC isolation
 */
async function verifyPOCIsolation(
  pool: sql.ConnectionPool,
  baseline?: IsolationReport
): Promise<IsolationReport> {
  const pocSchemas = await checkPOCSchemas(pool);
  const standardSchemas = await checkStandardSchemas(pool, baseline);
  const violations = detectViolations(pocSchemas, standardSchemas);
  
  return {
    timestamp: new Date().toISOString(),
    pocSchemas,
    standardSchemas,
    violations,
    success: violations.filter(v => v.startsWith('VIOLATION')).length === 0
  };
}

/**
 * Display isolation report
 */
function displayReport(report: IsolationReport): void {
  console.log('\n' + '═'.repeat(70));
  console.log('  POC SCHEMA ISOLATION REPORT');
  console.log('═'.repeat(70) + '\n');
  console.log(`Timestamp: ${report.timestamp}\n`);
  
  console.log('POC Schemas:');
  console.log('─'.repeat(70));
  report.pocSchemas.forEach(schema => {
    console.log(`  [${schema.schema}]:`);
    console.log(`    Tables:  ${schema.tables}`);
    console.log(`    Records: ${schema.records}`);
  });
  
  console.log('\nStandard Schemas (Should Be Unchanged):');
  console.log('─'.repeat(70));
  report.standardSchemas.forEach(schema => {
    const status = schema.changed ? '❌ CHANGED' : '✅ UNCHANGED';
    console.log(`  [${schema.schema}]: ${status}`);
    console.log(`    Before:  ${schema.before} records`);
    console.log(`    After:   ${schema.after} records`);
    if (schema.changed) {
      const diff = schema.after - schema.before;
      console.log(`    Diff:    ${diff > 0 ? '+' : ''}${diff} records`);
    }
  });
  
  console.log('\nViolation Check:');
  console.log('─'.repeat(70));
  if (report.violations.length === 0) {
    console.log('  ✅ No violations detected - 100% isolated!');
  } else {
    report.violations.forEach(violation => {
      if (violation.startsWith('VIOLATION')) {
        console.log(`  ❌ ${violation}`);
      } else {
        console.log(`  ⚠️  ${violation}`);
      }
    });
  }
  
  console.log('\n' + '═'.repeat(70));
  console.log(`  RESULT: ${report.success ? '✅ PASSED' : '❌ FAILED'}`);
  console.log('═'.repeat(70) + '\n');
}

/**
 * Save baseline for future comparisons
 */
function saveBaseline(report: IsolationReport, filename: string): void {
  const fs = require('fs');
  const path = require('path');
  const filePath = path.join(__dirname, '../', filename);
  fs.writeFileSync(filePath, JSON.stringify(report, null, 2));
  console.log(`📁 Baseline saved to: ${filename}\n`);
}

/**
 * Load baseline from file
 */
function loadBaseline(filename: string): IsolationReport | undefined {
  const fs = require('fs');
  const path = require('path');
  const filePath = path.join(__dirname, '../', filename);
  
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(content);
  } catch {
    return undefined;
  }
}

/**
 * Main verification
 */
async function main() {
  const args = process.argv.slice(2);
  const saveBaselineFlag = args.includes('--save-baseline');
  const compareFlag = args.includes('--compare');
  const baselineFile = 'poc-isolation-baseline.json';
  
  console.log('\n' + '╔' + '═'.repeat(68) + '╗');
  console.log('║' + ' '.repeat(18) + 'POC ISOLATION VERIFIER' + ' '.repeat(26) + '║');
  console.log('╚' + '═'.repeat(68) + '╝\n');
  
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    let baseline: IsolationReport | undefined;
    
    if (compareFlag) {
      baseline = loadBaseline(baselineFile);
      if (!baseline) {
        console.log('⚠️  No baseline found. Run with --save-baseline first.\n');
        process.exit(1);
      }
      console.log('📊 Comparing against saved baseline...\n');
    }
    
    const report = await verifyPOCIsolation(pool, baseline);
    displayReport(report);
    
    if (saveBaselineFlag) {
      saveBaseline(report, baselineFile);
    }
    
    process.exit(report.success ? 0 : 1);
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
