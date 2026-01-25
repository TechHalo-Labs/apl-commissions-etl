/**
 * SQL Server ETL Pipeline Orchestrator
 * =====================================
 * Runs the complete ETL pipeline for commission calculation
 * 
 * Usage:
 *   npx tsx scripts/run-pipeline.ts [options]
 * 
 * Options:
 *   --restore-backup   Restore raw data from raw_data schema backup (instead of CSV)
 *   --skip-schema      Skip schema setup (preserves existing raw/staging tables)
 *   --skip-ingest      Skip CSV data ingestion (use with --restore-backup or existing data)
 *   --skip-transform   Skip transform phase
 *   --skip-calc        Skip commission calculation phase
 *   --skip-export      Skip export to production phase
 *   --skip-grace-period-fix Skip grace period date fix (Bug #36)
 * 
 * Recommended usage for full pipeline with backup data:
 *   npx tsx scripts/run-pipeline.ts --restore-backup --skip-calc
 * 
 * Pipeline Stages:
 * ----------------
 * STEP 1: Schema Setup
 *   - Creates/truncates etl schema tables (raw, input, staging, calc)
 * 
 * STEP 2: Data Ingestion
 *   - Either: Restore from raw_data schema backup (--restore-backup)
 *   - Or: Ingest from CSV files
 *   - Populates input tables from raw data
 * 
 * STEP 3: Transforms (18 scripts)
 *   Phase 1: Reference tables (00-references.sql)
 *   Phase 2: Core entities (01-brokers, 02-groups, 03-products, 04-schedules)
 *   Phase 3: Tiered proposal creation
 *     - 06a: Simple groups (single config) → Create proposals
 *     - 06b: Non-conformant (multi-config per key) → PolicyHierarchyAssignments
 *     - 06c: Plan-differentiated proposals
 *     - 06d: Year-differentiated proposals
 *     - 06e: Granular proposals for remainder
 *     - 06f: Consolidate proposals (merge by config hash)
 *   Phase 4: Hierarchies (07-hierarchies, 08-hierarchy-splits)
 *   Phase 5: Policies and transactions (09-policies, 10-premium-transactions)
 *   Phase 6: Policy hierarchy assignments (11-policy-hierarchy-assignments)
 *   Phase 7: Additional entities (11-fees, 12-broker-banking-infos)
 * 
 * STEP 4: Commission Calculation (optional, usually skipped for ETL-only)
 * 
 * STEP 5: Export to Production (18 scripts in dependency order)
 *   - Brokers, Groups, Products, Plans
 *   - Schedules, Schedule Rates, Special Rates
 *   - Proposals, Hierarchies, Hierarchy Splits
 *   - Policies, Premium Splits, Premium Transactions
 *   - Policy Hierarchy Assignments (for non-conformant policies)
 *   - Commission Assignments (pass-through logic)
 *   - Broker Licenses, EO Insurance, Banking Info
 * 
 * STEP 6: Post-Export Data Fixes
 *   - Fix grace period dates (Bug #36): Corrects far-future expiration dates
 *     in BrokerLicenses, BrokerAppointments, and BrokerEOInsurances
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';

// =============================================================================
// Configuration
// =============================================================================

/**
 * Parse a SQL Server connection string into mssql config
 * Format: Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=...;Encrypt=...;
 */
function parseConnectionString(connStr: string): Partial<sql.config> {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'] || parts['user'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: parts['encrypt']?.toLowerCase() !== 'false',
      trustServerCertificate: parts['trustservercertificate']?.toLowerCase() === 'true',
    }
  };
}

/**
 * Get SQL Server configuration from environment
 * REQUIRES: $SQLSERVER connection string OR individual env vars
 * NO DEFAULTS - Will exit with error if not configured
 */
function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  
  if (connStr) {
    const parsed = parseConnectionString(connStr);
    if (!parsed.server || !parsed.database || !parsed.user || !parsed.password) {
      console.error('❌ Invalid $SQLSERVER connection string. Expected format:');
      console.error('   Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;');
      process.exit(1);
    }
    return {
      server: parsed.server,
      database: parsed.database,
      user: parsed.user,
      password: parsed.password,
      options: {
        encrypt: parsed.options?.encrypt ?? true,
        trustServerCertificate: parsed.options?.trustServerCertificate ?? true,
      },
      requestTimeout: 300000,
      connectionTimeout: 30000,
    };
  }
  
  // Fall back to individual environment variables
  const server = process.env.SQLSERVER_HOST;
  const database = process.env.SQLSERVER_DATABASE;
  const user = process.env.SQLSERVER_USER;
  const password = process.env.SQLSERVER_PASSWORD;
  
  if (!server || !database || !user || !password) {
    console.error('');
    console.error('❌ SQL Server connection not configured!');
    console.error('');
    console.error('Please set one of the following:');
    console.error('');
    console.error('Option 1: Single connection string (recommended)');
    console.error('  export SQLSERVER="Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;"');
    console.error('');
    console.error('Option 2: Individual environment variables');
    console.error('  export SQLSERVER_HOST=<host>');
    console.error('  export SQLSERVER_DATABASE=<db>');
    console.error('  export SQLSERVER_USER=<user>');
    console.error('  export SQLSERVER_PASSWORD=<pwd>');
    console.error('');
    process.exit(1);
  }
  
  return {
    server,
    database,
    user,
    password,
    options: {
      encrypt: true,
      trustServerCertificate: true,
    },
    requestTimeout: 300000,
    connectionTimeout: 30000,
  };
}

const config: sql.config = {
  ...getSqlConfig(),
  requestTimeout: 300000, // 5 minutes
  connectionTimeout: 30000,
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

const CSV_DATA_PATH = process.env.CSV_DATA_PATH || path.join(__dirname, '../../v4-etl/data');

// CSV file mapping to raw tables
// File names must match actual CSV files in rawdata directory
const csvFiles = {
  'raw_premiums': 'premiums.csv',
  'raw_commissions_detail': [
    'CommissionsDetail_20251101_20251115.csv',
    'CommissionsDetail_20251116_20251130.csv', 
    'CommissionsDetail_20251201_20251215.csv',
    'CommissionsDetail_20251216_20251231.csv',
  ],
  'raw_certificate_info': 'CertificateInfo.csv',
  'raw_individual_brokers': 'IndividualRosterExtract_20260107.csv',
  'raw_org_brokers': 'OrganizationRosterExtract_20260107.csv',
  'raw_licenses': 'BrokerLicenseExtract_20260107.csv',
  'raw_eo_insurance': 'BrokerEO_20260107.csv',
  'raw_schedule_rates': 'perf.csv',
  'raw_fees': 'Fees_20260107.csv',
  'raw_perf_groups': 'perf-group.csv',
};

// SQL files in execution order
const sqlFiles = {
  schema: 'sql/00-schema-setup.sql',
  rawTables: 'sql/01-raw-tables.sql',
  inputTables: 'sql/02-input-tables.sql',
  stagingTables: 'sql/03-staging-tables.sql',
  calcTables: 'sql/calc/00-calc-tables.sql',
  transforms: [
    // Phase 1: Reference tables
    'sql/transforms/00-references.sql',
    
    // Phase 2: Core entities
    'sql/transforms/01-brokers.sql',
    'sql/transforms/02-groups.sql',
    'sql/transforms/03-products.sql',
    'sql/transforms/04-schedules.sql',
    
    // Phase 3: Tiered proposal creation (replaces old 06-proposals.sql)
    'sql/transforms/06a-proposals-simple-groups.sql',      // Step 1: Simple groups (1 config)
    'sql/transforms/06b-proposals-non-conformant.sql',     // Step 2: Non-conformant -> PolicyHierarchyAssignments
    'sql/transforms/06c-proposals-plan-differentiated.sql', // Step 3: Plan-differentiated proposals
    'sql/transforms/06d-proposals-year-differentiated.sql', // Step 4: Year-differentiated proposals
    'sql/transforms/06e-proposals-granular.sql',           // Step 5: Granular proposals for remainder
    'sql/transforms/06f-consolidate-proposals.sql',        // Step 6: Consolidate proposals
    'sql/transforms/06g-normalize-proposal-date-ranges.sql', // Step 7: Normalize effective date ranges
    
    // Phase 4: Hierarchies and splits
    'sql/transforms/07-hierarchies.sql',
    'sql/transforms/08-hierarchy-splits.sql',
    
    // Phase 5: Policies and transactions
    'sql/transforms/09-policies.sql',
    'sql/transforms/10-premium-transactions.sql',
    
    // Phase 6: Policy hierarchy assignments (for non-conformant policies)
    'sql/transforms/11-policy-hierarchy-assignments.sql',
    
    // Phase 7: Additional entities
    'sql/transforms/11-fees.sql',
    'sql/transforms/12-broker-banking-infos.sql',
  ],
  calculation: 'sql/calc/run-calculation.sql',
  exports: [
    // Export in dependency order:
    // 1. Independent entities first
    'sql/export/02-export-brokers.sql',
    'sql/export/05-export-groups.sql',
    'sql/export/06-export-products.sql',
    'sql/export/06a-export-plans.sql',
    'sql/export/01-export-schedules.sql',
    // 2. Entities that depend on brokers/groups
    'sql/export/07-export-proposals.sql',
    'sql/export/08-export-hierarchies.sql',
    // 3. Entities that depend on proposals/hierarchies
    'sql/export/09-export-policies.sql',
    'sql/export/11-export-splits.sql',
    // 4. Entities that depend on policies
    // 'sql/export/10-export-premium-transactions.sql',  // Disabled - PremiumTransactions managed separately
    'sql/export/14-export-policy-hierarchy-assignments.sql',
    // 5. Fee schedules (depends on groups and proposals)
    'sql/export/15-export-fee-schedules.sql',
    // 6. Additional broker-related entities
    'sql/export/12-export-assignments.sql',
    'sql/export/13-export-licenses.sql',
    'sql/export/16-export-broker-banking-infos.sql',
    // 7. Calculated results (commented out in files)
    'sql/export/03-export-gl-entries.sql',
    'sql/export/04-export-traceability.sql',
  ],
};

// =============================================================================
// Utility Functions
// =============================================================================

function log(message: string, level: 'info' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const prefix = {
    info: '  ',
    error: '❌',
    success: '✅',
  }[level];
  console.log(`[${timestamp}] ${prefix} ${message}`);
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  const fullPath = path.join(__dirname, '..', filePath);
  
  if (!fs.existsSync(fullPath)) {
    log(`SQL file not found: ${fullPath}`, 'error');
    throw new Error(`SQL file not found: ${fullPath}`);
  }
  
  const sqlContent = fs.readFileSync(fullPath, 'utf-8');
  
  // Split by GO statements (SQL Server batch separator)
  const batches = sqlContent
    .split(/^\s*GO\s*$/gim)
    .filter(batch => batch.trim().length > 0);
  
  log(`Executing ${filePath} (${batches.length} batches)...`);
  
  for (let i = 0; i < batches.length; i++) {
    try {
      await pool.request().batch(batches[i]);
    } catch (err: any) {
      log(`Error in batch ${i + 1}: ${err.message}`, 'error');
      throw err;
    }
  }
  
  log(`${filePath} completed`, 'success');
}

async function bulkInsertCsv(
  pool: sql.ConnectionPool, 
  tableName: string, 
  csvPath: string
): Promise<number> {
  if (!fs.existsSync(csvPath)) {
    log(`CSV file not found: ${csvPath}`, 'error');
    return 0;
  }
  
  return new Promise((resolve, reject) => {
    const records: any[] = [];
    
    fs.createReadStream(csvPath)
      .pipe(parse({ columns: true, skip_empty_lines: true, relax_column_count: true }))
      .on('data', (record) => records.push(record))
      .on('end', async () => {
        if (records.length === 0) {
          log(`No records in ${csvPath}`, 'info');
          resolve(0);
          return;
        }
        
        // Get column names from first record
        const columns = Object.keys(records[0]);
        
        // Insert in batches of 1000
        const batchSize = 1000;
        let inserted = 0;
        
        try {
          for (let i = 0; i < records.length; i += batchSize) {
            const batch = records.slice(i, i + batchSize);
            
            // Build bulk insert using table value constructor
            const values = batch.map(record => {
              const vals = columns.map(col => {
                const val = record[col];
                if (val === null || val === undefined || val === '') {
                  return 'NULL';
                }
                // Escape single quotes and wrap in quotes
                return `N'${String(val).replace(/'/g, "''")}'`;
              });
              return `(${vals.join(', ')})`;
            }).join(',\n');
            
            const insertSql = `
              INSERT INTO [etl].[${tableName}] (${columns.map(c => `[${c}]`).join(', ')})
              VALUES ${values}
            `;
            
            await pool.request().batch(insertSql);
            inserted += batch.length;
            
            if (i % 10000 === 0 && i > 0) {
              log(`  ${tableName}: ${inserted.toLocaleString()} rows inserted...`);
            }
          }
          
          resolve(inserted);
        } catch (err: any) {
          log(`Error inserting into ${tableName}: ${err.message}`, 'error');
          reject(err);
        }
      })
      .on('error', reject);
  });
}

// =============================================================================
// Pipeline Steps
// =============================================================================

async function setupSchema(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 1: Schema Setup');
  log('='.repeat(60));
  
  await executeSqlFile(pool, sqlFiles.schema);
  await executeSqlFile(pool, sqlFiles.rawTables);
  await executeSqlFile(pool, sqlFiles.inputTables);
  await executeSqlFile(pool, sqlFiles.stagingTables);
  await executeSqlFile(pool, sqlFiles.calcTables);
}

async function restoreFromBackup(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 2: Restore Raw Data from Backup (raw_data schema)');
  log('='.repeat(60));
  
  // Table mapping from raw_data schema to etl schema
  const backupTables = [
    'raw_certificate_info',
    'raw_commissions_detail',
    'raw_individual_brokers',
    'raw_org_brokers',
    'raw_licenses',
    'raw_eo_insurance',
    'raw_schedule_rates',
    'raw_perf_groups',
    'raw_premiums',
  ];
  
  for (const table of backupTables) {
    log(`Restoring ${table}...`);
    
    // Check if backup table exists and has data
    const checkResult = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM [raw_data].[${table}]
    `);
    const rowCount = checkResult.recordset[0].cnt;
    
    if (rowCount === 0) {
      log(`  ${table}: No data in backup, skipping`);
      continue;
    }
    
    // Truncate target table
    await pool.request().query(`TRUNCATE TABLE [etl].[${table}]`);
    
    // Copy data from backup
    const result = await pool.request().query(`
      INSERT INTO [etl].[${table}]
      SELECT * FROM [raw_data].[${table}]
    `);
    
    const rowsInserted = result.rowsAffected?.[0] ?? rowCount;
    log(`  ${table}: ${rowsInserted.toLocaleString()} rows restored`, 'success');
  }
  
  // Now populate input tables from raw tables
  log('');
  log('Populating input tables from raw data...');
  
  // Helper to get row count from result
  const getRowCount = (result: sql.IResult<any>) => result.rowsAffected?.[0] ?? 0;
  
  // input_certificate_info - filter for Active certificates and Active records only
  await pool.request().query(`TRUNCATE TABLE [etl].[input_certificate_info]`);
  const certResult = await pool.request().query(`
    INSERT INTO [etl].[input_certificate_info]
    SELECT * FROM [etl].[raw_certificate_info]
    WHERE LTRIM(RTRIM(CertStatus)) = 'A'  -- Active certificates only (exclude Lapsed 'L' and Pending 'P')
      AND LTRIM(RTRIM(RecStatus)) = 'A'   -- Active split configurations only (exclude historical/deleted)
  `);
  log(`  input_certificate_info: ${getRowCount(certResult).toLocaleString()} rows (Active only)`, 'success');
  
  // input_commission_details - need to handle type conversion from nvarchar
  await pool.request().query(`TRUNCATE TABLE [etl].[input_commission_details]`);
  const commResult = await pool.request().query(`
    INSERT INTO [etl].[input_commission_details] (
      Company, CertificateId, CertEffectiveDate, SplitBrokerId, PmtPostedDate,
      PaidToDate, PaidAmount, TransActionType, InvoiceNumber, CertInForceMonths,
      CommissionRate, RealCommissionRate, PaidBrokerId, TransactionId
    )
    SELECT 
      Company, 
      TRY_CAST(CertificateId AS BIGINT) AS CertificateId, 
      TRY_CAST(CertEffectiveDate AS DATE) AS CertEffectiveDate, 
      SplitBrokerId, 
      TRY_CAST(PmtPostedDate AS DATE) AS PmtPostedDate,
      TRY_CAST(PaidToDate AS DATE) AS PaidToDate, 
      TRY_CAST(PaidAmount AS DECIMAL(18,2)) AS PaidAmount, 
      TransActionType, 
      InvoiceNumber, 
      TRY_CAST(CertInForceMonths AS INT) AS CertInForceMonths,
      TRY_CAST(CommissionRate AS DECIMAL(18,4)) AS CommissionRate, 
      TRY_CAST(RealCommissionRate AS DECIMAL(18,4)) AS RealCommissionRate, 
      PaidBrokerId, 
      TransactionId
    FROM [etl].[raw_commissions_detail]
  `);
  log(`  input_commission_details: ${getRowCount(commResult).toLocaleString()} rows`, 'success');
  
  // Note: Other input tables (input_individual_brokers, input_org_brokers, etc.)
  // are no longer created by 02-input-tables.sql. The transforms use raw_* tables directly.
  log('  (Transforms will use raw_* tables for brokers, licenses, schedules, etc.)');
  log('Input table population complete', 'success');
  
  log('Input tables populated', 'success');
}

async function ingestCsvData(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 2: CSV Data Ingestion');
  log('='.repeat(60));
  
  for (const [table, files] of Object.entries(csvFiles)) {
    const fileList = Array.isArray(files) ? files : [files];
    let totalRows = 0;
    
    for (const file of fileList) {
      const csvPath = path.join(CSV_DATA_PATH, file);
      if (fs.existsSync(csvPath)) {
        log(`Loading ${file} into ${table}...`);
        const rows = await bulkInsertCsv(pool, table, csvPath);
        totalRows += rows;
        log(`  ${file}: ${rows.toLocaleString()} rows`, 'success');
      }
    }
    
    if (totalRows > 0) {
      log(`Total ${table}: ${totalRows.toLocaleString()} rows`, 'success');
    }
  }
  
  // Process input tables
  await executeSqlFile(pool, sqlFiles.inputTables);
}

async function runTransforms(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 3: Data Transforms');
  log('='.repeat(60));
  
  for (const transformFile of sqlFiles.transforms) {
    await executeSqlFile(pool, transformFile);
  }
}

async function runCalculation(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 4: Commission Calculation');
  log('='.repeat(60));
  
  await executeSqlFile(pool, sqlFiles.calculation);
}

async function exportResults(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('STEP 5: Export to Production');
  log('='.repeat(60));
  
  for (const exportFile of sqlFiles.exports) {
    try {
      await executeSqlFile(pool, exportFile);
    } catch (err: any) {
      // Log but continue with other exports if one fails
      log(`Warning: ${exportFile} failed: ${err.message}`, 'error');
    }
  }
}

async function fixGracePeriodDates(pool: sql.ConnectionPool): Promise<void> {
  log('');
  log('='.repeat(60));
  log('POST-EXPORT: Fix Grace Period Dates (Bug #36)');
  log('='.repeat(60));
  
  try {
    // Import and execute the fix script
    const { spawn } = require('child_process');
    const scriptPath = path.resolve(__dirname, 'fix-grace-period-dates.ts');
    
    log(`Executing grace period date fix script...`);
    
    return new Promise((resolve, reject) => {
      const child = spawn('npx', ['tsx', scriptPath], {
        stdio: 'inherit',
        shell: true,
        cwd: path.resolve(__dirname, '..'),
      });
      
      child.on('close', (code: number) => {
        if (code === 0) {
          log('Grace period date fix completed', 'success');
          resolve();
        } else {
          log(`Grace period date fix failed with code ${code}`, 'error');
          reject(new Error(`Grace period fix script exited with code ${code}`));
        }
      });
      
      child.on('error', (err: Error) => {
        log(`Error running grace period fix script: ${err.message}`, 'error');
        reject(err);
      });
    });
  } catch (err: any) {
    log(`Grace period date fix failed: ${err.message}`, 'error');
    throw err;
  }
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const skipSchema = args.includes('--skip-schema');
  const skipIngest = args.includes('--skip-ingest');
  const restoreBackup = args.includes('--restore-backup');
  const skipTransform = args.includes('--skip-transform');
  const skipCalc = args.includes('--skip-calc');
  const skipExport = args.includes('--skip-export');
  const skipGracePeriodFix = args.includes('--skip-grace-period-fix');
  
  log('');
  log('='.repeat(60));
  log('SQL Server ETL Pipeline');
  log('='.repeat(60));
  log(`Server: ${config.server}`);
  log(`Database: ${config.database}`);
  if (restoreBackup) {
    log('Data Source: raw_data schema backup');
  } else {
    log(`Data Source: CSV files from ${CSV_DATA_PATH}`);
  }
  log('');
  
  let pool: sql.ConnectionPool | null = null;
  
  try {
    // Connect to SQL Server
    log('Connecting to SQL Server...');
    pool = await sql.connect(config);
    log('Connected', 'success');
    
    // Run pipeline steps
    if (!skipSchema) {
      await setupSchema(pool);
    } else {
      log('Skipping schema setup (--skip-schema)');
    }
    
    // Data ingestion: either from backup or CSV
    if (restoreBackup) {
      await restoreFromBackup(pool);
    } else if (!skipIngest) {
      await ingestCsvData(pool);
    } else {
      log('Skipping data ingestion (--skip-ingest)');
    }
    
    if (!skipTransform) {
      await runTransforms(pool);
    } else {
      log('Skipping transforms (--skip-transform)');
    }
    
    if (!skipCalc) {
      await runCalculation(pool);
    } else {
      log('Skipping calculation (--skip-calc)');
    }
    
    if (!skipExport) {
      await exportResults(pool);
    } else {
      log('Skipping export (--skip-export)');
    }
    
    // Post-export data fixes (Bug #36)
    if (!skipGracePeriodFix) {
      await fixGracePeriodDates(pool);
    } else {
      log('Skipping grace period date fix (--skip-grace-period-fix)');
    }
    
    log('');
    log('='.repeat(60));
    log('PIPELINE COMPLETED SUCCESSFULLY', 'success');
    log('='.repeat(60));
    
  } catch (err: any) {
    log(`Pipeline failed: ${err.message}`, 'error');
    console.error(err);
    process.exit(1);
  } finally {
    if (pool) {
      await pool.close();
      log('Connection closed');
    }
  }
}

main();

