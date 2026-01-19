/**
 * ETL Export with Verification
 * =============================
 * Exports staging data to production with full verification and progress reporting.
 * 
 * Usage:
 *   npx tsx scripts/export-with-verification.ts [options]
 * 
 * Options:
 *   --dry-run     Only capture counts, don't run exports
 *   --table <n>   Export only specific table(s) by name
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

// =============================================================================
// Configuration
// =============================================================================
const config: sql.config = {
  server: process.env.SQLSERVER_HOST || 'halo-sql.database.windows.net',
  database: process.env.SQLSERVER_DATABASE || 'halo-sqldb',
  user: process.env.SQLSERVER_USER || '***REMOVED***',
  password: process.env.SQLSERVER_PASSWORD || '***REMOVED***',
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  requestTimeout: 600000, // 10 minutes for large exports
  connectionTimeout: 30000,
};

// =============================================================================
// Table Mapping Configuration
// =============================================================================
interface ExportConfig {
  name: string;
  scriptPath: string;
  productionTables: string[];
  stagingTables: string[];
}

const exportConfigs: ExportConfig[] = [
  {
    name: 'Brokers',
    scriptPath: 'sql/export/02-export-brokers.sql',
    productionTables: ['Brokers', 'BrokerAppointments'],
    stagingTables: ['stg_brokers'],
  },
  {
    name: 'Groups',
    scriptPath: 'sql/export/05-export-groups.sql',
    productionTables: ['Group'],
    stagingTables: ['stg_groups'],
  },
  {
    name: 'Products',
    scriptPath: 'sql/export/06-export-products.sql',
    productionTables: ['Products', 'ProductCodes'],
    stagingTables: ['stg_products', 'stg_product_codes'],
  },
  {
    name: 'Plans',
    scriptPath: 'sql/export/06a-export-plans.sql',
    productionTables: ['Plans'],
    stagingTables: ['stg_plans'],
  },
  {
    name: 'Schedules',
    scriptPath: 'sql/export/01-export-schedules.sql',
    productionTables: ['Schedules', 'ScheduleVersions', 'ScheduleRates'],
    stagingTables: ['stg_schedules', 'stg_schedule_versions', 'stg_schedule_rates'],
  },
  {
    name: 'Proposals',
    scriptPath: 'sql/export/07-export-proposals.sql',
    productionTables: ['Proposals', 'ProposalProducts'],
    stagingTables: ['stg_proposals', 'stg_proposal_products'],
  },
  {
    name: 'Hierarchies',
    scriptPath: 'sql/export/08-export-hierarchies.sql',
    productionTables: ['Hierarchies', 'HierarchyVersions', 'HierarchyParticipants', 'StateRules', 'StateRuleStates', 'HierarchySplits'],
    stagingTables: ['stg_hierarchies', 'stg_hierarchy_versions', 'stg_hierarchy_participants', 'stg_state_rules', 'stg_state_rule_states', 'stg_hierarchy_splits'],
  },
  {
    name: 'Policies',
    scriptPath: 'sql/export/09-export-policies.sql',
    productionTables: ['Policies'],
    stagingTables: ['stg_policies'],
  },
  {
    name: 'PremiumSplits',
    scriptPath: 'sql/export/11-export-splits.sql',
    productionTables: ['PremiumSplitVersions', 'PremiumSplitParticipants'],
    stagingTables: ['stg_premium_split_versions', 'stg_premium_split_participants'],
  },
  // PremiumTransactions disabled - managed separately, not via ETL
  // {
  //   name: 'PremiumTransactions',
  //   scriptPath: 'sql/export/10-export-premium-transactions.sql',
  //   productionTables: ['PremiumTransactions'],
  //   stagingTables: ['stg_premium_transactions'],
  // },
  {
    name: 'PolicyHierarchyAssignments',
    scriptPath: 'sql/export/14-export-policy-hierarchy-assignments.sql',
    productionTables: ['PolicyHierarchyAssignments'],
    stagingTables: ['stg_policy_hierarchy_assignments'],
  },
  {
    name: 'FeeSchedules',
    scriptPath: 'sql/export/15-export-fee-schedules.sql',
    productionTables: ['FeeSchedules', 'FeeScheduleVersions', 'FeeScheduleItems'],
    stagingTables: ['stg_fee_schedules'],
  },
  {
    name: 'CommissionAssignments',
    scriptPath: 'sql/export/12-export-assignments.sql',
    productionTables: ['CommissionAssignmentVersions', 'CommissionAssignmentRecipients'],
    stagingTables: ['stg_commission_assignment_versions', 'stg_commission_assignment_recipients'],
  },
  {
    name: 'BrokerLicenses',
    scriptPath: 'sql/export/13-export-licenses.sql',
    productionTables: ['BrokerLicenses', 'BrokerEOInsurances'],
    stagingTables: ['stg_broker_licenses', 'stg_broker_eo_insurances'],
  },
  {
    name: 'BrokerBankingInfos',
    scriptPath: 'sql/export/16-export-broker-banking-infos.sql',
    productionTables: ['BrokerBankingInfos'],
    stagingTables: ['stg_broker_banking_infos'],
  },
];

// =============================================================================
// Types
// =============================================================================
interface TableCount {
  table: string;
  count: number;
}

interface ExportResult {
  name: string;
  status: 'SUCCESS' | 'FAILED' | 'SKIPPED';
  error?: string;
  beforeCounts: TableCount[];
  stagingCounts: TableCount[];
  afterCounts: TableCount[];
}

// =============================================================================
// Utility Functions
// =============================================================================
function log(message: string, indent = 0): void {
  const prefix = '  '.repeat(indent);
  console.log(`${prefix}${message}`);
}

function logSection(title: string): void {
  console.log('');
  console.log('='.repeat(64));
  console.log(title);
  console.log('='.repeat(64));
  console.log('');
}

function formatNumber(num: number): string {
  return num.toLocaleString();
}

async function getTableCount(pool: sql.ConnectionPool, schema: string, table: string): Promise<number> {
  try {
    const result = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM [${schema}].[${table}]
    `);
    return result.recordset[0]?.cnt ?? 0;
  } catch (err: any) {
    // Table might not exist
    return -1;
  }
}

async function getTableCounts(pool: sql.ConnectionPool, schema: string, tables: string[]): Promise<TableCount[]> {
  const counts: TableCount[] = [];
  for (const table of tables) {
    const count = await getTableCount(pool, schema, table);
    counts.push({ table, count });
  }
  return counts;
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  const fullPath = path.join(__dirname, '..', filePath);
  
  if (!fs.existsSync(fullPath)) {
    throw new Error(`SQL file not found: ${fullPath}`);
  }
  
  const sqlContent = fs.readFileSync(fullPath, 'utf-8');
  
  // Split by GO statements
  const batches = sqlContent
    .split(/^\s*GO\s*$/gim)
    .filter(batch => batch.trim().length > 0);
  
  for (let i = 0; i < batches.length; i++) {
    try {
      await pool.request().batch(batches[i]);
    } catch (err: any) {
      throw new Error(`Error in batch ${i + 1}: ${err.message}`);
    }
  }
}

// =============================================================================
// Main Export Logic
// =============================================================================
async function captureAllProductionCounts(pool: sql.ConnectionPool): Promise<Map<string, number>> {
  const counts = new Map<string, number>();
  
  // Get all unique production tables
  const allTables = new Set<string>();
  for (const config of exportConfigs) {
    for (const table of config.productionTables) {
      allTables.add(table);
    }
  }
  
  log('Capturing pre-export production counts...');
  for (const table of allTables) {
    const count = await getTableCount(pool, 'dbo', table);
    counts.set(table, count);
    if (count >= 0) {
      log(`${table}: ${formatNumber(count)}`, 1);
    } else {
      log(`${table}: (table not found)`, 1);
    }
  }
  
  return counts;
}

async function captureAllStagingCounts(pool: sql.ConnectionPool): Promise<Map<string, number>> {
  const counts = new Map<string, number>();
  
  // Get all unique staging tables
  const allTables = new Set<string>();
  for (const config of exportConfigs) {
    for (const table of config.stagingTables) {
      allTables.add(table);
    }
  }
  
  log('Capturing staging counts...');
  for (const table of allTables) {
    const count = await getTableCount(pool, 'etl', table);
    counts.set(table, count);
    if (count >= 0) {
      log(`${table}: ${formatNumber(count)}`, 1);
    } else {
      log(`${table}: (table not found)`, 1);
    }
  }
  
  return counts;
}

async function runExportWithVerification(
  pool: sql.ConnectionPool,
  config: ExportConfig,
  index: number,
  total: number,
  dryRun: boolean
): Promise<ExportResult> {
  log(`[${index + 1}/${total}] Exporting ${config.name}...`);
  
  // Capture before counts
  const beforeCounts = await getTableCounts(pool, 'dbo', config.productionTables);
  const stagingCounts = await getTableCounts(pool, 'etl', config.stagingTables);
  
  // Show staging counts
  for (const sc of stagingCounts) {
    log(`Staging (${sc.table}): ${sc.count >= 0 ? formatNumber(sc.count) : 'N/A'}`, 1);
  }
  
  // Show before counts
  for (const bc of beforeCounts) {
    log(`Production BEFORE (${bc.table}): ${bc.count >= 0 ? formatNumber(bc.count) : 'N/A'}`, 1);
  }
  
  if (dryRun) {
    log(`Status: SKIPPED (dry-run)`, 1);
    return {
      name: config.name,
      status: 'SKIPPED',
      beforeCounts,
      stagingCounts,
      afterCounts: beforeCounts, // No change in dry-run
    };
  }
  
  // Run export
  try {
    await executeSqlFile(pool, config.scriptPath);
    
    // Capture after counts
    const afterCounts = await getTableCounts(pool, 'dbo', config.productionTables);
    
    // Show after counts and deltas
    for (let i = 0; i < afterCounts.length; i++) {
      const before = beforeCounts[i]?.count ?? 0;
      const after = afterCounts[i]?.count ?? 0;
      const delta = after - before;
      const deltaStr = delta >= 0 ? `+${formatNumber(delta)}` : formatNumber(delta);
      log(`Production AFTER (${afterCounts[i].table}): ${formatNumber(after)} (${deltaStr})`, 1);
    }
    
    log(`Status: SUCCESS`, 1);
    console.log('');
    
    return {
      name: config.name,
      status: 'SUCCESS',
      beforeCounts,
      stagingCounts,
      afterCounts,
    };
  } catch (err: any) {
    log(`Status: FAILED - ${err.message}`, 1);
    console.log('');
    
    return {
      name: config.name,
      status: 'FAILED',
      error: err.message,
      beforeCounts,
      stagingCounts,
      afterCounts: beforeCounts, // Use before counts on failure
    };
  }
}

function printFinalSummary(results: ExportResult[]): void {
  logSection('FINAL SUMMARY');
  
  // Build summary table data
  const rows: { entity: string; table: string; before: string; staging: string; after: string; delta: string; status: string }[] = [];
  
  for (const result of results) {
    for (let i = 0; i < result.beforeCounts.length; i++) {
      const before = result.beforeCounts[i]?.count ?? 0;
      const staging = result.stagingCounts[i]?.count ?? -1;
      const after = result.afterCounts[i]?.count ?? 0;
      const delta = after - before;
      
      rows.push({
        entity: i === 0 ? result.name : '',
        table: result.beforeCounts[i]?.table ?? '',
        before: before >= 0 ? formatNumber(before) : 'N/A',
        staging: staging >= 0 ? formatNumber(staging) : 'N/A',
        after: after >= 0 ? formatNumber(after) : 'N/A',
        delta: delta >= 0 ? `+${formatNumber(delta)}` : formatNumber(delta),
        status: i === 0 ? result.status : '',
      });
    }
  }
  
  // Print table header
  const colWidths = {
    entity: 25,
    table: 30,
    before: 12,
    staging: 12,
    after: 12,
    delta: 12,
    status: 10,
  };
  
  const header = [
    'Entity'.padEnd(colWidths.entity),
    'Table'.padEnd(colWidths.table),
    'Before'.padStart(colWidths.before),
    'Staging'.padStart(colWidths.staging),
    'After'.padStart(colWidths.after),
    'Delta'.padStart(colWidths.delta),
    'Status'.padEnd(colWidths.status),
  ].join(' | ');
  
  const separator = '-'.repeat(header.length);
  
  console.log(header);
  console.log(separator);
  
  // Print rows
  for (const row of rows) {
    const line = [
      row.entity.padEnd(colWidths.entity),
      row.table.padEnd(colWidths.table),
      row.before.padStart(colWidths.before),
      row.staging.padStart(colWidths.staging),
      row.after.padStart(colWidths.after),
      row.delta.padStart(colWidths.delta),
      row.status.padEnd(colWidths.status),
    ].join(' | ');
    console.log(line);
  }
  
  console.log(separator);
  
  // Summary stats
  const successful = results.filter(r => r.status === 'SUCCESS').length;
  const failed = results.filter(r => r.status === 'FAILED').length;
  const skipped = results.filter(r => r.status === 'SKIPPED').length;
  
  console.log('');
  console.log(`Total exports: ${results.length}`);
  console.log(`  Successful: ${successful}`);
  console.log(`  Failed: ${failed}`);
  console.log(`  Skipped: ${skipped}`);
  
  // List failed exports
  if (failed > 0) {
    console.log('');
    console.log('Failed exports:');
    for (const result of results.filter(r => r.status === 'FAILED')) {
      console.log(`  - ${result.name}: ${result.error}`);
    }
  }
}

// =============================================================================
// Main Entry Point
// =============================================================================
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const tableFilter = args.includes('--table') ? args[args.indexOf('--table') + 1] : null;
  
  logSection('ETL EXPORT WITH VERIFICATION');
  
  log(`Server: ${config.server}`);
  log(`Database: ${config.database}`);
  if (dryRun) {
    log('Mode: DRY-RUN (no actual exports)');
  }
  if (tableFilter) {
    log(`Filter: ${tableFilter}`);
  }
  
  let pool: sql.ConnectionPool | null = null;
  
  try {
    // Connect
    log('');
    log('Connecting to SQL Server...');
    pool = await sql.connect(config);
    log('Connected successfully');
    
    // Filter configs if needed
    let configsToRun = exportConfigs;
    if (tableFilter) {
      configsToRun = exportConfigs.filter(c => 
        c.name.toLowerCase().includes(tableFilter.toLowerCase()) ||
        c.productionTables.some(t => t.toLowerCase().includes(tableFilter.toLowerCase()))
      );
      if (configsToRun.length === 0) {
        log(`No exports match filter: ${tableFilter}`);
        return;
      }
    }
    
    // Capture all counts upfront
    logSection('PRE-EXPORT COUNTS');
    await captureAllProductionCounts(pool);
    console.log('');
    await captureAllStagingCounts(pool);
    
    // Run exports
    logSection('EXPORT PHASE');
    
    const results: ExportResult[] = [];
    
    for (let i = 0; i < configsToRun.length; i++) {
      const result = await runExportWithVerification(
        pool,
        configsToRun[i],
        i,
        configsToRun.length,
        dryRun
      );
      results.push(result);
    }
    
    // Print summary
    printFinalSummary(results);
    
    // Exit code based on results
    const failed = results.filter(r => r.status === 'FAILED').length;
    if (failed > 0) {
      process.exit(1);
    }
    
  } catch (err: any) {
    console.error(`Fatal error: ${err.message}`);
    process.exit(1);
  } finally {
    if (pool) {
      await pool.close();
      log('');
      log('Connection closed');
    }
  }
}

main();
