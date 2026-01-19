/**
 * Run ONLY the transform step - does NOT recreate schema (preserves raw data)
 * 
 * Usage:
 *   npx tsx scripts/run-transforms-only.ts              # Run all transforms
 *   npx tsx scripts/run-transforms-only.ts --group G23326  # Filter to single group
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

const config: sql.config = {
  server: process.env.SQLSERVER_HOST || 'halo-sql.database.windows.net',
  database: process.env.SQLSERVER_DATABASE || 'halo-sqldb',
  user: process.env.SQLSERVER_USER || '***REMOVED***',
  password: process.env.SQLSERVER_PASSWORD || '***REMOVED***',
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  requestTimeout: 300000, // 5 minutes
  connectionTimeout: 30000,
};

const SQL_DIR = path.join(__dirname, '../sql');

// Transform SQL files in order
const transformFiles = [
  'transforms/00-references.sql',
  'transforms/01-brokers.sql',
  'transforms/02-groups.sql',
  'transforms/03-products.sql',
  'transforms/04-schedules.sql',
  'transforms/06-proposals.sql',
  'transforms/07-hierarchies.sql',
  'transforms/08-hierarchy-splits.sql',
  'transforms/09-policies.sql',
  'transforms/10-premium-transactions.sql',
  'transforms/11-fees.sql',
];

function log(message: string, level: 'info' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const prefix = { info: '  ', error: '❌', success: '✅' }[level];
  console.log(`[${timestamp}] ${prefix} ${message}`);
}

function parseArgs(): { groupFilter: string | null } {
  const args = process.argv.slice(2);
  let groupFilter: string | null = null;
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--group' && args[i + 1]) {
      groupFilter = args[i + 1];
      // Ensure G prefix
      if (!groupFilter.startsWith('G')) {
        groupFilter = 'G' + groupFilter;
      }
      i++;
    }
  }
  
  return { groupFilter };
}

async function filterInputTables(pool: sql.ConnectionPool, groupFilter: string): Promise<void> {
  log(`Filtering input tables to group: ${groupFilter}`);
  
  // Get the group number without the G prefix for raw data matching
  const groupNumber = groupFilter.replace(/^G/, '');
  
  // Filter input_certificate_info to only the specified group
  // CRITICAL: Must handle NULL GroupIds - they don't match <> comparison
  const certResult = await pool.request().query(`
    DELETE FROM [etl].[input_certificate_info]
    WHERE LTRIM(RTRIM(GroupId)) <> '${groupNumber}'
       OR GroupId IS NULL 
       OR LTRIM(RTRIM(GroupId)) = '';
    SELECT @@ROWCOUNT AS deleted;
  `);
  log(`input_certificate_info: Removed ${certResult.recordset[0].deleted} rows (keeping group ${groupNumber})`);
  
  // Filter input_commission_details to certificates from the specified group
  const commResult = await pool.request().query(`
    DELETE cd FROM [etl].[input_commission_details] cd
    WHERE NOT EXISTS (
      SELECT 1 FROM [etl].[input_certificate_info] ci
      WHERE ci.CertificateId = cd.CertificateId
    );
    SELECT @@ROWCOUNT AS deleted;
  `);
  log(`input_commission_details: Removed ${commResult.recordset[0].deleted} rows (keeping matching certificates)`);
  
  // Verify counts after filtering
  const counts = await pool.request().query(`
    SELECT 
      (SELECT COUNT(*) FROM [etl].[input_certificate_info]) AS cert_count,
      (SELECT COUNT(*) FROM [etl].[input_commission_details]) AS comm_count
  `);
  log(`Filtered counts - Certificates: ${counts.recordset[0].cert_count}, Commissions: ${counts.recordset[0].comm_count}`, 'success');
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  const fullPath = path.join(SQL_DIR, filePath);
  const sqlContent = fs.readFileSync(fullPath, 'utf-8');
  
  // Split by GO statements
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

async function main() {
  const { groupFilter } = parseArgs();
  
  log('');
  log('============================================================');
  log('SQL Server ETL - TRANSFORMS ONLY (Preserving Raw Data)');
  if (groupFilter) {
    log(`*** FILTERED MODE: Group ${groupFilter} only ***`);
  }
  log('============================================================');
  log('');
  
  const pool = await sql.connect(config);
  log('Connected to SQL Server', 'success');
  
  try {
    // First, recreate ONLY staging tables (not raw tables)
    log('');
    log('Creating staging tables...');
    await executeSqlFile(pool, '03-staging-tables.sql');
    
    // If group filter is specified, filter the input tables
    if (groupFilter) {
      log('');
      log('============================================================');
      log('Applying Group Filter');
      log('============================================================');
      await filterInputTables(pool, groupFilter);
    }
    
    // Run transforms
    log('');
    log('============================================================');
    log('Running Transforms');
    log('============================================================');
    
    for (const transformFile of transformFiles) {
      await executeSqlFile(pool, transformFile);
    }
    
    // Summary
    log('');
    log('============================================================');
    log('Transform Complete - Staging Table Counts');
    log('============================================================');
    
    const tables = [
      'stg_brokers', 'stg_groups', 'stg_products', 'stg_plans',
      'stg_schedules', 'stg_schedule_versions', 'stg_schedule_rates',
      'stg_proposals', 'stg_proposal_products',
      'stg_hierarchies', 'stg_hierarchy_versions', 'stg_hierarchy_participants',
      'stg_premium_split_versions', 'stg_premium_split_participants',
      'stg_policies', 'stg_premium_transactions', 'stg_fees'
    ];
    
    for (const table of tables) {
      try {
        const result = await pool.request().query(`SELECT COUNT(*) as cnt FROM [etl].[${table}]`);
        const count = result.recordset[0].cnt;
        const status = count > 0 ? '✅' : '⚠️';
        log(`${status} ${table}: ${count.toLocaleString()} rows`);
      } catch {
        log(`❌ ${table}: table not found`);
      }
    }
    
    // If group filter, show filtered data summary
    if (groupFilter) {
      log('');
      log('============================================================');
      log(`Filtered Data Summary for ${groupFilter}`);
      log('============================================================');
      
      const summary = await pool.request().query(`
        SELECT 
          (SELECT COUNT(*) FROM [etl].[stg_proposals] WHERE GroupId = '${groupFilter}') AS proposals,
          (SELECT COUNT(*) FROM [etl].[stg_proposal_products] WHERE ProposalId LIKE '%${groupFilter}%') AS proposal_products,
          (SELECT COUNT(*) FROM [etl].[stg_hierarchies] WHERE GroupId = '${groupFilter}') AS hierarchies,
          (SELECT COUNT(*) FROM [etl].[stg_policies] WHERE GroupId = '${groupFilter}') AS policies,
          (SELECT COUNT(*) FROM [etl].[stg_premium_split_versions] WHERE GroupId = '${groupFilter.replace('G', '')}') AS split_versions
      `);
      const s = summary.recordset[0];
      log(`Proposals: ${s.proposals}`);
      log(`Proposal Products: ${s.proposal_products}`);
      log(`Hierarchies: ${s.hierarchies}`);
      log(`Policies: ${s.policies}`);
      log(`Split Versions: ${s.split_versions}`);
    }
    
  } finally {
    await pool.close();
    log('');
    log('Connection closed');
  }
}

main().catch(err => {
  console.error('Transform failed:', err);
  process.exit(1);
});
