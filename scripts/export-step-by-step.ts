/**
 * Export Step-by-Step: Execute exports one at a time with progress updates
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';

function parseConnectionString(connStr: string) {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  const encrypt = parts['encrypt'];
  const trustCert = parts['trustservercertificate'];
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: encrypt === undefined ? true : encrypt.toLowerCase() === 'true',
      trustServerCertificate: trustCert === undefined ? true : trustCert.toLowerCase() === 'true',
      enableArithAbort: true,
    },
    pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 600000,
  };
}

async function getRowCount(pool: sql.ConnectionPool, schema: string, table: string): Promise<number> {
  try {
    const result = await pool.request().query(`SELECT COUNT(*) AS cnt FROM [${schema}].[${table}]`);
    return result.recordset[0]?.cnt || 0;
  } catch (e: any) {
    return 0;
  }
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string, etlSchema: string, productionSchema: string): Promise<{ success: boolean; rowsAffected: number; error?: string }> {
  const fileName = path.basename(filePath);
  
  if (!fs.existsSync(filePath)) {
    return { success: false, rowsAffected: 0, error: `File not found: ${filePath}` };
  }
  
  let sqlContent = fs.readFileSync(filePath, 'utf8');
  
  // Replace SQLCMD variables
  sqlContent = sqlContent.replace(/\$\(PRODUCTION_SCHEMA\)/g, productionSchema);
  sqlContent = sqlContent.replace(/\$\(ETL_SCHEMA\)/g, etlSchema);
  
  const batches = sqlContent.split(/^\s*GO\s*$/im).filter(b => b.trim().length > 0);
  
  let totalRowsAffected = 0;
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (batch.length === 0) continue;
    
    try {
      const result = await pool.request().query(batch);
      if (result.rowsAffected && result.rowsAffected.length > 0) {
        totalRowsAffected += result.rowsAffected.reduce((a, b) => a + b, 0);
      }
    } catch (err: any) {
      return { success: false, rowsAffected: totalRowsAffected, error: `Error in batch ${i + 1}: ${err.message}` };
    }
  }
  
  return { success: true, rowsAffected: totalRowsAffected };
}

async function main() {
  console.log('============================================================');
  console.log('STEP-BY-STEP EXPORT: Staging → Production');
  console.log('============================================================\n');

  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  const etlSchema = config.database.schemas.processing || 'etl';
  const productionSchema = config.database.schemas.production || 'dbo';

  // Export scripts in FK order (parents first)
  const exportScripts = [
    { name: 'Schedules', file: 'sql/export/01-export-schedules.sql', tables: ['Schedules'] },
    { name: 'Brokers', file: 'sql/export/02-export-brokers.sql', tables: ['Brokers'] },
    { name: 'Broker Licenses', file: 'sql/export/13-export-licenses.sql', tables: ['BrokerLicenses'] },
    { name: 'Broker Banking Info', file: 'sql/export/16-export-broker-banking-infos.sql', tables: ['BrokerBankingInfos'] },
    { name: 'Groups', file: 'sql/export/05-export-groups-simple.sql', tables: ['EmployerGroups'] },
    { name: 'Products', file: 'sql/export/06-export-products.sql', tables: ['Products'] },
    { name: 'Plans', file: 'sql/export/06a-export-plans.sql', tables: ['Plans'] },
    { name: 'Proposals', file: 'sql/export/07-export-proposals.sql', tables: ['Proposals'] },
    { name: 'Hierarchies', file: 'sql/export/08-export-hierarchies.sql', tables: ['Hierarchies'] },
    { name: 'Policies', file: 'sql/export/09-export-policies.sql', tables: ['Policies'] },
    { name: 'Premium Splits', file: 'sql/export/11-export-splits.sql', tables: ['PremiumSplitVersions', 'PremiumSplitParticipants'] },
    { name: 'Commission Assignments', file: 'sql/export/13-export-commission-assignments.sql', tables: ['CommissionAssignmentVersions', 'CommissionAssignmentRecipients'] },
    { name: 'Policy Hierarchy Assignments', file: 'sql/export/14-export-policy-hierarchy-assignments.sql', tables: ['PolicyHierarchyAssignments'] },
  ];

  try {
    for (let i = 0; i < exportScripts.length; i++) {
      const script = exportScripts[i];
      console.log(`\n[${i + 1}/${exportScripts.length}] ${script.name}`);
      console.log('─'.repeat(60));
      console.log(`Executing: ${path.basename(script.file)}`);

      // Get before counts
      const beforeCounts: Record<string, number> = {};
      for (const table of script.tables) {
        beforeCounts[table] = await getRowCount(pool, productionSchema, table);
        console.log(`   Before: ${table}=${beforeCounts[table].toLocaleString()}`);
      }

      // Execute script
      const result = await executeSqlFile(pool, script.file, etlSchema, productionSchema);

      if (!result.success) {
        console.log(`   ❌ FAILED: ${result.error}`);
        console.log(`   Rows affected before error: ${result.rowsAffected}`);
        console.log(`\n⚠️  Export failed. Continue with next step? (y/n)`);
        // For now, continue automatically
        continue;
      }

      // Get after counts
      const afterCounts: Record<string, number> = {};
      for (const table of script.tables) {
        afterCounts[table] = await getRowCount(pool, productionSchema, table);
        const diff = afterCounts[table] - beforeCounts[table];
        console.log(`   After:  ${table}=${afterCounts[table].toLocaleString()} (+${diff.toLocaleString()})`);
      }

      console.log(`   ✅ SUCCESS: ${result.rowsAffected} rows affected`);
    }

    console.log('\n============================================================');
    console.log('✅ EXPORT COMPLETE');
    console.log('============================================================\n');

    // Final summary
    console.log('📊 FINAL PRODUCTION STATE:\n');
    const summaryTables = ['Brokers', 'BrokerLicenses', 'EmployerGroups', 'Products', 'Plans', 
                          'Schedules', 'Proposals', 'Policies', 'Hierarchies', 'PremiumTransactions'];
    for (const table of summaryTables) {
      const count = await getRowCount(pool, productionSchema, table);
      console.log(`  ${table.padEnd(25)}: ${count.toLocaleString().padStart(10)} rows`);
    }

  } catch (error: any) {
    console.error('\n❌ Fatal error:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
