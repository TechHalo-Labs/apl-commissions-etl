import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

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
    requestTimeout: 600000, // 10 minutes for large deletes
  };
}

async function executeSqlFile(pool: sql.ConnectionPool, filePath: string): Promise<void> {
  const fileName = path.basename(filePath);
  console.log(`\n📋 Executing ${fileName}...`);
  
  let sqlContent = fs.readFileSync(filePath, 'utf8');
  
  // Replace SQLCMD variables
  sqlContent = sqlContent.replace(/\$\(PRODUCTION_SCHEMA\)/g, 'dbo');
  sqlContent = sqlContent.replace(/\$\(ETL_SCHEMA\)/g, 'etl');
  
  const batches = sqlContent.split(/^\s*GO\s*$/im).filter(b => b.trim().length > 0);
  
  console.log(`   ${batches.length} batch(es) found`);
  
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i].trim();
    if (batch.length === 0) continue;
    
    try {
      const result = await pool.request().query(batch);
      // Show affected rows if it's a DELETE or INSERT
      if (result.rowsAffected && result.rowsAffected.length > 0) {
        const total = result.rowsAffected.reduce((a, b) => a + b, 0);
        if (total > 0) {
          console.log(`   ✅ ${total} rows affected`);
        }
      }
    } catch (err: any) {
      console.error(`   ❌ Error in batch ${i + 1}: ${err.message}`);
      throw err;
    }
  }
  
  console.log(`✅ ${fileName} completed`);
}

async function main() {
  console.log('============================================================');
  console.log('DESTRUCTIVE EXPORT: Delete Production + Insert Staging');
  console.log('============================================================\n');
  
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  const config = parseConnectionString(connStr);
  const pool = await sql.connect(config);
  
  try {
    // Step 1: DELETE production data (in FK order - children first)
    console.log('\n📋 STEP 1: Deleting production data (FK order)');
    console.log('='.repeat(60));
    
    const deletions = [
      // Children first (FK dependencies - most dependent to least dependent)
      { table: 'CommissionAssignmentRecipients', sql: 'DELETE FROM [dbo].[CommissionAssignmentRecipients];' },
      { table: 'CommissionAssignmentVersions', sql: 'DELETE FROM [dbo].[CommissionAssignmentVersions];' },
      { table: 'PolicyHierarchyAssignments', sql: 'DELETE FROM [dbo].[PolicyHierarchyAssignments];' },
      { table: 'PremiumTransactions', sql: 'DELETE FROM [dbo].[PremiumTransactions];' },
      { table: 'Policies', sql: 'DELETE FROM [dbo].[Policies];' },
      { table: 'PremiumSplitParticipants', sql: 'DELETE FROM [dbo].[PremiumSplitParticipants];' },
      { table: 'PremiumSplitVersions', sql: 'DELETE FROM [dbo].[PremiumSplitVersions];' },
      { table: 'HierarchyParticipants', sql: 'DELETE FROM [dbo].[HierarchyParticipants];' },
      { table: 'Hierarchies', sql: 'DELETE FROM [dbo].[Hierarchies];' },
      { table: 'HierarchyTemplates', sql: 'DELETE FROM [dbo].[HierarchyTemplates];' },
      { table: 'ProposalProducts', sql: 'DELETE FROM [dbo].[ProposalProducts];' },
      { table: 'Proposals', sql: 'DELETE FROM [dbo].[Proposals];' },
      { table: 'Products', sql: 'DELETE FROM [dbo].[Products];' },
      { table: 'Plans', sql: 'DELETE FROM [dbo].[Plans];' },
      { table: 'ScheduleRateTiers', sql: 'DELETE FROM [dbo].[ScheduleRateTiers];' },
      { table: 'SpecialScheduleRates', sql: 'DELETE FROM [dbo].[SpecialScheduleRates];' },
      { table: 'HierarchyParticipantProductRates', sql: 'DELETE FROM [dbo].[HierarchyParticipantProductRates];' },
      { table: 'ScheduleRates', sql: 'DELETE FROM [dbo].[ScheduleRates];' },
      { table: 'Schedules', sql: 'DELETE FROM [dbo].[Schedules];' },
      // Group child tables
      { table: 'GroupAddresses', sql: 'DELETE FROM [dbo].[GroupAddresses];' },
      { table: 'GroupContacts', sql: 'DELETE FROM [dbo].[GroupContacts];' },
      { table: 'GroupProduct', sql: 'DELETE FROM [dbo].[GroupProduct];' },
      { table: 'EmployerGroups', sql: 'DELETE FROM [dbo].[EmployerGroups];' },
      // Broker child tables
      { table: 'BrokerBankingInfos', sql: 'DELETE FROM [dbo].[BrokerBankingInfos];' },
      { table: 'BrokerLicenses', sql: 'DELETE FROM [dbo].[BrokerLicenses];' },
      { table: 'BrokerAddresses', sql: 'DELETE FROM [dbo].[BrokerAddresses];' },
      { table: 'BrokerContacts', sql: 'DELETE FROM [dbo].[BrokerContacts];' },
      { table: 'Brokers', sql: 'DELETE FROM [dbo].[Brokers];' },
    ];
    
    for (const deletion of deletions) {
      console.log(`\n🗑️  Deleting from ${deletion.table}...`);
      try {
        // Check if table exists first
        const checkTable = await pool.request().query(`
          SELECT COUNT(*) as cnt
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '${deletion.table}'
        `);
        
        if (checkTable.recordset[0].cnt === 0) {
          console.log(`   ⏭️  Table does not exist, skipping`);
          continue;
        }
        
        const result = await pool.request().query(deletion.sql);
        const count = result.rowsAffected[0] || 0;
        console.log(`   ✅ Deleted ${count} rows`);
      } catch (err: any) {
        console.error(`   ❌ Error: ${err.message}`);
        // Don't throw - continue with other deletions
      }
    }
    
    // Step 2: INSERT staging data (in FK order - parents first)
    console.log('\n\n📋 STEP 2: Exporting staging to production (FK order)');
    console.log('='.repeat(60));
    
    const exportScripts = [
      'sql/export/02-export-brokers.sql',
      'sql/export/13-export-licenses.sql',
      'sql/export/16-export-broker-banking-infos.sql',
      'sql/export/05-export-groups-simple.sql',  // Use simple export (no conformance columns)
      'sql/export/06-export-products.sql',
      'sql/export/06a-export-plans.sql',
      'sql/export/01-export-schedules.sql',
      'sql/export/17-export-special-schedule-rates.sql',
      'sql/export/18-export-schedule-rate-tiers.sql',
      'sql/export/19-export-hierarchy-product-rates.sql',
      'sql/export/07-export-proposals.sql',
      'sql/export/08-export-hierarchies.sql',
      'sql/export/11-export-splits.sql',
      'sql/export/09-export-policies.sql',
      'sql/export/10-export-premium-transactions.sql',
      'sql/export/14-export-policy-hierarchy-assignments.sql',
      'sql/export/13-export-commission-assignments.sql',  // Commission assignments from staging
    ];
    
    for (const scriptPath of exportScripts) {
      const fullPath = path.join(process.cwd(), scriptPath);
      if (fs.existsSync(fullPath)) {
        await executeSqlFile(pool, fullPath);
      } else {
        console.log(`⚠️  Skipping ${scriptPath} (not found)`);
      }
    }
    
    // Step 3: Verify counts
    console.log('\n\n📋 STEP 3: Verifying export counts');
    console.log('='.repeat(60));
    
    const verification = await pool.request().query(`
      SELECT 'Brokers' as Entity, COUNT(*) as [Production Count] FROM [dbo].[Brokers]
      UNION ALL SELECT 'Commission Assignment Versions', COUNT(*) FROM [dbo].[CommissionAssignmentVersions]
      UNION ALL SELECT 'Commission Assignment Recipients', COUNT(*) FROM [dbo].[CommissionAssignmentRecipients]
      UNION ALL SELECT 'Groups', COUNT(*) FROM [dbo].[EmployerGroups]
      UNION ALL SELECT 'Hierarchies', COUNT(*) FROM [dbo].[Hierarchies]
      UNION ALL SELECT 'Policies', COUNT(*) FROM [dbo].[Policies]
      UNION ALL SELECT 'Premium Split Versions', COUNT(*) FROM [dbo].[PremiumSplitVersions]
      UNION ALL SELECT 'Premium Transactions', COUNT(*) FROM [dbo].[PremiumTransactions]
      UNION ALL SELECT 'Products', COUNT(*) FROM [dbo].[Products]
      UNION ALL SELECT 'Proposals', COUNT(*) FROM [dbo].[Proposals]
      UNION ALL SELECT 'Schedules', COUNT(*) FROM [dbo].[Schedules]
      ORDER BY 1
    `);
    
    console.log('\n✅ Production Data Summary:');
    console.table(verification.recordset);
    
    console.log('\n============================================================');
    console.log('✅ DESTRUCTIVE EXPORT COMPLETED SUCCESSFULLY');
    console.log('============================================================\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(err => {
  console.error('\n❌ Export failed:', err);
  process.exit(1);
});
