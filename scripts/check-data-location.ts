import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT s.name as SchemaName, COUNT(t.name) as TableCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name IN ('etl', 'poc_etl', 'dbo', 'poc_dbo')
      GROUP BY s.name
      ORDER BY s.name
    `);
    
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Schema Table Counts');
    console.log('═══════════════════════════════════════════════\n');
    
    result.recordset.forEach(row => {
      console.log(`  [${row.SchemaName}]: ${row.TableCount} tables`);
    });
    
    // Check key staging tables
    const stagingTables = ['stg_brokers', 'stg_policies', 'stg_groups', 'stg_proposals'];
    
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Staging Data Location');
    console.log('═══════════════════════════════════════════════\n');
    
    for (const table of stagingTables) {
      for (const schema of ['etl', 'poc_etl']) {
        try {
          const count = await pool.request().query(`
            SELECT COUNT(*) as cnt FROM [${schema}].[${table}]
          `);
          if (count.recordset[0].cnt > 0) {
            console.log(`  ✅ [${schema}].[${table}]: ${count.recordset[0].cnt} records`);
          }
        } catch {
          // Table doesn't exist in this schema
        }
      }
    }
    
    // Check key production tables
    const prodTables = ['Brokers', 'Policies', 'Proposals'];
    
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Production Data Location');
    console.log('═══════════════════════════════════════════════\n');
    
    for (const table of prodTables) {
      for (const schema of ['dbo', 'poc_dbo']) {
        try {
          const count = await pool.request().query(`
            SELECT COUNT(*) as cnt FROM [${schema}].[${table}]
          `);
          console.log(`  [${schema}].[${table}]: ${count.recordset[0].cnt} records`);
        } catch {
          console.log(`  [${schema}].[${table}]: Table not found`);
        }
      }
    }
    
    console.log('\n═══════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
