import * as sql from 'mssql';
import * as fs from 'fs';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🧪 Testing full staging tables SQL execution...\n');
    
    // Check before
    const before = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl' AND t.name LIKE 'stg_%'
    `);
    console.log(`Before: ${before.recordset[0].cnt} staging tables in [poc_etl]\n`);
    
    // Read and execute the processed SQL
    const sql_content = fs.readFileSync('/tmp/processed-03-staging-tables.sql', 'utf-8');
    console.log(`SQL file size: ${sql_content.length} characters\n`);
    console.log(`Executing SQL...`);
    
    try {
      const result = await pool.request().query(sql_content);
      console.log(`✅ Execution completed`);
      console.log(`   Rows affected: ${result.rowsAffected?.length || 0} operations\n`);
    } catch (e: any) {
      console.log(`❌ Execution failed: ${e.message}\n`);
      return;
    }
    
    // Check after
    const after = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl' AND t.name LIKE 'stg_%'
    `);
    console.log(`After: ${after.recordset[0].cnt} staging tables in [poc_etl]\n`);
    
    if (after.recordset[0].cnt > before.recordset[0].cnt) {
      console.log(`✅ SUCCESS: ${after.recordset[0].cnt - before.recordset[0].cnt} tables created!\n`);
    } else {
      console.log(`❌ PROBLEM: No tables created\n`);
    }
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
