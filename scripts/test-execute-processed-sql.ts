import * as sql from 'mssql';
import * as fs from 'fs';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🧪 Testing execution of processed SQL...\n');
    
    // Read the processed SQL
    const processedSQL = fs.readFileSync('/tmp/processed-staging-tables.sql', 'utf-8');
    
    // Count schema references
    const pocEtlRefs = (processedSQL.match(/\[poc_etl\]\./g) || []).length;
    const etlRefs = (processedSQL.match(/\[etl\]\./g) || []).length;
    
    console.log(`Schema references in SQL:`);
    console.log(`  [poc_etl].: ${pocEtlRefs}`);
    console.log(`  [etl].:     ${etlRefs}`);
    console.log('');
    
    // Check before state
    const beforeTables = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl'
    `);
    console.log(`Before: [poc_etl] has ${beforeTables.recordset[0].cnt} tables\n`);
    
    // Execute just the first CREATE TABLE statement
    console.log('Executing first CREATE TABLE statement...');
    const firstCreate = processedSQL.match(/CREATE TABLE \[poc_etl\]\.\[stg_brokers\][\s\S]*?;/)?.[0];
    
    if (firstCreate) {
      console.log('Statement found, executing...\n');
      try {
        await pool.request().query(firstCreate);
        console.log('✅ Executed successfully\n');
      } catch (e: any) {
        console.log(`❌ Execution failed: ${e.message}\n`);
      }
    }
    
    // Check after state
    const afterTables = await pool.request().query(`
      SELECT COUNT(*) as cnt FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl'
    `);
    console.log(`After: [poc_etl] has ${afterTables.recordset[0].cnt} tables\n`);
    
    // Check if stg_brokers exists
    try {
      const check = await pool.request().query(`
        SELECT COUNT(*) as cnt FROM [poc_etl].[stg_brokers]
      `);
      console.log(`✅ [poc_etl].[stg_brokers] exists!\n`);
    } catch (e: any) {
      console.log(`❌ [poc_etl].[stg_brokers] does not exist: ${e.message}\n`);
    }
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
