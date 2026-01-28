import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🧪 Testing direct table creation in [poc_etl]...\n');
    
    // Drop test table if exists
    await pool.request().query(`
      DROP TABLE IF EXISTS [poc_etl].[test_table_poc];
      DROP TABLE IF EXISTS [etl].[test_table_poc];
    `);
    
    // Create test table explicitly in poc_etl
    console.log('Creating test table in [poc_etl]...');
    await pool.request().query(`
      CREATE TABLE [poc_etl].[test_table_poc] (
        Id INT PRIMARY KEY,
        Name NVARCHAR(100)
      );
    `);
    console.log('✅ Table created\n');
    
    // Insert test data
    await pool.request().query(`
      INSERT INTO [poc_etl].[test_table_poc] (Id, Name) VALUES (1, 'Test');
    `);
    console.log('✅ Data inserted\n');
    
    // Check where it ended up
    console.log('Checking table location...\n');
    
    const pocResult = await pool.request().query(`
      SELECT * FROM [poc_etl].[test_table_poc]
    `);
    console.log(`[poc_etl].[test_table_poc]: ${pocResult.recordset.length} rows`);
    
    try {
      const etlResult = await pool.request().query(`
        SELECT * FROM [etl].[test_table_poc]
      `);
      console.log(`[etl].[test_table_poc]: ${etlResult.recordset.length} rows`);
    } catch (e: any) {
      if (e.message.includes('Invalid object name')) {
        console.log(`[etl].[test_table_poc]: Table not found (GOOD!)`);
      }
    }
    
    // Cleanup
    await pool.request().query(`DROP TABLE [poc_etl].[test_table_poc]`);
    console.log('\n✅ Test complete - table created in correct schema!\n');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
