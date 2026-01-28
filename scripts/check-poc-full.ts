import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  POC Schema Data Summary');
    console.log('════════════════════════════════════════════════\n');
    
    // Check poc_raw_data
    const pocRaw = await pool.request().query(`
      SELECT TOP 5 t.name, 
             (SELECT SUM(p.rows) FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_raw_data' AND t.name LIKE 'raw_%'
      ORDER BY RecordCount DESC
    `);
    
    console.log('[poc_raw_data] - Top 5 tables:');
    pocRaw.recordset.forEach(t => {
      console.log(`   ${t.name}: ${t.RecordCount.toLocaleString()} records`);
    });
    
    // Check poc_etl input tables
    const pocInput = await pool.request().query(`
      SELECT t.name, 
             (SELECT SUM(p.rows) FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl' AND (t.name LIKE 'input_%' OR t.name LIKE 'prep_%')
      ORDER BY t.name
    `);
    
    console.log('\n[poc_etl] - Input/Prep tables:');
    pocInput.recordset.forEach(t => {
      const icon = t.RecordCount > 0 ? '✅' : '⚠️ ';
      console.log(`   ${icon} ${t.name}: ${t.RecordCount.toLocaleString()}`);
    });
    
    // Check poc_etl staging tables
    const pocStaging = await pool.request().query(`
      SELECT TOP 10 t.name, 
             (SELECT SUM(p.rows) FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl' AND t.name LIKE 'stg_%'
      ORDER BY RecordCount DESC
    `);
    
    console.log('\n[poc_etl] - Top 10 staging tables:');
    pocStaging.recordset.forEach(t => {
      const icon = t.RecordCount > 0 ? '✅' : '⚠️ ';
      console.log(`   ${icon} ${t.name}: ${t.RecordCount.toLocaleString()}`);
    });
    
    console.log('\n════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
