import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT t.name, 
             (SELECT SUM(p.rows) 
              FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'etl' AND t.name LIKE 'stg_%'
      ORDER BY RecordCount DESC
    `);
    
    console.log('\n════════════════════════════════════════════════');
    console.log('  Staging Tables in [etl] - By Record Count');
    console.log('════════════════════════════════════════════════\n');
    
    const totalRecords = result.recordset.reduce((sum, t) => sum + (t.RecordCount || 0), 0);
    
    console.log(`Total: ${result.recordset.length} tables, ${totalRecords.toLocaleString()} records\n`);
    
    console.log('Top 15 tables:\n');
    result.recordset.slice(0, 15).forEach((t, i) => {
      const icon = t.RecordCount > 0 ? '✅' : '⚠️ ';
      console.log(`${i + 1}. ${icon} ${t.name}: ${t.RecordCount.toLocaleString()} records`);
    });
    
    console.log('\n════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
