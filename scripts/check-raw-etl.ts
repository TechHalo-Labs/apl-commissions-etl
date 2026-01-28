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
      WHERE s.name = 'etl' AND t.name LIKE 'raw_%'
      ORDER BY RecordCount DESC
    `);
    
    console.log('\n════════════════════════════════════════════════');
    console.log('  Raw Tables in [etl]');
    console.log('════════════════════════════════════════════════\n');
    
    result.recordset.forEach(t => {
      const icon = t.RecordCount > 0 ? '✅' : '⚠️ ';
      console.log(`${icon} ${t.name}: ${t.RecordCount.toLocaleString()} records`);
    });
    
    console.log('\n════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
