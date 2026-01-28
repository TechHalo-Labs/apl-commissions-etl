import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n════════════════════════════════════════════════');
    console.log('  Source Data Availability');
    console.log('════════════════════════════════════════════════\n');
    
    const sourceSchema = config.database.schemas.source; // 'new_data'
    const transitionSchema = config.database.schemas.transition; // 'raw_data'
    
    console.log(`Checking [${sourceSchema}] schema...`);
    
    const newDataTables = await pool.request().query(`
      SELECT t.name, 
             (SELECT SUM(p.rows) 
              FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = '${sourceSchema}'
      ORDER BY t.name
    `);
    
    if (newDataTables.recordset.length === 0) {
      console.log(`  ❌ No tables found in [${sourceSchema}]`);
    } else {
      console.log(`  ✅ ${newDataTables.recordset.length} tables found:\n`);
      newDataTables.recordset.forEach(t => {
        console.log(`     ${t.name}: ${t.RecordCount.toLocaleString()} records`);
      });
    }
    
    console.log(`\nChecking [${transitionSchema}] schema...`);
    
    const rawDataTables = await pool.request().query(`
      SELECT t.name, 
             (SELECT SUM(p.rows) 
              FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = '${transitionSchema}'
      ORDER BY t.name
    `);
    
    if (rawDataTables.recordset.length === 0) {
      console.log(`  ❌ No tables found in [${transitionSchema}]`);
    } else {
      console.log(`  ✅ ${rawDataTables.recordset.length} tables found:\n`);
      rawDataTables.recordset.forEach(t => {
        console.log(`     ${t.name}: ${t.RecordCount.toLocaleString()} records`);
      });
    }
    
    console.log('\n════════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
