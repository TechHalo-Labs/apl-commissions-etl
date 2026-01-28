import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT s.name as SchemaName, t.name as TableName,
             (SELECT COUNT(*) 
              FROM sys.columns c 
              WHERE c.object_id = t.object_id) as ColumnCount,
             (SELECT SUM(p.rows) 
              FROM sys.partitions p 
              WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as RecordCount
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name IN ('etl', 'poc_etl')
        AND t.name LIKE 'stg_%'
      ORDER BY s.name, t.name
    `);
    
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Staging Tables (stg_*) Location');
    console.log('═══════════════════════════════════════════════\n');
    
    const etlTables = result.recordset.filter(r => r.SchemaName === 'etl');
    const pocEtlTables = result.recordset.filter(r => r.SchemaName === 'poc_etl');
    
    console.log(`[etl] Schema: ${etlTables.length} staging tables`);
    if (etlTables.length > 0) {
      etlTables.slice(0, 5).forEach(r => {
        console.log(`  ${r.TableName}: ${r.RecordCount} rows, ${r.ColumnCount} cols`);
      });
      if (etlTables.length > 5) {
        console.log(`  ... and ${etlTables.length - 5} more tables`);
      }
    }
    
    console.log(`\n[poc_etl] Schema: ${pocEtlTables.length} staging tables`);
    if (pocEtlTables.length > 0) {
      pocEtlTables.forEach(r => {
        console.log(`  ${r.TableName}: ${r.RecordCount} rows, ${r.ColumnCount} cols`);
      });
    } else {
      console.log('  (none - tables may have been created in [etl] instead)');
    }
    
    console.log('\n═══════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
