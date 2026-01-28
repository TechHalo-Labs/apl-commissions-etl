import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const tables = await pool.request().query(`
      SELECT t.name as TableName
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'poc_etl'
      ORDER BY t.name
    `);
    
    console.log('\n═══════════════════════════════════════════════');
    console.log('  Tables in [poc_etl]:');
    console.log('═══════════════════════════════════════════════\n');
    
    for (const row of tables.recordset) {
      try {
        const count = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM [poc_etl].[${row.TableName}]
        `);
        console.log(`  ${row.TableName}: ${count.recordset[0].cnt} records`);
      } catch (e: any) {
        console.log(`  ${row.TableName}: Error - ${e.message}`);
      }
    }
    
    console.log('\n═══════════════════════════════════════════════\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
