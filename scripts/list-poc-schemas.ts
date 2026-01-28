import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT name FROM sys.schemas WHERE name LIKE 'poc%' ORDER BY name
    `);
    
    console.log('\nPOC Schemas:');
    result.recordset.forEach(s => console.log('  ✅ ' + s.name));
    
    if (result.recordset.length === 0) {
      console.log('  ❌ No POC schemas found!');
    }
    
    console.log('');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
