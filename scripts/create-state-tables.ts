import * as sql from 'mssql';
import * as fs from 'fs';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🔧 Creating state management tables in [etl]...\n');
    
    const stateSQL = fs.readFileSync('sql/00a-state-management-tables.sql', 'utf-8');
    const batches = stateSQL.split(/^\s*GO\s*$/gm).map(b => b.trim()).filter(b => b.length > 0);
    
    console.log(`   Split into ${batches.length} batches\n`);
    
    for (let i = 0; i < batches.length; i++) {
      try {
        await pool.request().query(batches[i]);
        console.log(`   ✅ Batch ${i+1}/${batches.length} executed`);
      } catch (e: any) {
        console.log(`   ❌ Batch ${i+1}/${batches.length} failed: ${e.message}`);
        if (i < 5) {
          console.log(`      Preview: ${batches[i].substring(0, 100)}...`);
        }
      }
    }
    
    console.log('\n✅ State management tables created successfully\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
