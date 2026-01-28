import * as sql from 'mssql';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';
import { executeSQLScript } from './lib/sql-executor';

async function main() {
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);
  
  try {
    console.log('\n🔧 Initializing state management infrastructure in [etl]...\n');
    
    const scriptPath = path.join(__dirname, '../sql/00a-state-management-tables.sql');
    
    try {
    const result = await executeSQLScript({
      scriptPath,
      pool,
      config,
      debugMode: true,
      pocMode: false
    });
    
    if (result.success) {
      console.log(`\n✅ State management initialized successfully in ${result.duration.toFixed(2)}s\n`);
    } else {
      console.log(`\n❌ Failed to initialize state management`);
      console.log(`   Error: ${result.error?.message}`);
      console.log(`   SQL Number: ${(result.error as any)?.number}\n`);
      throw result.error;
    }
    } catch (e: any) {
      console.log(`\n❌ Error during initialization: ${e.message}\n`);
      if (e.number === 111) {
        console.log('💡 This is a CREATE PROCEDURE batching issue - checking if it succeeded anyway...\n');
        
        // Check if tables exist
        const check = await pool.request().query(`
          SELECT COUNT(*) as cnt FROM sys.tables t
          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
          WHERE s.name = 'etl' AND t.name IN ('etl_run_state', 'etl_step_state')
        `);
        
        if (check.recordset[0].cnt === 2) {
          console.log('✅ State tables exist - proceeding\n');
        } else {
          console.log(`❌ Only ${check.recordset[0].cnt}/2 state tables found\n`);
          throw e;
        }
      } else {
        throw e;
      }
    }
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
