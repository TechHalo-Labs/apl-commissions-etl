import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    console.log('\n🔧 Dropping state management stored procedures...\n');
    
    const procs = [
      'sp_start_run',
      'sp_update_run_progress',
      'sp_complete_run',
      'sp_fail_run',
      'sp_start_step',
      'sp_complete_step',
      'sp_fail_step',
      'sp_update_step_progress',
      'sp_get_last_run',
      'sp_get_incomplete_steps'
    ];
    
    for (const proc of procs) {
      try {
        await pool.request().query(`DROP PROCEDURE IF EXISTS [etl].[${proc}]`);
        console.log(`   ✅ Dropped [etl].[${proc}]`);
      } catch (e: any) {
        console.log(`   ⚠️  [etl].[${proc}]: ${e.message}`);
      }
    }
    
    // Drop state tables too
    await pool.request().query(`DROP TABLE IF EXISTS [etl].[etl_step_state]`);
    await pool.request().query(`DROP TABLE IF EXISTS [etl].[etl_run_state]`);
    
    console.log('\n✅ All state management objects dropped\n');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
