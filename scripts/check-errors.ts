import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT StepNumber, ScriptName, Status, ErrorMessage, RecordsProcessed
      FROM [poc_etl].[etl_step_state]
      WHERE RunId = (SELECT TOP 1 RunId FROM [poc_etl].[etl_run_state] ORDER BY StartTime DESC)
        AND (Status = 'failed' OR ErrorMessage IS NOT NULL)
      ORDER BY StepNumber
    `);
    
    if (result.recordset.length === 0) {
      console.log('\n✅ No errors found in latest run\n');
    } else {
      console.log('\n❌ Errors found:\n');
      result.recordset.forEach(r => {
        console.log(`  Step ${r.StepNumber}: ${r.ScriptName}`);
        console.log(`    Status: ${r.Status}`);
        console.log(`    Error: ${r.ErrorMessage}`);
        console.log('');
      });
    }
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
