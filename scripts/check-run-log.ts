import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));
  
  try {
    const result = await pool.request().query(`
      SELECT TOP 10 StepNumber, ScriptName, Status, RecordsProcessed, ErrorMessage
      FROM [poc_etl].[etl_step_state]
      WHERE RunId = (SELECT TOP 1 RunId FROM [poc_etl].[etl_run_state] ORDER BY StartTime DESC)
      ORDER BY StepNumber
    `);
    
    console.log('\nFirst 10 Steps Executed:\n');
    result.recordset.forEach(r => {
      const status = r.Status === 'completed' ? '✅' : '❌';
      console.log(`  ${status} Step ${r.StepNumber}: ${r.ScriptName} - ${r.Status}`);
      if (r.ErrorMessage) console.log(`     Error: ${r.ErrorMessage}`);
    });
    console.log('');
    
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
