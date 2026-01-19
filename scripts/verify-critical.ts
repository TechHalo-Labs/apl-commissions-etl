import sql from 'mssql';

const config = { 
  server: 'halo-sql.database.windows.net', 
  database: 'halo-sqldb', 
  user: '***REMOVED***', 
  password: '***REMOVED***', 
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 120000
};

async function run() {
  const pool = await sql.connect(config);
  
  console.log('='.repeat(80));
  console.log('CRITICAL TABLE VERIFICATION');
  console.log('='.repeat(80));
  
  const tables = [
    { schema: 'etl', name: 'stg_product_codes', status: 'CRITICAL' },
    { schema: 'etl', name: 'stg_plans', status: 'CRITICAL' },
    { schema: 'etl', name: 'stg_proposal_products', status: 'CRITICAL' },
    { schema: 'etl', name: 'stg_hierarchy_splits', status: 'CRITICAL' },
    { schema: 'etl', name: 'stg_state_rules', status: 'OPTIONAL' },
    { schema: 'etl', name: 'stg_state_rule_states', status: 'OPTIONAL' },
    { schema: 'etl', name: 'stg_split_distributions', status: 'OPTIONAL' },
  ];
  
  for (const t of tables) {
    try {
      const exists = await pool.request().query(
        "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + t.schema + "' AND TABLE_NAME = '" + t.name + "'"
      );
      if (exists.recordset[0].cnt > 0) {
        const cnt = await pool.request().query('SELECT COUNT(*) as cnt FROM [' + t.schema + '].[' + t.name + ']');
        console.log(t.name.padEnd(35) + cnt.recordset[0].cnt.toLocaleString().padStart(12) + '  [' + t.status + ']');
      } else {
        console.log(t.name.padEnd(35) + 'TABLE MISSING'.padStart(12) + '  [' + t.status + ']');
      }
    } catch (err) {
      console.log(t.name.padEnd(35) + 'ERROR'.padStart(12) + '  [' + t.status + ']');
    }
  }
  
  console.log('\n' + '='.repeat(80));
  
  await pool.close();
}
run().catch(console.error);
