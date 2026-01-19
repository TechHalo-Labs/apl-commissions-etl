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
  console.log('STAGING TABLE VERIFICATION');
  console.log('='.repeat(80));
  
  const tables = [
    { schema: 'etl', name: 'raw_premiums', category: 'RAW' },
    { schema: 'etl', name: 'raw_commissions_detail', category: 'RAW' },
    { schema: 'etl', name: 'raw_certificate_info', category: 'RAW' },
    { schema: 'etl', name: 'raw_individual_brokers', category: 'RAW' },
    { schema: 'etl', name: 'raw_org_brokers', category: 'RAW' },
    { schema: 'etl', name: 'raw_licenses', category: 'RAW' },
    { schema: 'etl', name: 'raw_eo_insurance', category: 'RAW' },
    { schema: 'etl', name: 'raw_schedule_rates', category: 'RAW' },
    { schema: 'etl', name: 'raw_perf_groups', category: 'RAW' },
    { schema: 'etl', name: 'input_certificate_info', category: 'INPUT' },
    { schema: 'etl', name: 'input_commissions_detail', category: 'INPUT' },
    { schema: 'etl', name: 'stg_brokers', category: 'STAGING' },
    { schema: 'etl', name: 'stg_groups', category: 'STAGING' },
    { schema: 'etl', name: 'stg_products', category: 'STAGING' },
    { schema: 'etl', name: 'stg_product_codes', category: 'STAGING' },
    { schema: 'etl', name: 'stg_plans', category: 'STAGING' },
    { schema: 'etl', name: 'stg_schedules', category: 'STAGING' },
    { schema: 'etl', name: 'stg_schedule_versions', category: 'STAGING' },
    { schema: 'etl', name: 'stg_schedule_rates', category: 'STAGING' },
    { schema: 'etl', name: 'stg_proposals', category: 'STAGING' },
    { schema: 'etl', name: 'stg_proposal_products', category: 'STAGING' },
    { schema: 'etl', name: 'stg_premium_split_versions', category: 'STAGING' },
    { schema: 'etl', name: 'stg_premium_split_participants', category: 'STAGING' },
    { schema: 'etl', name: 'stg_hierarchies', category: 'STAGING' },
    { schema: 'etl', name: 'stg_hierarchy_versions', category: 'STAGING' },
    { schema: 'etl', name: 'stg_hierarchy_participants', category: 'STAGING' },
    { schema: 'etl', name: 'stg_hierarchy_splits', category: 'STAGING' },
    { schema: 'etl', name: 'stg_commission_assignment_versions', category: 'STAGING' },
    { schema: 'etl', name: 'stg_commission_assignment_recipients', category: 'STAGING' },
    { schema: 'etl', name: 'stg_policies', category: 'STAGING' },
    { schema: 'etl', name: 'stg_premium_transactions', category: 'STAGING' },
    { schema: 'etl', name: 'stg_broker_licenses', category: 'STAGING' },
    { schema: 'etl', name: 'stg_broker_eo_insurances', category: 'STAGING' },
  ];
  
  let currentCategory = '';
  for (const t of tables) {
    if (t.category !== currentCategory) {
      currentCategory = t.category;
      console.log('\n--- ' + currentCategory + ' TABLES ---');
    }
    try {
      const cnt = await pool.request().query('SELECT COUNT(*) as cnt FROM [' + t.schema + '].[' + t.name + ']');
      console.log(t.name.padEnd(45) + cnt.recordset[0].cnt.toLocaleString().padStart(12));
    } catch (err) {
      console.log(t.name.padEnd(45) + 'N/A'.padStart(12));
    }
  }
  
  console.log('\n' + '='.repeat(80));
  console.log('STAGING VERIFICATION COMPLETE');
  console.log('='.repeat(80));
  
  await pool.close();
}
run().catch(console.error);
