import sql from 'mssql';

const config: sql.config = {
  server: 'halo-sql.database.windows.net',
  database: 'halo-sqldb',
  user: '***REMOVED***',
  password: '***REMOVED***',
  options: { encrypt: true, trustServerCertificate: true },
  requestTimeout: 120000,
};

async function run() {
  const pool = await sql.connect(config);

  console.log('=== GROUP NAME SOURCES ANALYSIS ===');

  // Check raw_premiums for ANY group names
  console.log('\n--- raw_premiums group names ---');
  const premCheck = await pool.request().query(`
    SELECT COUNT(DISTINCT LTRIM(RTRIM(GroupNumber))) as groups_with_names
    FROM [etl].[raw_premiums]
    WHERE LTRIM(RTRIM(GroupName)) <> ''
      AND LTRIM(RTRIM(GroupName)) <> 'NULL'
  `);
  console.log('Premium groups with names: ' + premCheck.recordset[0].groups_with_names);

  // Sample premium group names
  const premSample = await pool.request().query(`
    SELECT TOP 5 
      LTRIM(RTRIM(GroupNumber)) as GroupNumber,
      MAX(LTRIM(RTRIM(GroupName))) as GroupName
    FROM [etl].[raw_premiums]
    WHERE LTRIM(RTRIM(GroupName)) <> ''
      AND LTRIM(RTRIM(GroupName)) <> 'NULL'
    GROUP BY LTRIM(RTRIM(GroupNumber))
  `);
  console.log('Sample premium group names:');
  for (const r of premSample.recordset) {
    console.log('  ' + r.GroupNumber + ': ' + r.GroupName);
  }

  // Check for groups that have generic names but COULD get names from premiums
  console.log('\n--- Groups that could get names from premiums ---');
  const canRecover = await pool.request().query(`
    SELECT COUNT(*) as cnt
    FROM [etl].[stg_groups] g
    WHERE g.Name LIKE 'Group %'
      AND EXISTS (
        SELECT 1 FROM [etl].[raw_premiums] p 
        WHERE LTRIM(RTRIM(p.GroupNumber)) = g.Code
          AND LTRIM(RTRIM(p.GroupName)) <> ''
          AND LTRIM(RTRIM(p.GroupName)) <> 'NULL'
      )
  `);
  console.log('Groups that could get names from premiums: ' + canRecover.recordset[0].cnt);

  // Overall summary
  console.log('\n=== SUMMARY ===');
  const summary = await pool.request().query(`
    SELECT 
      COUNT(*) as total_groups,
      SUM(CASE WHEN Name NOT LIKE 'Group %' THEN 1 ELSE 0 END) as named_groups,
      SUM(CASE WHEN Name LIKE 'Group %' THEN 1 ELSE 0 END) as generic_groups
    FROM [etl].[stg_groups]
  `);
  const s = summary.recordset[0];
  console.log('Total groups: ' + s.total_groups);
  console.log('Named groups: ' + s.named_groups + ' (' + Math.round(s.named_groups / s.total_groups * 100) + '%)');
  console.log('Generic groups: ' + s.generic_groups + ' (' + Math.round(s.generic_groups / s.total_groups * 100) + '%)');

  await pool.close();
}

run().catch(console.error);

