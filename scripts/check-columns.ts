import * as sql from 'mssql';

async function main() {
  const connStr = process.env.SQLSERVER;
  if (!connStr) throw new Error('SQLSERVER not set');
  
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  
  const pool = await sql.connect({
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: { encrypt: true, trustServerCertificate: true }
  });
  
  // Check columns in input_certificate_info
  const cols = await pool.request().query(`
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'input_certificate_info'
    ORDER BY ORDINAL_POSITION
  `);
  console.log('Columns in input_certificate_info:');
  cols.recordset.forEach(r => console.log('  ' + r.COLUMN_NAME));
  
  // Check G25565 config distribution using stg_proposals
  const configDist = await pool.request().query(`
    SELECT 
      SplitConfigHash,
      ProductCodes,
      COUNT(*) AS ProposalCount
    FROM [etl].[stg_proposals]
    WHERE GroupId = 'G25565'
    GROUP BY SplitConfigHash, ProductCodes
    ORDER BY ProposalCount DESC
  `);
  console.log('\nG25565 proposal configs:');
  configDist.recordset.forEach(r => console.log(`  ${r.SplitConfigHash?.substring(0,16)}... ${r.ProductCodes} (${r.ProposalCount} proposals)`));
  
  await pool.close();
}

main().catch(e => console.error(e));
