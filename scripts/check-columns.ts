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
    options: { encrypt: true, trustServerCertificate: true },
    requestTimeout: 120000
  });
  
  // Check hierarchy distribution for G25565
  const hierDist = await pool.request().query(`
    WITH CertHierarchy AS (
      SELECT 
        ci.CertificateId,
        ci.Product,
        ci.PlanCode,
        STRING_AGG(
          CONCAT(ISNULL(ci.SplitBrokerId,'NULL'), ':', ISNULL(ci.CommissionsSchedule,'NULL')),
          '|'
        ) AS HierarchySignature
      FROM [etl].[input_certificate_info] ci
      WHERE LTRIM(RTRIM(ci.GroupId)) IN ('25565', 'G25565')
        AND ci.CertStatus = 'A'
        AND ci.RecStatus = 'A'
      GROUP BY ci.CertificateId, ci.Product, ci.PlanCode
    )
    SELECT 
      HierarchySignature,
      COUNT(DISTINCT CertificateId) AS CertCount,
      COUNT(*) AS TotalRows
    FROM CertHierarchy
    GROUP BY HierarchySignature
    ORDER BY CertCount DESC
  `);
  
  console.log('G25565 hierarchy signature distribution:');
  let total = 0;
  hierDist.recordset.forEach(r => {
    total += r.CertCount;
    console.log(`  ${r.HierarchySignature?.substring(0,50)}... ${r.CertCount} certs, ${r.TotalRows} rows`);
  });
  console.log(`\nTotal unique signatures: ${hierDist.recordset.length}`);
  console.log(`Total certificates: ${total}`);
  
  await pool.close();
}

main().catch(e => console.error(e));
