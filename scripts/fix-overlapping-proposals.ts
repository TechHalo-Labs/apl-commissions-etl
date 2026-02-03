/**
 * Fix Overlapping Proposals - Second Pass
 * 
 * Identifies certificates that match multiple proposals (ambiguous routing)
 * and routes them to PHA (Policy Hierarchy Assignments) for manual resolution.
 * 
 * Usage:
 *   npx tsx scripts/fix-overlapping-proposals.ts [--dry-run] [--groups G1234,G5678]
 */

import * as sql from 'mssql';

interface OverlappingCert {
  groupId: string;
  certificateId: string;
  product: string;
  planCode: string;
  effectiveDate: Date;
  proposalIds: string;
  proposalCount: number;
}

interface DatabaseConfig {
  server: string;
  database: string;
  user: string;
  password: string;
  options?: {
    encrypt?: boolean;
    trustServerCertificate?: boolean;
  };
}

function parseConnectionString(connStr: string): DatabaseConfig {
  const parts = connStr.split(';').reduce((acc, part) => {
    const [key, value] = part.split('=');
    if (key && value) acc[key.trim().toLowerCase()] = value.trim();
    return acc;
  }, {} as Record<string, string>);

  return {
    server: parts['server'] || parts['data source'] || '',
    database: parts['database'] || parts['initial catalog'] || '',
    user: parts['user id'] || parts['uid'] || '',
    password: parts['password'] || parts['pwd'] || '',
    options: { encrypt: true, trustServerCertificate: true }
  };
}

async function findOverlappingCertificates(pool: sql.ConnectionPool, schema: string, groups?: string[]): Promise<OverlappingCert[]> {
  const groupFilter = groups && groups.length > 0
    ? `AND LTRIM(RTRIM(ci.GroupId)) IN (${groups.map(g => `'${g.replace('G', '')}'`).join(',')})`
    : '';

  const result = await pool.request().query(`
    WITH ActiveCerts AS (
      SELECT
        LTRIM(RTRIM(ci.GroupId)) AS GroupIdRaw,
        'G' + LTRIM(RTRIM(REPLACE(ci.GroupId, 'G', ''))) AS GroupId,
        LTRIM(RTRIM(ci.Product)) AS Product,
        LTRIM(RTRIM(ci.PlanCode)) AS PlanCode,
        TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(ci.CertEffectiveDate)), '')) AS CertEffectiveDate,
        LTRIM(RTRIM(ci.CertificateId)) AS CertificateId
      FROM [${schema}].[raw_certificate_info] ci
      WHERE LTRIM(RTRIM(ci.CertStatus)) = 'A'
        AND LTRIM(RTRIM(ci.RecStatus)) = 'A'
        AND ci.CertEffectiveDate IS NOT NULL
        ${groupFilter}
    ),
    PhaCerts AS (
      SELECT DISTINCT pha.PolicyId AS CertificateId, h.GroupId
      FROM [${schema}].[stg_policy_hierarchy_assignments] pha
      INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId
    ),
    NonPha AS (
      SELECT ac.*
      FROM ActiveCerts ac
      LEFT JOIN PhaCerts p ON p.CertificateId = ac.CertificateId AND p.GroupId = ac.GroupId
      WHERE p.CertificateId IS NULL
    ),
    Matches AS (
      SELECT 
        np.GroupId,
        np.CertificateId, 
        np.Product, 
        np.PlanCode, 
        np.CertEffectiveDate, 
        p.Id AS ProposalId
      FROM NonPha np
      INNER JOIN [${schema}].[stg_proposals] p
        ON p.GroupId = np.GroupId
       AND np.CertEffectiveDate > p.EffectiveDateFrom
       AND np.CertEffectiveDate <= p.EffectiveDateTo
       AND (
         p.ProductCodes IS NULL OR LTRIM(RTRIM(p.ProductCodes)) = '' OR LTRIM(RTRIM(p.ProductCodes)) = '*'
         OR EXISTS (SELECT 1 FROM STRING_SPLIT(p.ProductCodes, ',') s WHERE LTRIM(RTRIM(s.value)) = np.Product)
       )
       AND (
         p.PlanCodes IS NULL OR LTRIM(RTRIM(p.PlanCodes)) = '' OR LTRIM(RTRIM(p.PlanCodes)) = '*'
         OR EXISTS (SELECT 1 FROM STRING_SPLIT(p.PlanCodes, ',') s WHERE LTRIM(RTRIM(s.value)) = np.PlanCode)
       )
    )
    SELECT 
      m.GroupId,
      m.CertificateId, 
      m.Product, 
      m.PlanCode, 
      m.CertEffectiveDate AS EffectiveDate,
      STRING_AGG(m.ProposalId, ',') AS ProposalIds,
      COUNT(DISTINCT m.ProposalId) AS ProposalCount
    FROM Matches m
    GROUP BY m.GroupId, m.CertificateId, m.Product, m.PlanCode, m.CertEffectiveDate
    HAVING COUNT(DISTINCT m.ProposalId) > 1
    ORDER BY m.GroupId, m.CertEffectiveDate
  `);

  return result.recordset.map(r => ({
    groupId: r.GroupId,
    certificateId: r.CertificateId,
    product: r.Product,
    planCode: r.PlanCode,
    effectiveDate: r.EffectiveDate,
    proposalIds: r.ProposalIds,
    proposalCount: r.ProposalCount
  }));
}

async function routeToPhA(
  pool: sql.ConnectionPool, 
  schema: string, 
  certs: OverlappingCert[],
  dryRun: boolean
): Promise<number> {
  if (certs.length === 0) return 0;

  // Group by groupId for batch processing
  const byGroup = new Map<string, OverlappingCert[]>();
  for (const cert of certs) {
    if (!byGroup.has(cert.groupId)) byGroup.set(cert.groupId, []);
    byGroup.get(cert.groupId)!.push(cert);
  }

  let totalRouted = 0;

  for (const [groupId, groupCerts] of byGroup) {
    console.log(`\n  Processing ${groupId}: ${groupCerts.length} overlapping certs`);

    if (dryRun) {
      console.log(`    [DRY RUN] Would route ${groupCerts.length} certs to PHA`);
      totalRouted += groupCerts.length;
      continue;
    }

    // Get hierarchy info for this group (use first available or create)
    const hierarchyResult = await pool.request().query(`
      SELECT TOP 1 h.Id, h.BrokerId, h.BrokerName
      FROM [${schema}].[stg_hierarchies] h
      WHERE h.GroupId = '${groupId}'
    `);

    let hierarchyId: string;
    let brokerId: string | null = null;
    let brokerName: string | null = null;

    if (hierarchyResult.recordset.length > 0) {
      // Use existing hierarchy info as template
      brokerId = hierarchyResult.recordset[0].BrokerId;
      brokerName = hierarchyResult.recordset[0].BrokerName;
    }

    // Get max PHA counter for this group
    const maxCounter = await pool.request().query(`
      SELECT ISNULL(MAX(CAST(
        REPLACE(REPLACE(Id, 'H-PHA-${groupId.replace('G', '')}-', ''), 'H-PHA-OVERLAP-${groupId.replace('G', '')}-', '')
      AS INT)), 0) AS MaxCounter
      FROM [${schema}].[stg_hierarchies]
      WHERE GroupId = '${groupId}' AND Id LIKE 'H-PHA-%'
    `);
    let counter = (maxCounter.recordset[0]?.MaxCounter || 0) + 1;

    for (const cert of groupCerts) {
      hierarchyId = `H-PHA-OVERLAP-${groupId.replace('G', '')}-${counter}`;
      const versionId = `${hierarchyId}-V1`;
      const assignmentId = `PHA-OVERLAP-${groupId.replace('G', '')}-${cert.certificateId}`;

      // Create hierarchy for this PHA
      await pool.request().query(`
        IF NOT EXISTS (SELECT 1 FROM [${schema}].[stg_hierarchies] WHERE Id = '${hierarchyId}')
        INSERT INTO [${schema}].[stg_hierarchies] (Id, GroupId, BrokerId, BrokerName, ProposalId, CurrentVersionId, EffectiveDate, SitusState, Status, Name)
        VALUES ('${hierarchyId}', '${groupId}', ${brokerId ? `'${brokerId}'` : 'NULL'}, ${brokerName ? `'${brokerName}'` : 'NULL'}, NULL, '${versionId}', '${cert.effectiveDate.toISOString().split('T')[0]}', NULL, 0, 'PHA Overlap Resolution for ${cert.certificateId}')
      `);

      // Create hierarchy version
      await pool.request().query(`
        IF NOT EXISTS (SELECT 1 FROM [${schema}].[stg_hierarchy_versions] WHERE Id = '${versionId}')
        INSERT INTO [${schema}].[stg_hierarchy_versions] (Id, HierarchyId, VersionNumber, EffectiveFrom, EffectiveTo, Status)
        VALUES ('${versionId}', '${hierarchyId}', 1, '${cert.effectiveDate.toISOString().split('T')[0]}', '2099-01-01', 0)
      `);

      // Create PHA assignment
      await pool.request().query(`
        IF NOT EXISTS (SELECT 1 FROM [${schema}].[stg_policy_hierarchy_assignments] WHERE PolicyId = '${cert.certificateId}' AND HierarchyId = '${hierarchyId}')
        INSERT INTO [${schema}].[stg_policy_hierarchy_assignments] (Id, PolicyId, HierarchyId, SplitSequence, SplitPercent, WritingBrokerId, IsNonConforming, NonConformantReason)
        VALUES ('${assignmentId}', '${cert.certificateId}', '${hierarchyId}', 1, 100.00, ${brokerId ? `'${brokerId}'` : 'NULL'}, 1, 'Overlapping proposals: ${cert.proposalIds}')
      `);

      counter++;
      totalRouted++;
    }

    console.log(`    ✓ Routed ${groupCerts.length} certs to PHA`);
  }

  return totalRouted;
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const schema = args.includes('--schema') 
    ? args[args.indexOf('--schema') + 1] 
    : 'etl';
  
  let groups: string[] | undefined;
  if (args.includes('--groups')) {
    const groupsArg = args[args.indexOf('--groups') + 1];
    if (groupsArg && !groupsArg.startsWith('--')) {
      groups = groupsArg.split(',').map(g => g.trim());
    }
  }

  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    console.error('ERROR: SQLSERVER environment variable not set');
    process.exit(1);
  }

  const config = parseConnectionString(connectionString);
  const pool = await sql.connect(config);

  try {
    console.log('='.repeat(60));
    console.log('FIX OVERLAPPING PROPOSALS - Route to PHA');
    console.log('='.repeat(60));
    if (dryRun) console.log('MODE: DRY RUN (no changes will be made)\n');
    if (groups) console.log(`GROUPS FILTER: ${groups.join(', ')}\n`);

    console.log('Finding certificates with overlapping proposals...');
    const overlapping = await findOverlappingCertificates(pool, schema, groups);
    
    if (overlapping.length === 0) {
      console.log('\n✓ No overlapping certificates found!');
      return;
    }

    // Summary by group
    const byGroup = new Map<string, number>();
    for (const cert of overlapping) {
      byGroup.set(cert.groupId, (byGroup.get(cert.groupId) || 0) + 1);
    }

    console.log(`\nFound ${overlapping.length} overlapping certificates in ${byGroup.size} groups:`);
    for (const [groupId, count] of Array.from(byGroup.entries()).sort((a, b) => b[1] - a[1]).slice(0, 20)) {
      console.log(`  ${groupId}: ${count} certs`);
    }
    if (byGroup.size > 20) console.log(`  ... and ${byGroup.size - 20} more groups`);

    console.log('\nRouting overlapping certificates to PHA...');
    const routed = await routeToPhA(pool, schema, overlapping, dryRun);

    console.log('\n' + '='.repeat(60));
    console.log(`✅ ${dryRun ? 'Would route' : 'Routed'} ${routed} certificates to PHA`);
    console.log('='.repeat(60));

  } finally {
    await pool.close();
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ Error:', err.message || err);
    process.exit(1);
  });
