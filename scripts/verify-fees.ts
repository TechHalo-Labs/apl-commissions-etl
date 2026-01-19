/**
 * Verify Fees Implementation
 * ==========================
 * Validates that fee normalization and export worked correctly
 * 
 * Usage:
 *   npx tsx scripts/verify-fees.ts
 */

import * as sql from 'mssql';

const config: sql.config = {
  server: process.env.SQLSERVER_HOST || 'halo-sql.database.windows.net',
  database: process.env.SQLSERVER_DATABASE || 'halo-sqldb',
  user: process.env.SQLSERVER_USER || '***REMOVED***',
  password: process.env.SQLSERVER_PASSWORD || '***REMOVED***',
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  requestTimeout: 60000,
};

interface VerificationResult {
  check: string;
  status: 'PASS' | 'FAIL' | 'WARN';
  message: string;
  count?: number;
}

async function runCheck(
  pool: sql.ConnectionPool,
  check: string,
  query: string,
  expectedMin: number = 0,
  warnIfZero: boolean = false
): Promise<VerificationResult> {
  try {
    const result = await pool.request().query(query);
    const count = result.recordset[0]?.cnt || result.recordset[0]?.count || 0;

    if (count >= expectedMin) {
      return {
        check,
        status: 'PASS',
        message: `Found ${count.toLocaleString()} records`,
        count,
      };
    } else if (warnIfZero && count === 0) {
      return {
        check,
        status: 'WARN',
        message: `Found 0 records (expected > 0)`,
        count,
      };
    } else {
      return {
        check,
        status: 'FAIL',
        message: `Found ${count.toLocaleString()} records (expected >= ${expectedMin})`,
        count,
      };
    }
  } catch (err: any) {
    return {
      check,
      status: 'FAIL',
      message: `Query failed: ${err.message}`,
    };
  }
}

async function main() {
  console.log('='.repeat(80));
  console.log('FEE NORMALIZATION VERIFICATION');
  console.log('='.repeat(80));
  console.log('');

  const pool = await sql.connect(config);
  console.log('Connected to SQL Server');
  console.log('');

  const results: VerificationResult[] = [];

  // ============================================================================
  // STAGING CHECKS
  // ============================================================================
  console.log('STAGING CHECKS (etl.stg_fees)');
  console.log('-'.repeat(80));

  results.push(await runCheck(
    pool,
    'Raw fees loaded',
    'SELECT COUNT(*) as cnt FROM [etl].[raw_fees]',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'Staging fees created',
    'SELECT COUNT(*) as cnt FROM [etl].[stg_fees]',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'Fees with resolved GroupId',
    'SELECT COUNT(*) as cnt FROM [etl].[stg_fees] WHERE GroupId IS NOT NULL',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'Fees with resolved BrokerName',
    'SELECT COUNT(*) as cnt FROM [etl].[stg_fees] WHERE RecipientBrokerName IS NOT NULL',
    1,
    true
  ));

  // Check canonical codes distribution
  console.log('');
  console.log('Canonical Fee Type Distribution:');
  const codeDistResult = await pool.request().query(`
    SELECT 
      FeeTypeCode,
      FeeTypeName,
      COUNT(*) AS cnt
    FROM [etl].[stg_fees]
    GROUP BY FeeTypeCode, FeeTypeName
    ORDER BY cnt DESC
  `);
  console.table(codeDistResult.recordset);

  // ============================================================================
  // PRODUCTION CHECKS
  // ============================================================================
  console.log('');
  console.log('PRODUCTION CHECKS (dbo.FeeSchedules*)');
  console.log('-'.repeat(80));

  results.push(await runCheck(
    pool,
    'FeeTypes seeded',
    `SELECT COUNT(*) as cnt FROM [dbo].[FeeTypes] 
     WHERE Code IN ('CERT_FEE', 'FLAT_FEE', 'PROD_FEE', 'ONETIME_FEE', 'PREM_FEE_M', 'PREM_FEE_A')`,
    6
  ));

  results.push(await runCheck(
    pool,
    'FeeSchedules created',
    'SELECT COUNT(*) as cnt FROM [dbo].[FeeSchedules]',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'FeeScheduleVersions created',
    'SELECT COUNT(*) as cnt FROM [dbo].[FeeScheduleVersions]',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'FeeScheduleVersions Active',
    'SELECT COUNT(*) as cnt FROM [dbo].[FeeScheduleVersions] WHERE [Status] = 1',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'FeeScheduleItems created',
    'SELECT COUNT(*) as cnt FROM [dbo].[FeeScheduleItems]',
    1,
    true
  ));

  results.push(await runCheck(
    pool,
    'Proposals with FeeSchedules',
    `SELECT COUNT(DISTINCT p.Id) as cnt 
     FROM [dbo].[Proposals] p
     INNER JOIN [dbo].[FeeSchedules] fs ON fs.ProposalId = p.Id`,
    1,
    true
  ));

  // Check fee schedule items by type
  console.log('');
  console.log('Fee Schedule Items by Type:');
  const itemDistResult = await pool.request().query(`
    SELECT 
      FeeTypeCode,
      FeeTypeName,
      Frequency,
      Basis,
      COUNT(*) AS cnt,
      AVG(CASE WHEN Amount IS NOT NULL THEN Amount ELSE 0 END) AS AvgAmount,
      AVG(CASE WHEN [Percent] IS NOT NULL THEN [Percent] ELSE 0 END) AS AvgPercent
    FROM [dbo].[FeeScheduleItems]
    GROUP BY FeeTypeCode, FeeTypeName, Frequency, Basis
    ORDER BY cnt DESC
  `);
  console.table(itemDistResult.recordset);

  // ============================================================================
  // LINKAGE CHECKS
  // ============================================================================
  console.log('');
  console.log('LINKAGE CHECKS');
  console.log('-'.repeat(80));

  // Check version-to-schedule linkage
  const linkageResult = await pool.request().query(`
    SELECT 
      'FeeSchedules without Versions' AS issue,
      COUNT(*) AS cnt
    FROM [dbo].[FeeSchedules] fs
    LEFT JOIN [dbo].[FeeScheduleVersions] fsv ON fsv.FeeScheduleId = fs.Id
    WHERE fsv.Id IS NULL
    
    UNION ALL
    
    SELECT 
      'FeeScheduleVersions without Items' AS issue,
      COUNT(*) AS cnt
    FROM [dbo].[FeeScheduleVersions] fsv
    LEFT JOIN [dbo].[FeeScheduleItems] fsi ON fsi.FeeScheduleVersionId = fsv.Id
    WHERE fsi.Id IS NULL
  `);

  for (const row of linkageResult.recordset) {
    if (row.cnt > 0) {
      results.push({
        check: row.issue,
        status: 'WARN',
        message: `Found ${row.cnt} orphaned records`,
        count: row.cnt,
      });
    } else {
      results.push({
        check: row.issue,
        status: 'PASS',
        message: 'No orphaned records',
        count: 0,
      });
    }
  }

  // ============================================================================
  // SUMMARY
  // ============================================================================
  console.log('');
  console.log('='.repeat(80));
  console.log('VERIFICATION SUMMARY');
  console.log('='.repeat(80));
  console.log('');

  const passed = results.filter(r => r.status === 'PASS').length;
  const failed = results.filter(r => r.status === 'FAIL').length;
  const warned = results.filter(r => r.status === 'WARN').length;

  for (const result of results) {
    const icon = result.status === 'PASS' ? '✅' : result.status === 'WARN' ? '⚠️' : '❌';
    console.log(`${icon} ${result.check}: ${result.message}`);
  }

  console.log('');
  console.log(`TOTAL: ${passed} passed, ${warned} warnings, ${failed} failed`);
  console.log('');

  if (failed > 0) {
    console.log('❌ VERIFICATION FAILED - Fix issues before proceeding');
    process.exit(1);
  } else if (warned > 0) {
    console.log('⚠️  VERIFICATION PASSED WITH WARNINGS - Review warnings');
  } else {
    console.log('✅ VERIFICATION PASSED - Fee normalization is working correctly');
  }

  await pool.close();
}

main().catch(err => {
  console.error('Verification failed:', err);
  process.exit(1);
});
