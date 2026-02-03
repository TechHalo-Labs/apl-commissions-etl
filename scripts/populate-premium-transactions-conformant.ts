/**
 * Populate PremiumTransactions for Conformant Groups
 * 
 * This script:
 * 1. Finds conformant groups from new_data.CertificateInfo
 * 2. For each conformant group, gets certificates in that group
 * 3. Maps certificates to production Policies by PolicyNumber
 * 4. Populates PremiumTransactions from raw_premiums for those certificates
 * 
 * Usage:
 *   npx tsx scripts/populate-premium-transactions-conformant.ts [options]
 * 
 * Options:
 *   --debug, -d                  Process only the first group
 *   --max-groups <number>        Limit processing to N groups
 *   --transaction-date <date>    Override transaction date (YYYY-MM-DD format)
 * 
 * Examples:
 *   npx tsx scripts/populate-premium-transactions-conformant.ts --debug
 *   npx tsx scripts/populate-premium-transactions-conformant.ts --max-groups 5
 *   npx tsx scripts/populate-premium-transactions-conformant.ts --transaction-date 2025-01-30
 *   npx tsx scripts/populate-premium-transactions-conformant.ts --max-groups 10 --transaction-date 2025-01-30
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

interface ConformantGroup {
  GroupId: string;
  GroupName: string | null;
  SitusState: string | null;
  TotalCertificates: number;
  ConformantCertificates: number;
  NonConformantCertificates: number;
  ConformancePercentage: number;
  GroupClassification: string;
}

interface PremiumTransaction {
  CertificateId: bigint;
  TransactionDate: Date;
  PremiumAmount: number;
  BillingPeriodStart: Date | null;
  BillingPeriodEnd: Date | null;
  PaymentStatus: string;
  SourceSystem: string;
}

/**
 * Get conformant groups from the analysis query
 */
async function getConformantGroups(
  pool: sql.ConnectionPool,
  pocEtlSchema: string,
  debugMode: boolean,
  maxGroups: number | null
): Promise<ConformantGroup[]> {
  console.log('\n📊 Finding conformant groups...');

  const query = `
    -- Clean up temp tables if they exist
    DROP TABLE IF EXISTS #cert_keys;
    DROP TABLE IF EXISTS #cert_proposal_matches;
    DROP TABLE IF EXISTS #cert_classification;
    DROP TABLE IF EXISTS #conformant_groups;

    -- Step 1: Extract certificate keys from new_data.CertificateInfo
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        YEAR(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS EffectiveYear,
        LTRIM(RTRIM(ci.Product)) AS ProductCode,
        CASE 
            WHEN ci.PlanCode IS NULL THEN '*'
            WHEN LTRIM(RTRIM(ci.PlanCode)) = '' THEN '*'
            WHEN LTRIM(RTRIM(ci.PlanCode)) = 'NULL' THEN '*'
            WHEN LTRIM(RTRIM(ci.PlanCode)) = 'N/A' THEN '*'
            ELSE LTRIM(RTRIM(ci.PlanCode))
        END AS PlanCode,
        TRY_CAST(ci.CertificateId AS BIGINT) AS CertificateId,
        TRY_CAST(ci.CertEffectiveDate AS DATE) AS CertEffectiveDate
    INTO #cert_keys
    FROM new_data.CertificateInfo ci
    WHERE ci.CertStatus = 'A'
      AND ci.RecStatus = 'A'
      AND ci.GroupId IS NOT NULL
      AND LTRIM(RTRIM(ci.GroupId)) <> ''
      AND ci.Product IS NOT NULL
      AND LTRIM(RTRIM(ci.Product)) <> '';

    -- Step 2: Map certificates to proposals
    SELECT 
        c.GroupId,
        c.EffectiveYear,
        c.ProductCode,
        c.PlanCode,
        c.CertificateId,
        c.CertEffectiveDate,
        COUNT(pkm.ProposalId) AS MatchCount,
        STRING_AGG(pkm.ProposalId, ', ') AS MatchedProposalIds
    INTO #cert_proposal_matches
    FROM #cert_keys c
    LEFT JOIN [${pocEtlSchema}].[stg_proposal_key_mapping] pkm
        ON pkm.GroupId = c.GroupId
        AND pkm.EffectiveYear = c.EffectiveYear
        AND pkm.ProductCode = c.ProductCode
        AND pkm.PlanCode = c.PlanCode
    GROUP BY 
        c.GroupId, c.EffectiveYear, c.ProductCode, c.PlanCode,
        c.CertificateId, c.CertEffectiveDate;

    -- Step 3: Classify certificates
    SELECT
        GroupId,
        CertificateId,
        ProductCode,
        PlanCode,
        EffectiveYear,
        MatchCount,
        MatchedProposalIds,
        CASE
            WHEN MatchCount = 1 THEN 'Conformant'
            WHEN MatchCount = 0 THEN 'Non-Conformant (No Match)'
            WHEN MatchCount > 1 THEN 'Non-Conformant (Multiple Matches)'
        END AS ConformanceStatus
    INTO #cert_classification
    FROM #cert_proposal_matches;

    -- Step 4: Aggregate by group
    WITH GroupStats AS (
        SELECT 
            cc.GroupId,
            g.Name AS GroupName,
            g.[State] AS SitusState,
            COUNT(*) AS TotalCertificates,
            SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) AS ConformantCertificates,
            SUM(CASE WHEN cc.ConformanceStatus LIKE 'Non-Conformant%' THEN 1 ELSE 0 END) AS NonConformantCertificates,
            CAST(SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) * 100.0 / 
                 NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS ConformancePercentage
        FROM #cert_classification cc
        LEFT JOIN [${pocEtlSchema}].[stg_groups] g ON g.Id = cc.GroupId
        WHERE 1=1
            AND cc.GroupId IS NOT NULL
            AND cc.GroupId <> ''
            AND cc.GroupId <> 'G'
            AND NOT (LEN(REPLACE(cc.GroupId, 'G', '')) = 5 AND LEFT(REPLACE(cc.GroupId, 'G', ''), 1) = '7')
            AND (g.Name IS NULL OR g.Name NOT LIKE 'Universal Trucking%')
        GROUP BY cc.GroupId, g.Name, g.[State]
    )
    SELECT 
        GroupId,
        GroupName,
        SitusState,
        TotalCertificates,
        ConformantCertificates,
        NonConformantCertificates,
        ConformancePercentage,
        CASE
            WHEN ConformancePercentage = 100.0 THEN 'Conformant'
            WHEN ConformancePercentage >= 95.0 THEN 'Nearly Conformant (>=95%)'
            ELSE 'Non-Conformant'
        END AS GroupClassification
    INTO #conformant_groups
    FROM GroupStats;

    -- Return only conformant groups
    SELECT 
        GroupId,
        GroupName,
        SitusState,
        TotalCertificates,
        ConformantCertificates,
        NonConformantCertificates,
        ConformancePercentage,
        GroupClassification
    FROM #conformant_groups
    WHERE GroupClassification = 'Conformant'
    ORDER BY TotalCertificates DESC;
  `;

  const result = await pool.request().query(query);
  let groups: ConformantGroup[] = result.recordset.map((row: any) => ({
    GroupId: row.GroupId,
    GroupName: row.GroupName,
    SitusState: row.SitusState,
    TotalCertificates: row.TotalCertificates,
    ConformantCertificates: row.ConformantCertificates,
    NonConformantCertificates: row.NonConformantCertificates,
    ConformancePercentage: Number.parseFloat(row.ConformancePercentage),
    GroupClassification: row.GroupClassification,
  }));

  console.log(`   Found ${groups.length} conformant groups`);

  if (debugMode && groups.length > 0) {
    console.log(`   🔍 DEBUG MODE: Processing only first group: ${groups[0].GroupId}`);
    return [groups[0]];
  }

  if (maxGroups && maxGroups < groups.length) {
    console.log(`   📊 Limiting to ${maxGroups} groups`);
    groups = groups.slice(0, maxGroups);
  }

  return groups;
}

/**
 * Get certificates for a specific group from new_data.CertificateInfo
 */
async function getCertificatesForGroup(
  pool: sql.ConnectionPool,
  groupId: string
): Promise<bigint[]> {
  // Remove 'G' prefix if present
  const cleanGroupId = groupId.startsWith('G') ? groupId.substring(1) : groupId;

  const query = `
    SELECT DISTINCT
        TRY_CAST(CertificateId AS BIGINT) AS CertificateId
    FROM new_data.CertificateInfo
    WHERE CertStatus = 'A'
      AND RecStatus = 'A'
      AND LTRIM(RTRIM(GroupId)) = @groupId
      AND CertificateId IS NOT NULL
      AND TRY_CAST(CertificateId AS BIGINT) IS NOT NULL;
  `;

  const result = await pool
    .request()
    .input('groupId', sql.NVarChar, cleanGroupId)
    .query(query);

  return result.recordset.map((row: any) => BigInt(row.CertificateId));
}

/**
 * Get premium transactions for certificates from raw_premiums or stg_policies
 */
async function getPremiumTransactions(
  pool: sql.ConnectionPool,
  etlSchema: string,
  pocEtlSchema: string,
  certificateIds: bigint[],
  transactionDate: Date | null
): Promise<PremiumTransaction[]> {
  if (certificateIds.length === 0) {
    return [];
  }

  // Convert bigint array to string array for SQL IN clause
  const certIdStrings = certificateIds.map(id => id.toString());
  const inClause = certIdStrings.join(',');

  const transactions: PremiumTransaction[] = [];

  // First, try to get from raw_premiums
  try {
    const transDateCol = transactionDate 
      ? `CAST('${transactionDate.toISOString().split('T')[0]}' AS DATE)`
      : 'TRY_CAST(p.DatePost AS DATE)';
    
    const query = `
      SELECT DISTINCT
          TRY_CAST(p.Policy AS BIGINT) AS CertificateId,
          ${transDateCol} AS TransactionDate,
          TRY_CAST(p.Amount AS DECIMAL(18,2)) AS PremiumAmount,
          TRY_CAST(p.DatePaidTo AS DATE) AS BillingPeriodStart,
          DATEADD(MONTH, 1, TRY_CAST(p.DatePaidTo AS DATE)) AS BillingPeriodEnd,
          'Completed' AS PaymentStatus,
          'raw_premiums' AS SourceSystem
      FROM [${pocEtlSchema}].[raw_premiums] p
      WHERE p.Policy IS NOT NULL 
        AND LTRIM(RTRIM(p.Policy)) <> ''
        AND TRY_CAST(p.Policy AS BIGINT) IN (${inClause})
        AND TRY_CAST(p.Amount AS DECIMAL(18,2)) IS NOT NULL
        ${transactionDate ? '' : 'AND TRY_CAST(p.DatePost AS DATE) IS NOT NULL'};
    `;

    const result = await pool.request().query(query);
    const rawPremiums = result.recordset.map((row: any) => ({
      CertificateId: BigInt(row.CertificateId),
      TransactionDate: transactionDate || row.TransactionDate,
      PremiumAmount: Number.parseFloat(row.PremiumAmount),
      BillingPeriodStart: row.BillingPeriodStart,
      BillingPeriodEnd: row.BillingPeriodEnd,
      PaymentStatus: row.PaymentStatus,
      SourceSystem: row.SourceSystem,
    }));
    transactions.push(...rawPremiums);
  } catch (error: any) {
    console.log(`   ⚠️  Could not read from raw_premiums: ${error.message}`);
  }

  // If no premiums found, fall back to stg_policies (using CertPremium from CertificateInfo)
  if (transactions.length === 0) {
    console.log(`   📋 Falling back to CertificateInfo.CertPremium...`);
    
    const transDateCol = transactionDate 
      ? `CAST('${transactionDate.toISOString().split('T')[0]}' AS DATE)`
      : 'TRY_CAST(ci.CertEffectiveDate AS DATE)';
    
    const fallbackQuery = `
      SELECT DISTINCT
          TRY_CAST(ci.CertificateId AS BIGINT) AS CertificateId,
          ${transDateCol} AS TransactionDate,
          TRY_CAST(ci.CertPremium AS DECIMAL(18,2)) AS PremiumAmount,
          TRY_CAST(ci.CertEffectiveDate AS DATE) AS BillingPeriodStart,
          DATEADD(MONTH, 1, TRY_CAST(ci.CertEffectiveDate AS DATE)) AS BillingPeriodEnd,
          'Completed' AS PaymentStatus,
          'CertificateInfo' AS SourceSystem
      FROM new_data.CertificateInfo ci
      WHERE ci.CertificateId IS NOT NULL
        AND TRY_CAST(ci.CertificateId AS BIGINT) IN (${inClause})
        AND ci.CertStatus = 'A'
        AND ci.RecStatus = 'A'
        AND TRY_CAST(ci.CertPremium AS DECIMAL(18,2)) IS NOT NULL
        AND TRY_CAST(ci.CertPremium AS DECIMAL(18,2)) > 0
        ${transactionDate ? '' : 'AND TRY_CAST(ci.CertEffectiveDate AS DATE) IS NOT NULL'};
    `;

    try {
      const result = await pool.request().query(fallbackQuery);
      const policyPremiums = result.recordset.map((row: any) => ({
        CertificateId: BigInt(row.CertificateId),
        TransactionDate: transactionDate || row.TransactionDate,
        PremiumAmount: Number.parseFloat(row.PremiumAmount),
        BillingPeriodStart: row.BillingPeriodStart,
        BillingPeriodEnd: row.BillingPeriodEnd,
        PaymentStatus: row.PaymentStatus,
        SourceSystem: row.SourceSystem,
      }));
      transactions.push(...policyPremiums);
    } catch (error: any) {
      console.log(`   ⚠️  Could not read from CertificateInfo: ${error.message}`);
    }
  }

  return transactions;
}

/**
 * Map certificate IDs to production Policy IDs
 */
async function mapCertificatesToPolicyIds(
  pool: sql.ConnectionPool,
  productionSchema: string,
  certificateIds: bigint[]
): Promise<Map<bigint, bigint>> {
  if (certificateIds.length === 0) {
    return new Map();
  }

  const certIdStrings = certificateIds.map(id => id.toString());
  const inClause = certIdStrings.join(',');

  const query = `
    SELECT 
        CAST(PolicyNumber AS BIGINT) AS CertificateId,
        Id AS PolicyId
    FROM [${productionSchema}].[Policies]
    WHERE TRY_CAST(PolicyNumber AS BIGINT) IN (${inClause})
      AND PolicyNumber IS NOT NULL
      AND IsDeleted = 0;
  `;

  try {
    const result = await pool.request().query(query);
    const mapping = new Map<bigint, bigint>();
    
    for (const row of result.recordset) {
      mapping.set(BigInt(row.CertificateId), BigInt(row.PolicyId));
    }
    
    return mapping;
  } catch (error: any) {
    console.error(`   ⚠️  Error mapping certificates to policy IDs: ${error.message}`);
    return new Map();
  }
}

/**
 * Insert premium transactions into dbo.PremiumTransactions
 */
async function insertPremiumTransactions(
  pool: sql.ConnectionPool,
  productionSchema: string,
  transactions: PremiumTransaction[],
  certToPolicyMap: Map<bigint, bigint>
): Promise<number> {
  if (transactions.length === 0) {
    return 0;
  }

  console.log(`   Inserting ${transactions.length} premium transactions using bulk INSERT INTO ... SELECT...`);

  const now = new Date();
  
  // Format dates for SQL
  const formatDate = (d: Date | null): string => {
    if (!d) return 'NULL';
    return `CAST('${d.toISOString().split('T')[0]}' AS DATE)`;
  };

  const formatDateTime = (d: Date): string => {
    return `CAST('${d.toISOString()}' AS DATETIME2)`;
  };

  // Escape strings for SQL
  const escapeSql = (s: string): string => {
    return s.replaceAll("'", "''");
  };

  // Insert in batches to avoid huge SQL statements
  let inserted = 0;
  const batchSize = 1000; // Larger batches since we're using VALUES directly

  for (let i = 0; i < transactions.length; i += batchSize) {
    const batch = transactions.slice(i, i + batchSize);
    
    // Build VALUES clause directly in SQL (safe because we control the data source)
    const values = batch.map((tx) => {
      // Map certificate ID to policy ID
      const policyId = certToPolicyMap.get(tx.CertificateId);
      if (!policyId) {
        console.warn(`   ⚠️  No policy mapping for certificate ${tx.CertificateId}, skipping`);
        return null;
      }
      
      const txDate = formatDate(tx.TransactionDate);
      const billStart = formatDate(tx.BillingPeriodStart);
      const billEnd = formatDate(tx.BillingPeriodEnd);
      const payStatus = escapeSql(tx.PaymentStatus || 'Processed');
      const sourceSys = escapeSql(tx.SourceSystem || 'ETL');
      
      return `(
        ${policyId},
        ${txDate},
        ${tx.PremiumAmount},
        ${billStart},
        ${billEnd},
        '${payStatus}',
        '${sourceSys}',
        ${formatDateTime(now)},
        0,
        NULL,
        NULL,
        ${formatDateTime(now)},
        0
      )`;
    }).filter(v => v !== null).join(',\n');

    if (!values) {
      console.log(`   ⚠️  Batch ${Math.floor(i / batchSize) + 1}: No valid transactions to insert`);
      continue;
    }

    const insertSql = `
      INSERT INTO [${productionSchema}].[PremiumTransactions] (
          certificateId, transactionDate, premiumAmount, 
          billingPeriodStart, billingPeriodEnd, paymentStatus, sourceSystem,
          CreatedDate, isDryRun, sourcePolicyId, sourceTagIds,
          CreationTime, IsDeleted
      )
      SELECT 
          certificateId, transactionDate, premiumAmount,
          billingPeriodStart, billingPeriodEnd, paymentStatus, sourceSystem,
          CreatedDate, isDryRun, sourcePolicyId, sourceTagIds,
          CreationTime, IsDeleted
      FROM (VALUES ${values}) AS v(
          certificateId, transactionDate, premiumAmount,
          billingPeriodStart, billingPeriodEnd, paymentStatus, sourceSystem,
          CreatedDate, isDryRun, sourcePolicyId, sourceTagIds,
          CreationTime, IsDeleted
      )
      WHERE NOT EXISTS (
          SELECT 1 FROM [${productionSchema}].[PremiumTransactions] pt
          WHERE pt.certificateId = v.certificateId
            AND pt.transactionDate = v.transactionDate
            AND pt.premiumAmount = v.premiumAmount
      );
    `;

    try {
      const batchResult = await pool.request().query(insertSql);
      if (batchResult.rowsAffected && batchResult.rowsAffected.length > 0) {
        const batchInserted = batchResult.rowsAffected.reduce((a, b) => a + b, 0);
        inserted += batchInserted;
        if (batchInserted > 0) {
          console.log(`   ✅ Batch ${Math.floor(i / batchSize) + 1}: Inserted ${batchInserted} transactions`);
        }
      }
    } catch (error: any) {
      console.error(`   ⚠️  Error inserting batch ${Math.floor(i / batchSize) + 1}: ${error.message}`);
      // Continue with next batch
    }
  }

  return inserted;
}

/**
 * Process a single conformant group
 */
async function processGroup(
  pool: sql.ConnectionPool,
  etlSchema: string,
  pocEtlSchema: string,
  productionSchema: string,
  group: ConformantGroup,
  transactionDate: Date | null
): Promise<{ certificates: number; transactions: number; inserted: number }> {
  console.log(`\n📦 Processing group: ${group.GroupId} (${group.GroupName || 'N/A'})`);
  console.log(`   Certificates: ${group.TotalCertificates}`);

  // Get certificates for this group
  const certificates = await getCertificatesForGroup(pool, group.GroupId);
  console.log(`   Found ${certificates.length} certificates in new_data.CertificateInfo`);

  if (certificates.length === 0) {
    console.log(`   ⚠️  No certificates found, skipping`);
    return { certificates: 0, transactions: 0, inserted: 0 };
  }

  // Map certificates to production policy IDs
  console.log(`   Mapping certificates to production Policies...`);
  const certToPolicyMap = await mapCertificatesToPolicyIds(pool, productionSchema, certificates);
  console.log(`   Mapped ${certToPolicyMap.size} certificates to policy IDs`);

  if (certToPolicyMap.size === 0) {
    console.log(`   ⚠️  No certificates mapped to production policies, skipping`);
    return { certificates: certificates.length, transactions: 0, inserted: 0 };
  }

  // Get premium transactions for these certificates
  const transactions = await getPremiumTransactions(pool, etlSchema, pocEtlSchema, certificates, transactionDate);
  console.log(`   Found ${transactions.length} premium transactions`);

  if (transactions.length === 0) {
    console.log(`   ⚠️  No premium transactions found, skipping`);
    return { certificates: certificates.length, transactions: 0, inserted: 0 };
  }

  // Filter to only transactions with valid policy mappings
  const validTransactions = transactions.filter(tx => certToPolicyMap.has(tx.CertificateId));
  console.log(`   ${validTransactions.length} transactions have valid policy mappings`);

  if (validTransactions.length === 0) {
    console.log(`   ⚠️  No transactions with valid policy mappings, skipping`);
    return { certificates: certificates.length, transactions: transactions.length, inserted: 0 };
  }

  // Insert into PremiumTransactions
  const inserted = await insertPremiumTransactions(pool, productionSchema, validTransactions, certToPolicyMap);
  console.log(`   ✅ Inserted ${inserted} new premium transactions`);

  return {
    certificates: certificates.length,
    transactions: validTransactions.length,
    inserted,
  };
}

/**
 * Parse command line arguments
 */
function parseArgs(): {
  debugMode: boolean;
  maxGroups: number | null;
  transactionDate: Date | null;
} {
  const args = process.argv.slice(2);
  let debugMode = false;
  let maxGroups: number | null = null;
  let transactionDate: Date | null = null;

  let i = 0;
  while (i < args.length) {
    const arg = args[i];
    
    if (arg === '--debug' || arg === '-d') {
      debugMode = true;
      i++;
    } else if (arg === '--max-groups' && i + 1 < args.length) {
      maxGroups = Number.parseInt(args[i + 1], 10);
      if (Number.isNaN(maxGroups) || maxGroups <= 0) {
        throw new TypeError('--max-groups must be a positive integer');
      }
      i += 2;
    } else if (arg === '--transaction-date' && i + 1 < args.length) {
      transactionDate = new Date(args[i + 1]);
      if (Number.isNaN(transactionDate.getTime())) {
        throw new TypeError('--transaction-date must be a valid date (YYYY-MM-DD)');
      }
      i += 2;
    } else {
      i++;
    }
  }

  return { debugMode, maxGroups, transactionDate };
}

/**
 * Main function
 */
async function main() {
  console.log('============================================================');
  console.log('POPULATE PREMIUM TRANSACTIONS - CONFORMANT GROUPS');
  console.log('============================================================\n');

  const { debugMode, maxGroups, transactionDate } = parseArgs();

  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  // Use poc_etl for staging tables (matches the SQL query)
  const etlSchema = config.database.pocMode ? 'poc_etl' : (config.database.schemas.processing || 'etl');
  const productionSchema = config.database.schemas.production || 'dbo';

  if (debugMode) {
    console.log('🔍 DEBUG MODE: Processing only first conformant group\n');
  }

  if (maxGroups) {
    console.log(`📊 MAX GROUPS: Processing up to ${maxGroups} groups\n`);
  }

  if (transactionDate) {
    console.log(`📅 TRANSACTION DATE: Using ${transactionDate.toISOString().split('T')[0]} for all transactions\n`);
  }

  try {
    // Get conformant groups (use poc_etl schema for staging tables)
    const pocEtlSchema = config.database.pocMode ? 'poc_etl' : (config.database.schemas.processing || 'etl');
    const groups = await getConformantGroups(pool, pocEtlSchema, debugMode, maxGroups);

    if (groups.length === 0) {
      console.log('❌ No conformant groups found');
      return;
    }

    console.log(`\n📊 Processing ${groups.length} conformant group(s)...\n`);

    // Process each group
    let totalCertificates = 0;
    let totalTransactions = 0;
    let totalInserted = 0;

    for (const group of groups) {
      const result = await processGroup(pool, etlSchema, pocEtlSchema, productionSchema, group, transactionDate);
      totalCertificates += result.certificates;
      totalTransactions += result.transactions;
      totalInserted += result.inserted;
    }

    // Summary
    console.log('\n============================================================');
    console.log('SUMMARY');
    console.log('============================================================');
    console.log(`Groups processed: ${groups.length}`);
    console.log(`Total certificates: ${totalCertificates}`);
    console.log(`Total premium transactions found: ${totalTransactions}`);
    console.log(`Total premium transactions inserted: ${totalInserted}`);
    console.log('============================================================\n');

  } catch (error: any) {
    console.error('\n❌ Error:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

// Run if executed directly
if (require.main === module) {
  main().catch(console.error);
}

export { main };
