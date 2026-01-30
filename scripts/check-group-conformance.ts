/**
 * Check if a specific group is conformant
 */

import * as sql from 'mssql';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function checkGroupConformance(pool: sql.ConnectionPool, pocEtlSchema: string, groupId: string) {
  console.log(`\n🔍 Checking conformance for group: ${groupId}\n`);

  const query = `
    -- Clean up temp tables if they exist
    DROP TABLE IF EXISTS #cert_keys;
    DROP TABLE IF EXISTS #cert_proposal_matches;
    DROP TABLE IF EXISTS #cert_classification;
    DROP TABLE IF EXISTS #group_stats;

    -- Step 1: Extract certificate keys from new_data.CertificateInfo for this group
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
      AND CONCAT('G', LTRIM(RTRIM(ci.GroupId))) = @groupId
      AND ci.Product IS NOT NULL
      AND LTRIM(RTRIM(ci.Product)) <> '';

    DECLARE @total_certs INT = @@ROWCOUNT;
    PRINT 'Total certificates for group: ' + CAST(@total_certs AS VARCHAR);

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
    SELECT 
        cc.GroupId,
        g.Name AS GroupName,
        g.[State] AS SitusState,
        COUNT(*) AS TotalCertificates,
        SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) AS ConformantCertificates,
        SUM(CASE WHEN cc.ConformanceStatus LIKE 'Non-Conformant%' THEN 1 ELSE 0 END) AS NonConformantCertificates,
        CAST(SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) * 100.0 / 
             NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS ConformancePercentage
    INTO #group_stats
    FROM #cert_classification cc
    LEFT JOIN [${pocEtlSchema}].[stg_groups] g ON g.Id = cc.GroupId
    GROUP BY cc.GroupId, g.Name, g.[State];

    -- Return results
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
    FROM #group_stats;

    -- Show non-conformant certificates
    SELECT TOP 20
        CertificateId,
        ProductCode,
        PlanCode,
        EffectiveYear,
        MatchCount,
        MatchedProposalIds,
        ConformanceStatus
    FROM #cert_classification
    WHERE ConformanceStatus LIKE 'Non-Conformant%'
    ORDER BY MatchCount DESC, CertificateId;
  `;

  const result = await pool.request()
    .input('groupId', sql.NVarChar, groupId)
    .query(query);

  // Parse results
  if (result.recordset.length === 0) {
    console.log('❌ No data found for this group');
    return;
  }

  const groupStats = result.recordset[0];
  console.log('═══════════════════════════════════════════════════════════');
  console.log('GROUP CONFORMANCE ANALYSIS');
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`Group ID: ${groupStats.GroupId}`);
  console.log(`Group Name: ${groupStats.GroupName || 'N/A'}`);
  console.log(`State: ${groupStats.SitusState || 'N/A'}`);
  console.log(`Total Certificates: ${groupStats.TotalCertificates}`);
  console.log(`Conformant Certificates: ${groupStats.ConformantCertificates}`);
  console.log(`Non-Conformant Certificates: ${groupStats.NonConformantCertificates}`);
  console.log(`Conformance Percentage: ${groupStats.ConformancePercentage}%`);
  console.log(`Classification: ${groupStats.GroupClassification}`);
  console.log('═══════════════════════════════════════════════════════════\n');

  // Get non-conformant examples
  const nonConformantQuery = `
    SELECT TOP 20
        CertificateId,
        ProductCode,
        PlanCode,
        EffectiveYear,
        MatchCount,
        MatchedProposalIds,
        ConformanceStatus
    FROM #cert_classification
    WHERE ConformanceStatus LIKE 'Non-Conformant%'
    ORDER BY MatchCount DESC, CertificateId;
  `;

  try {
    const nonConformantResult = await pool.request().query(nonConformantQuery);
    if (nonConformantResult.recordset.length > 0) {
      console.log(`\n⚠️  Non-Conformant Certificates (showing up to 20):`);
      console.log('─────────────────────────────────────────────────────────');
      nonConformantResult.recordset.forEach((cert: any, idx: number) => {
        console.log(`${idx + 1}. Certificate ${cert.CertificateId}`);
        console.log(`   Product: ${cert.ProductCode}, Plan: ${cert.PlanCode}, Year: ${cert.EffectiveYear}`);
        console.log(`   Matches: ${cert.MatchCount} (${cert.MatchedProposalIds || 'None'})`);
        console.log(`   Status: ${cert.ConformanceStatus}`);
        console.log('');
      });
    } else {
      console.log('\n✅ All certificates are conformant!\n');
    }
  } catch (e) {
    // Temp table might be gone, that's okay
  }
}

async function main() {
  const groupId = process.argv[2] || 'G25992';
  
  if (!groupId.startsWith('G')) {
    console.log('⚠️  Group ID should start with "G", adding prefix...');
  }
  const fullGroupId = groupId.startsWith('G') ? groupId : `G${groupId}`;

  console.log('============================================================');
  console.log('GROUP CONFORMANCE CHECKER');
  console.log('============================================================');

  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  try {
    const pocEtlSchema = config.database.pocMode ? 'poc_etl' : (config.database.schemas.processing || 'etl');
    await checkGroupConformance(pool, pocEtlSchema, fullGroupId);
  } catch (error: any) {
    console.error('\n❌ Error:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
