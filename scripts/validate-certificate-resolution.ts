/**
 * Certificate Resolution Validation
 * 
 * Validates that the TypeScript proposal builder correctly resolves certificates
 * by comparing against source data (NOT SQL output - which may be incorrect).
 * 
 * Validation Strategy:
 * 1. Random sample certificates (stratified by scenario type)
 * 2. Trace certificate → proposal → splits → hierarchies
 * 3. Validate against source data in input_certificate_info
 * 4. Check entity completeness and integrity
 * 5. Validate ConfigHash uniqueness and FK relationships
 * 
 * Sample Sizes:
 * - Small (10-20): Quick validation, diverse scenarios
 * - Medium (100-200): Edge cases, deeper coverage
 * - Large (1000+): Statistical confidence
 * 
 * Usage:
 *   npx tsx scripts/validate-certificate-resolution.ts [--sample small|medium|large]
 */

import * as sql from 'mssql';
import * as crypto from 'crypto';
import { loadConfig, getSqlConfig } from './lib/config-loader';

// =============================================================================
// Types
// =============================================================================

interface CertificateRecord {
  certificateId: string;
  groupId: string;
  groupName: string | null;
  certEffectiveDate: Date;
  productCode: string;
  planCode: string | null;
  situsState: string | null;
  certSplitSeq: number;
  certSplitPercent: number;
  splitBrokerSeq: number;
  splitBrokerId: string;
  splitBrokerName: string | null;
  commissionSchedule: string | null;
}

interface ValidationResult {
  certificateId: string;
  passed: boolean;
  checks: {
    proposalFound: boolean;
    proposalCorrect: boolean;
    splitConfigCorrect: boolean;
    hierarchyFound: boolean;
    hierarchyCorrect: boolean;
    foreignKeysIntact: boolean;
    configHashValid: boolean;
  };
  errors: string[];
  warnings: string[];
  details: {
    expectedProposalKey?: string;
    actualProposalId?: string;
    expectedSplits?: number;
    actualSplits?: number;
    expectedHierarchies?: number;
    actualHierarchies?: number;
  };
}

interface ValidationSummary {
  totalCertificates: number;
  passed: number;
  failed: number;
  passRate: number;
  checkResults: {
    proposalFound: number;
    proposalCorrect: number;
    splitConfigCorrect: number;
    hierarchyFound: number;
    hierarchyCorrect: number;
    foreignKeysIntact: number;
    configHashValid: number;
  };
  scenarios: {
    [key: string]: {
      count: number;
      passed: number;
      failed: number;
    };
  };
}

interface SampleConfig {
  size: 'small' | 'medium' | 'large';
  count: number;
  stratified: boolean;
}

// =============================================================================
// Configuration
// =============================================================================

const SAMPLE_CONFIGS: { [key: string]: SampleConfig } = {
  small: { size: 'small', count: 20, stratified: true },
  medium: { size: 'medium', count: 200, stratified: true },
  large: { size: 'large', count: 1000, stratified: false }
};

// =============================================================================
// Sample Certificate Selection
// =============================================================================

async function selectSampleCertificates(
  pool: sql.ConnectionPool,
  config: SampleConfig,
  schema: string = 'etl'
): Promise<CertificateRecord[]> {
  console.log(`\n📋 Selecting ${config.count} sample certificates (${config.size})...`);

  if (config.stratified) {
    // Stratified sampling: ensure diverse scenarios
    const scenarios = [
      { name: 'Single Split', condition: 'CertSplitSeq = 1 AND CertSplitPercent = 100' },
      { name: 'Multi Split', condition: 'CertSplitSeq > 1' },
      { name: 'Complex Hierarchy', condition: 'SplitBrokerSeq > 2' },
      { name: 'DTC (No Group)', condition: '(GroupId IS NULL OR LTRIM(RTRIM(GroupId)) = \'\')' },
      { name: 'Standard', condition: '1=1' }
    ];

    const samplesPerScenario = Math.ceil(config.count / scenarios.length);
    let allSamples: CertificateRecord[] = [];

    for (const scenario of scenarios) {
      const result = await pool.request().query(`
        SELECT TOP ${samplesPerScenario}
          CertificateId AS certificateId,
          LTRIM(RTRIM(ISNULL(GroupId, ''))) AS groupId,
          NULL AS groupName,
          TRY_CAST(CertEffectiveDate AS DATE) AS certEffectiveDate,
          LTRIM(RTRIM(Product)) AS productCode,
          LTRIM(RTRIM(ISNULL(PlanCode, ''))) AS planCode,
          CertIssuedState AS situsState,
          CertSplitSeq AS certSplitSeq,
          TRY_CAST(CertSplitPercent AS DECIMAL(18,4)) AS certSplitPercent,
          SplitBrokerSeq AS splitBrokerSeq,
          SplitBrokerId AS splitBrokerId,
          NULL AS splitBrokerName,
          CommissionsSchedule AS commissionSchedule
        FROM [${schema}].[input_certificate_info]
        WHERE CertStatus = 'A'
          AND RecStatus = 'A'
          AND CertEffectiveDate IS NOT NULL
          AND (${scenario.condition})
        ORDER BY NEWID()
      `);

      console.log(`  ${scenario.name}: ${result.recordset.length} samples`);
      allSamples = allSamples.concat(result.recordset);
    }

    // Deduplicate by certificateId
    const uniqueSamples = Array.from(
      new Map(allSamples.map(s => [s.certificateId, s])).values()
    ).slice(0, config.count);

    console.log(`  ✓ Selected ${uniqueSamples.length} unique certificates`);
    return uniqueSamples;
  } else {
    // Random sampling
    const result = await pool.request().query(`
      SELECT TOP ${config.count}
        CertificateId AS certificateId,
        LTRIM(RTRIM(ISNULL(GroupId, ''))) AS groupId,
        NULL AS groupName,
        TRY_CAST(CertEffectiveDate AS DATE) AS certEffectiveDate,
        LTRIM(RTRIM(Product)) AS productCode,
        LTRIM(RTRIM(ISNULL(PlanCode, ''))) AS planCode,
        CertIssuedState AS situsState,
        CertSplitSeq AS certSplitSeq,
        TRY_CAST(CertSplitPercent AS DECIMAL(18,4)) AS certSplitPercent,
        SplitBrokerSeq AS splitBrokerSeq,
        SplitBrokerId AS splitBrokerId,
        NULL AS splitBrokerName,
        CommissionsSchedule AS commissionSchedule
      FROM [${schema}].[input_certificate_info]
      WHERE CertStatus = 'A'
        AND RecStatus = 'A'
        AND CertEffectiveDate IS NOT NULL
      ORDER BY NEWID()
    `);

    console.log(`  ✓ Selected ${result.recordset.length} random certificates`);
    return result.recordset;
  }
}

// =============================================================================
// Certificate Validation
// =============================================================================

async function validateCertificate(
  pool: sql.ConnectionPool,
  cert: CertificateRecord,
  schema: string = 'etl'
): Promise<ValidationResult> {
  const result: ValidationResult = {
    certificateId: cert.certificateId,
    passed: false,
    checks: {
      proposalFound: false,
      proposalCorrect: false,
      splitConfigCorrect: false,
      hierarchyFound: false,
      hierarchyCorrect: false,
      foreignKeysIntact: false,
      configHashValid: false
    },
    errors: [],
    warnings: [],
    details: {}
  };

  try {
    // Step 1: Get all records for this certificate to build expected config
    const certRecords = await pool.request()
      .input('certId', sql.NVarChar(100), cert.certificateId)
      .query(`
        SELECT 
          CertSplitSeq, CertSplitPercent, SplitBrokerSeq,
          SplitBrokerId, CommissionsSchedule
        FROM [${schema}].[input_certificate_info]
        WHERE CertificateId = @certId
        ORDER BY CertSplitSeq, SplitBrokerSeq
      `);

    // Build expected split configuration
    const splits = new Map<number, any[]>();
    for (const rec of certRecords.recordset) {
      if (!splits.has(rec.CertSplitSeq)) {
        splits.set(rec.CertSplitSeq, []);
      }
      splits.get(rec.CertSplitSeq)!.push(rec);
    }

    const expectedSplitCount = splits.size;
    result.details.expectedSplits = expectedSplitCount;

    // Compute expected config hash
    const splitConfig = Array.from(splits.entries()).map(([seq, brokers]) => {
      const tiers = brokers.map(b => ({
        level: b.SplitBrokerSeq,
        brokerId: b.SplitBrokerId,
        schedule: b.CommissionsSchedule
      }));
      const hierarchyJson = JSON.stringify(tiers);
      const hierarchyHash = crypto.createHash('sha256').update(hierarchyJson).digest('hex').toUpperCase();
      
      return {
        seq,
        pct: brokers[0].CertSplitPercent,
        hierarchyHash
      };
    });
    const configJson = JSON.stringify(splitConfig);
    const expectedConfigHash = crypto.createHash('sha256').update(configJson).digest('hex').toUpperCase();

    // Check if group is invalid (should route to PHA)
    const isInvalidGroup = !cert.groupId || cert.groupId.trim() === '' || /^0+$/.test(cert.groupId.trim());

    if (isInvalidGroup) {
      // Should be in PHA, not proposals
      const phaCheck = await pool.request()
        .input('certId', sql.NVarChar(100), cert.certificateId)
        .query(`
          SELECT COUNT(*) as count
          FROM [${schema}].[stg_policy_hierarchy_assignments]
          WHERE PolicyId = @certId
        `);

      if (phaCheck.recordset[0].count > 0) {
        result.checks.proposalFound = true; // Found in PHA (correct)
        result.checks.proposalCorrect = true;
        result.warnings.push('Certificate has invalid GroupId - correctly routed to PHA');
      } else {
        result.errors.push('Certificate has invalid GroupId but not found in PHA');
      }
    } else {
      // Step 2: Find proposal via key mapping
      const proposalLookup = await pool.request()
        .input('groupId', sql.NVarChar(100), cert.groupId)
        .input('year', sql.Int, cert.certEffectiveDate.getFullYear())
        .input('productCode', sql.NVarChar(100), cert.productCode)
        .input('planCode', sql.NVarChar(100), cert.planCode || '')
        .query(`
          SELECT ProposalId, SplitConfigHash
          FROM [${schema}].[stg_proposal_key_mapping]
          WHERE GroupId = @groupId
            AND EffectiveYear = @year
            AND ProductCode = @productCode
            AND PlanCode = @planCode
        `);

      if (proposalLookup.recordset.length === 0) {
        result.errors.push('No proposal found for certificate via key mapping');
        return result;
      }

      result.checks.proposalFound = true;
      const proposalId = proposalLookup.recordset[0].ProposalId;
      const actualConfigHash = proposalLookup.recordset[0].SplitConfigHash;
      result.details.actualProposalId = proposalId;

      // Step 3: Validate ConfigHash matches expected
      if (actualConfigHash === expectedConfigHash) {
        result.checks.configHashValid = true;
      } else {
        result.errors.push(`ConfigHash mismatch: expected ${expectedConfigHash.substring(0, 16)}... vs actual ${actualConfigHash.substring(0, 16)}...`);
      }

      // Step 4: Validate split configuration
      const splitVersion = await pool.request()
        .input('proposalId', sql.NVarChar(100), proposalId)
        .query(`
          SELECT Id, TotalSplitPercent
          FROM [${schema}].[stg_premium_split_versions]
          WHERE ProposalId = @proposalId
        `);

      if (splitVersion.recordset.length === 0) {
        result.errors.push('No split version found for proposal');
        return result;
      }

      const versionId = splitVersion.recordset[0].Id;

      const splitParticipants = await pool.request()
        .input('versionId', sql.NVarChar(100), versionId)
        .query(`
          SELECT COUNT(*) as count, SUM(SplitPercent) as totalPct
          FROM [${schema}].[stg_premium_split_participants]
          WHERE VersionId = @versionId
        `);

      const actualSplitCount = splitParticipants.recordset[0].count;
      const totalSplitPct = splitParticipants.recordset[0].totalPct;

      result.details.actualSplits = actualSplitCount;

      if (actualSplitCount === expectedSplitCount) {
        result.checks.splitConfigCorrect = true;
      } else {
        result.errors.push(`Split count mismatch: expected ${expectedSplitCount}, actual ${actualSplitCount}`);
      }

      // Step 5: Validate hierarchies
      const hierarchies = await pool.request()
        .input('versionId', sql.NVarChar(100), versionId)
        .query(`
          SELECT DISTINCT HierarchyId
          FROM [${schema}].[stg_premium_split_participants]
          WHERE VersionId = @versionId
        `);

      const actualHierarchyCount = hierarchies.recordset.length;
      result.details.actualHierarchies = actualHierarchyCount;
      result.details.expectedHierarchies = expectedSplitCount;

      if (actualHierarchyCount > 0) {
        result.checks.hierarchyFound = true;

        // Validate hierarchy participants for first hierarchy
        const hierarchyId = hierarchies.recordset[0].HierarchyId;
        const hierarchyData = await pool.request()
          .input('hierarchyId', sql.NVarChar(100), hierarchyId)
          .query(`
            SELECT hv.Id as VersionId
            FROM [${schema}].[stg_hierarchies] h
            JOIN [${schema}].[stg_hierarchy_versions] hv ON hv.HierarchyId = h.Id
            WHERE h.Id = @hierarchyId
          `);

        if (hierarchyData.recordset.length > 0) {
          const hvId = hierarchyData.recordset[0].VersionId;
          const participants = await pool.request()
            .input('hvId', sql.NVarChar(100), hvId)
            .query(`
              SELECT EntityId, Level, ScheduleCode
              FROM [${schema}].[stg_hierarchy_participants]
              WHERE HierarchyVersionId = @hvId
              ORDER BY Level
            `);

          // Compare with expected (first split's brokers)
          const firstSplit = splits.get(1) || [];
          if (participants.recordset.length === firstSplit.length) {
            result.checks.hierarchyCorrect = true;
            
            // Validate each tier
            for (let i = 0; i < participants.recordset.length; i++) {
              const actual = participants.recordset[i];
              const expected = firstSplit[i];
              
              if (actual.EntityId !== expected.SplitBrokerId) {
                result.warnings.push(`Hierarchy tier ${i + 1} broker mismatch: expected ${expected.SplitBrokerId}, actual ${actual.EntityId}`);
              }
            }
          } else {
            result.errors.push(`Hierarchy participant count mismatch: expected ${firstSplit.length}, actual ${participants.recordset.length}`);
          }
        }
      } else {
        result.errors.push('No hierarchies found for splits');
      }

      // Step 6: Check foreign key integrity
      const fkCheck = await pool.request()
        .input('proposalId', sql.NVarChar(100), proposalId)
        .query(`
          SELECT 
            (SELECT COUNT(*) FROM [${schema}].[stg_proposals] WHERE Id = @proposalId) as proposalExists,
            (SELECT COUNT(*) FROM [${schema}].[stg_premium_split_versions] WHERE ProposalId = @proposalId) as versionExists,
            (SELECT COUNT(*) FROM [${schema}].[stg_premium_split_participants] WHERE VersionId IN (
              SELECT Id FROM [${schema}].[stg_premium_split_versions] WHERE ProposalId = @proposalId
            )) as participantExists
        `);

      const fk = fkCheck.recordset[0];
      if (fk.proposalExists > 0 && fk.versionExists > 0 && fk.participantExists > 0) {
        result.checks.foreignKeysIntact = true;
        result.checks.proposalCorrect = true;
      } else {
        result.errors.push('Foreign key integrity check failed');
      }
    }

    // Determine overall pass/fail
    const allChecksPassed = Object.values(result.checks).every(c => c === true);
    result.passed = allChecksPassed && result.errors.length === 0;

  } catch (err: any) {
    result.errors.push(`Validation error: ${err.message}`);
    result.passed = false;
  }

  return result;
}

// =============================================================================
// Summary Statistics
// =============================================================================

function generateSummary(results: ValidationResult[]): ValidationSummary {
  const summary: ValidationSummary = {
    totalCertificates: results.length,
    passed: results.filter(r => r.passed).length,
    failed: results.filter(r => !r.passed).length,
    passRate: 0,
    checkResults: {
      proposalFound: 0,
      proposalCorrect: 0,
      splitConfigCorrect: 0,
      hierarchyFound: 0,
      hierarchyCorrect: 0,
      foreignKeysIntact: 0,
      configHashValid: 0
    },
    scenarios: {}
  };

  summary.passRate = (summary.passed / summary.totalCertificates) * 100;

  for (const result of results) {
    for (const [check, value] of Object.entries(result.checks)) {
      if (value) {
        summary.checkResults[check as keyof typeof summary.checkResults]++;
      }
    }
  }

  return summary;
}

// =============================================================================
// Reporting
// =============================================================================

function printResults(results: ValidationResult[], summary: ValidationSummary): void {
  console.log('\n' + '='.repeat(80));
  console.log('VALIDATION RESULTS');
  console.log('='.repeat(80));
  
  console.log(`\n📊 Overall Statistics:`);
  console.log(`   Total Certificates: ${summary.totalCertificates}`);
  console.log(`   Passed: ${summary.passed} (${summary.passRate.toFixed(1)}%)`);
  console.log(`   Failed: ${summary.failed} (${(100 - summary.passRate).toFixed(1)}%)`);
  
  console.log(`\n✅ Check Results:`);
  for (const [check, count] of Object.entries(summary.checkResults)) {
    const pct = (count / summary.totalCertificates) * 100;
    const icon = pct >= 95 ? '✅' : pct >= 80 ? '⚠️' : '❌';
    console.log(`   ${icon} ${check}: ${count}/${summary.totalCertificates} (${pct.toFixed(1)}%)`);
  }

  // Show failed certificates
  const failed = results.filter(r => !r.passed);
  if (failed.length > 0) {
    console.log(`\n❌ Failed Certificates (${failed.length}):`);
    for (const result of failed.slice(0, 10)) { // Show first 10
      console.log(`\n   Certificate: ${result.certificateId}`);
      for (const error of result.errors) {
        console.log(`      ❌ ${error}`);
      }
      for (const warning of result.warnings) {
        console.log(`      ⚠️  ${warning}`);
      }
    }
    if (failed.length > 10) {
      console.log(`   ... and ${failed.length - 10} more`);
    }
  }

  console.log('\n' + '='.repeat(80));
  
  if (summary.passRate >= 95) {
    console.log('🎉 VALIDATION PASSED - Pass rate >= 95%');
  } else if (summary.passRate >= 80) {
    console.log('⚠️  VALIDATION WARNING - Pass rate 80-95%, review failures');
  } else {
    console.log('❌ VALIDATION FAILED - Pass rate < 80%, significant issues detected');
  }
  console.log('='.repeat(80) + '\n');
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main() {
  const args = process.argv.slice(2);
  const sampleType = args.includes('--sample') 
    ? args[args.indexOf('--sample') + 1] 
    : 'small';

  const sampleConfig = SAMPLE_CONFIGS[sampleType] || SAMPLE_CONFIGS.small;

  console.log('='.repeat(80));
  console.log('CERTIFICATE RESOLUTION VALIDATION');
  console.log('='.repeat(80));
  console.log(`Sample Size: ${sampleConfig.size} (${sampleConfig.count} certificates)`);
  console.log(`Stratified Sampling: ${sampleConfig.stratified ? 'Yes' : 'No'}`);
  console.log('='.repeat(80));

  // Load configuration
  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);

  const pool = await sql.connect(sqlConfig);

  try {
    const schema = config.database.targetSchema || 'etl';

    // Select sample certificates
    const samples = await selectSampleCertificates(pool, sampleConfig, schema);

    // Validate each certificate
    console.log(`\n🔍 Validating ${samples.length} certificates...`);
    const results: ValidationResult[] = [];

    for (let i = 0; i < samples.length; i++) {
      const cert = samples[i];
      if (i % 10 === 0) {
        process.stdout.write(`\r   Progress: ${i}/${samples.length} (${((i/samples.length)*100).toFixed(0)}%)`);
      }
      const result = await validateCertificate(pool, cert, schema);
      results.push(result);
    }
    process.stdout.write(`\r   Progress: ${samples.length}/${samples.length} (100%)\n`);

    // Generate summary
    const summary = generateSummary(results);

    // Print results
    printResults(results, summary);

    // Exit code
    process.exit(summary.passRate >= 95 ? 0 : 1);

  } finally {
    await pool.close();
  }
}

// Run
if (require.main === module) {
  main().catch(err => {
    console.error('❌ Validation failed:', err);
    process.exit(1);
  });
}
