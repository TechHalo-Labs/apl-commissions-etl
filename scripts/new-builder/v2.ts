/**
 * APL Commission System - Proposal Builder v2 (Entropy + Hard Rules)
 *
 * Parallel entry point that reuses v1 staging generation and writer,
 * but adds entropy-based routing to PHA for high-variability groups.
 *
 * Constants are sourced from appsettings.json (ProposalBuilderV2 section).
 */

import * as fs from 'fs';
import * as path from 'path';
import * as sql from 'mssql';
import {
  ProposalBuilder,
  EntropyOptions,
  BuilderOptions,
  ExecutionMode,
  loadCertificatesFromDatabase,
  writeStagingOutput,
  exportStagingToProduction
} from '../proposal-builder';

interface AppSettings {
  ProposalBuilderV2?: {
    highEntropyUniqueRatio?: number;
    highEntropyShannon?: number;
    dominantCoverageThreshold?: number;
    phaClusterSizeThreshold?: number;
    logEntropyByGroup?: boolean;
  };
  BlobBulk?: {
    containerUrl?: string;
    endpoint?: string;
    container?: string;
    token?: string;
    bulkPrefix?: string;
  };
}

interface ValidationResult {
  groupId: string;
  nonPhaRows: number;
  unmatchedRows: number;
  overlappingRows: number;
  // Chain validation (only populated with --deep flag)
  proposalsWithoutSplitVersion?: number;
  splitVersionsWithoutParticipants?: number;
  participantsWithoutHierarchy?: number;
  hierarchiesWithoutParticipants?: number;
  hierarchyParticipantsWithoutSchedule?: number;
  // Content validation (only populated with --deep flag)
  missingBrokers?: number;
  missingSchedules?: number;
  certsWithMissingBroker?: number;
  certsWithMissingSchedule?: number;
}

interface OverlappingCert {
  certificateId: string;
  groupId: string;
  product: string;
  planCode: string;
  certEffectiveDate: Date;
  matchingProposalIds: string[];
}

function parseAuditLogForIssues(auditPath: string): { overlapping: string[], unmatched: string[] } {
  const content = fs.readFileSync(auditPath, 'utf8');
  const lines = content.split('\n');
  
  const overlapping: string[] = [];
  const unmatched: string[] = [];
  
  for (const line of lines) {
    // Match lines like: "Validating G0033... ❌ non-PHA=47, OVERLAPPING=12"
    const overlapMatch = line.match(/Validating (G\d+)\.\.\. ❌.*OVERLAPPING=(\d+)/);
    if (overlapMatch) {
      overlapping.push(overlapMatch[1]);
      continue;
    }
    
    // Match lines like: "Validating G15793... ❌ non-PHA=39, unmatched=3"
    const unmatchedMatch = line.match(/Validating (G\d+)\.\.\. ❌.*unmatched=(\d+)/);
    if (unmatchedMatch) {
      unmatched.push(unmatchedMatch[1]);
    }
  }
  
  return { overlapping, unmatched };
}

function loadEntropyOptions(configPath?: string): EntropyOptions {
  const appSettingsPath = configPath || path.join(process.cwd(), 'appsettings.json');
  if (!fs.existsSync(appSettingsPath)) {
    throw new Error(`appsettings.json not found at ${appSettingsPath}`);
  }

  const settings = JSON.parse(fs.readFileSync(appSettingsPath, 'utf8')) as AppSettings;
  const cfg = settings.ProposalBuilderV2;
  if (!cfg) {
    throw new Error('ProposalBuilderV2 section missing in appsettings.json');
  }

  const required = [
    'highEntropyUniqueRatio',
    'highEntropyShannon',
    'dominantCoverageThreshold',
    'phaClusterSizeThreshold'
  ] as const;

  for (const key of required) {
    if (cfg[key] === undefined || cfg[key] === null) {
      throw new Error(`ProposalBuilderV2.${key} is required in appsettings.json`);
    }
  }

  return {
    highEntropyUniqueRatio: cfg.highEntropyUniqueRatio!,
    highEntropyShannon: cfg.highEntropyShannon!,
    dominantCoverageThreshold: cfg.dominantCoverageThreshold!,
    phaClusterSizeThreshold: cfg.phaClusterSizeThreshold!,
    logEntropyByGroup: cfg.logEntropyByGroup ?? false
  };
}

function loadBlobBulkConfig(configPath?: string): AppSettings['BlobBulk'] {
  const appSettingsPath = configPath || path.join(process.cwd(), 'appsettings.json');
  if (!fs.existsSync(appSettingsPath)) {
    return undefined;
  }
  const settings = JSON.parse(fs.readFileSync(appSettingsPath, 'utf8')) as AppSettings;
  return settings.BlobBulk;
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

  const server = parts['server'] || parts['data source'] || '';
  const database = parts['database'] || parts['initial catalog'] || '';
  if (!server || !database) {
    throw new Error('Invalid SQLSERVER connection string: server and database are required');
  }

  return {
    server,
    database,
    user: parts['user id'] || parts['uid'] || '',
    password: parts['password'] || parts['pwd'] || '',
    options: {
      encrypt: true,
      trustServerCertificate: true
    }
  };
}

async function loadDistinctGroups(config: DatabaseConfig, options: BuilderOptions): Promise<string[]> {
  const schema = options.schema || 'etl';
  const pool = await sql.connect(config);
  try {
    // Only load groups that have commission detail records (actual transactions)
    const result = await pool.request().query(`
      SELECT DISTINCT LTRIM(RTRIM(ci.GroupId)) AS GroupId
      FROM [${schema}].[raw_certificate_info] ci
      INNER JOIN [${schema}].[raw_commissions_detail] cd 
        ON LTRIM(RTRIM(ci.CertificateId)) = LTRIM(RTRIM(cd.CertificateId))
      WHERE LTRIM(RTRIM(ci.CertStatus)) = 'A'
        AND LTRIM(RTRIM(ci.RecStatus)) = 'A'
        AND ci.CertEffectiveDate IS NOT NULL
      ORDER BY LTRIM(RTRIM(ci.GroupId))
    `);
    return result.recordset.map(r => r.GroupId).filter((g: string) => g && g.trim() !== '');
  } finally {
    await pool.close();
  }
}

async function validateGroups(config: DatabaseConfig, groups: string[], deepValidation: boolean = false): Promise<ValidationResult[]> {
  if (groups.length === 0) return [];
  const pool = await sql.connect({
    ...config,
    requestTimeout: 300000
  });
  const total = groups.length;
  try {
    const results: ValidationResult[] = [];
    for (let idx = 0; idx < groups.length; idx++) {
      const rawGroupId = groups[idx];
      const trimmed = rawGroupId.trim();
      const groupIdNumeric = trimmed.replace(/^[A-Za-z]+/, '');
      const groupIdWithPrefix = `G${groupIdNumeric}`;
      
      process.stdout.write(`  [${idx + 1}/${total}] Validating ${groupIdWithPrefix}... `);

      const counts = await pool.request().query(`
        WITH Raw AS (
          SELECT
            LTRIM(RTRIM(GroupId)) AS GroupId,
            LTRIM(RTRIM(Product)) AS Product,
            LTRIM(RTRIM(PlanCode)) AS PlanCode,
            TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(CertEffectiveDate)), '')) AS CertEffectiveDate,
            LTRIM(RTRIM(CertificateId)) AS CertificateId
          FROM [etl].[raw_certificate_info]
          WHERE LTRIM(RTRIM(GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
            AND LTRIM(RTRIM(CertStatus)) = 'A'
            AND LTRIM(RTRIM(RecStatus)) = 'A'
        ),
        PhaCerts AS (
          SELECT DISTINCT pha.PolicyId AS CertificateId
          FROM [etl].[stg_policy_hierarchy_assignments] pha
          INNER JOIN [etl].[stg_hierarchies] h ON h.Id = pha.HierarchyId
          WHERE h.GroupId = '${groupIdWithPrefix}'
        ),
        NonPha AS (
          SELECT r.*
          FROM Raw r
          LEFT JOIN PhaCerts p ON p.CertificateId = r.CertificateId
          WHERE p.CertificateId IS NULL
        ),
        Matches AS (
          SELECT np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate, p.Id AS ProposalId
          FROM NonPha np
          INNER JOIN [etl].[stg_proposals] p
            ON p.GroupId = '${groupIdWithPrefix}'
           AND np.CertEffectiveDate > p.EffectiveDateFrom
           AND np.CertEffectiveDate <= p.EffectiveDateTo
           AND (
             p.ProductCodes IS NULL OR LTRIM(RTRIM(p.ProductCodes)) = '' OR LTRIM(RTRIM(p.ProductCodes)) = '*'
             OR EXISTS (
               SELECT 1 FROM STRING_SPLIT(p.ProductCodes, ',') s
               WHERE LTRIM(RTRIM(s.value)) = np.Product
             )
           )
           AND (
             p.PlanCodes IS NULL OR LTRIM(RTRIM(p.PlanCodes)) = '' OR LTRIM(RTRIM(p.PlanCodes)) = '*'
             OR EXISTS (
               SELECT 1 FROM STRING_SPLIT(p.PlanCodes, ',') s
               WHERE LTRIM(RTRIM(s.value)) = np.PlanCode
             )
           )
        )
        SELECT
          (SELECT COUNT(*) FROM NonPha) AS NonPhaRows,
          (SELECT COUNT(*) FROM NonPha np
            LEFT JOIN Matches m ON m.CertificateId = np.CertificateId
             AND m.Product = np.Product
             AND m.PlanCode = np.PlanCode
             AND m.CertEffectiveDate = np.CertEffectiveDate
           WHERE m.ProposalId IS NULL) AS UnmatchedRows,
          (SELECT COUNT(*) FROM (
            SELECT np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate
            FROM NonPha np
            INNER JOIN Matches m ON m.CertificateId = np.CertificateId
             AND m.Product = np.Product
             AND m.PlanCode = np.PlanCode
             AND m.CertEffectiveDate = np.CertEffectiveDate
            GROUP BY np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate
            HAVING COUNT(DISTINCT m.ProposalId) > 1
          ) overlaps) AS OverlappingRows;
      `);

      const nonPhaRows = counts.recordset[0]?.NonPhaRows || 0;
      const unmatchedRows = counts.recordset[0]?.UnmatchedRows || 0;
      const overlappingRows = counts.recordset[0]?.OverlappingRows || 0;
      
      results.push({
        groupId: groupIdWithPrefix,
        nonPhaRows,
        unmatchedRows,
        overlappingRows
      });

      // Log result immediately for this group
      if (unmatchedRows > 0 || overlappingRows > 0) {
        const issues = [];
        if (unmatchedRows > 0) issues.push(`unmatched=${unmatchedRows}`);
        if (overlappingRows > 0) issues.push(`OVERLAPPING=${overlappingRows}`);
        console.log(`❌ non-PHA=${nonPhaRows}, ${issues.join(', ')}`);
      } else if (nonPhaRows > 0) {
        console.log(`✓ non-PHA=${nonPhaRows}, all matched (no overlaps)`);
      } else {
        console.log(`✓ all routed to PHA`);
      }

      if (unmatchedRows > 0) {
        const sample = await pool.request().query(`
          WITH Raw AS (
            SELECT
              LTRIM(RTRIM(GroupId)) AS GroupId,
              LTRIM(RTRIM(Product)) AS Product,
              LTRIM(RTRIM(PlanCode)) AS PlanCode,
              TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(CertEffectiveDate)), '')) AS CertEffectiveDate,
              LTRIM(RTRIM(CertificateId)) AS CertificateId
            FROM [etl].[raw_certificate_info]
            WHERE LTRIM(RTRIM(GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
              AND LTRIM(RTRIM(CertStatus)) = 'A'
              AND LTRIM(RTRIM(RecStatus)) = 'A'
          ),
          PhaCerts AS (
            SELECT DISTINCT pha.PolicyId AS CertificateId
            FROM [etl].[stg_policy_hierarchy_assignments] pha
            INNER JOIN [etl].[stg_hierarchies] h ON h.Id = pha.HierarchyId
            WHERE h.GroupId = '${groupIdWithPrefix}'
          ),
          NonPha AS (
            SELECT r.*
            FROM Raw r
            LEFT JOIN PhaCerts p ON p.CertificateId = r.CertificateId
            WHERE p.CertificateId IS NULL
          ),
          Matches AS (
            SELECT np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate, p.Id AS ProposalId
            FROM NonPha np
            INNER JOIN [etl].[stg_proposals] p
              ON p.GroupId = '${groupIdWithPrefix}'
             AND np.CertEffectiveDate > p.EffectiveDateFrom
             AND np.CertEffectiveDate <= p.EffectiveDateTo
             AND (
               p.ProductCodes IS NULL OR LTRIM(RTRIM(p.ProductCodes)) = '' OR LTRIM(RTRIM(p.ProductCodes)) = '*'
               OR EXISTS (
                 SELECT 1 FROM STRING_SPLIT(p.ProductCodes, ',') s
                 WHERE LTRIM(RTRIM(s.value)) = np.Product
               )
             )
             AND (
               p.PlanCodes IS NULL OR LTRIM(RTRIM(p.PlanCodes)) = '' OR LTRIM(RTRIM(p.PlanCodes)) = '*'
               OR EXISTS (
                 SELECT 1 FROM STRING_SPLIT(p.PlanCodes, ',') s
                 WHERE LTRIM(RTRIM(s.value)) = np.PlanCode
               )
             )
          )
          SELECT TOP 5 np.*
          FROM NonPha np
          LEFT JOIN Matches m ON m.CertificateId = np.CertificateId
           AND m.Product = np.Product
           AND m.PlanCode = np.PlanCode
           AND m.CertEffectiveDate = np.CertEffectiveDate
          WHERE m.ProposalId IS NULL
          ORDER BY np.CertEffectiveDate, np.Product, np.PlanCode;
        `);

        console.log(`  ⚠️ Validation samples for ${groupIdWithPrefix}:`);
        for (const row of sample.recordset) {
          console.log(`    - Cert ${row.CertificateId} ${row.Product} ${row.PlanCode} ${row.CertEffectiveDate}`);
        }
      }

      // Deep chain validation if requested
      if (deepValidation && nonPhaRows > 0) {
        const chainCheck = await pool.request().query(`
          -- Check proposal chain integrity for group ${groupIdWithPrefix}
          WITH ProposalsForGroup AS (
            SELECT Id FROM [etl].[stg_proposals] WHERE GroupId = '${groupIdWithPrefix}'
          ),
          -- 1. Proposals without PremiumSplitVersions
          ProposalsWithoutPSV AS (
            SELECT p.Id
            FROM ProposalsForGroup p
            LEFT JOIN [etl].[stg_premium_split_versions] psv ON psv.ProposalId = p.Id
            WHERE psv.Id IS NULL
          ),
          -- 2. PremiumSplitVersions without Participants
          PSVWithoutParticipants AS (
            SELECT psv.Id
            FROM [etl].[stg_premium_split_versions] psv
            WHERE psv.GroupId = '${groupIdWithPrefix}'
              AND NOT EXISTS (
                SELECT 1 FROM [etl].[stg_premium_split_participants] psp WHERE psp.VersionId = psv.Id
              )
          ),
          -- 3. PremiumSplitParticipants without valid Hierarchy link
          PSPWithoutHierarchy AS (
            SELECT psp.Id
            FROM [etl].[stg_premium_split_participants] psp
            WHERE psp.GroupId = '${groupIdWithPrefix}'
              AND (psp.HierarchyId IS NULL OR NOT EXISTS (
                SELECT 1 FROM [etl].[stg_hierarchies] h WHERE h.Id = psp.HierarchyId
              ))
          ),
          -- 4. Hierarchies without HierarchyParticipants (via versions)
          HierarchiesWithoutParticipants AS (
            SELECT h.Id
            FROM [etl].[stg_hierarchies] h
            WHERE h.GroupId = '${groupIdWithPrefix}'
              AND NOT EXISTS (
                SELECT 1 FROM [etl].[stg_hierarchy_versions] hv
                INNER JOIN [etl].[stg_hierarchy_participants] hp ON hp.HierarchyVersionId = hv.Id
                WHERE hv.HierarchyId = h.Id
              )
          ),
          -- 5. HierarchyParticipants without ScheduleId
          HPWithoutSchedule AS (
            SELECT hp.Id
            FROM [etl].[stg_hierarchy_participants] hp
            INNER JOIN [etl].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
            INNER JOIN [etl].[stg_hierarchies] h ON h.Id = hv.HierarchyId
            WHERE h.GroupId = '${groupIdWithPrefix}'
              AND (hp.ScheduleId IS NULL OR hp.ScheduleId = '')
          )
          SELECT
            (SELECT COUNT(*) FROM ProposalsWithoutPSV) AS ProposalsWithoutPSV,
            (SELECT COUNT(*) FROM PSVWithoutParticipants) AS PSVWithoutParticipants,
            (SELECT COUNT(*) FROM PSPWithoutHierarchy) AS PSPWithoutHierarchy,
            (SELECT COUNT(*) FROM HierarchiesWithoutParticipants) AS HierarchiesWithoutParticipants,
            (SELECT COUNT(*) FROM HPWithoutSchedule) AS HPWithoutSchedule
        `);

        const chain = chainCheck.recordset[0];
        results[results.length - 1].proposalsWithoutSplitVersion = chain.ProposalsWithoutPSV || 0;
        results[results.length - 1].splitVersionsWithoutParticipants = chain.PSVWithoutParticipants || 0;
        results[results.length - 1].participantsWithoutHierarchy = chain.PSPWithoutHierarchy || 0;
        results[results.length - 1].hierarchiesWithoutParticipants = chain.HierarchiesWithoutParticipants || 0;
        results[results.length - 1].hierarchyParticipantsWithoutSchedule = chain.HPWithoutSchedule || 0;

        const chainIssues: string[] = [];
        if (chain.ProposalsWithoutPSV > 0) chainIssues.push(`proposals-no-PSV=${chain.ProposalsWithoutPSV}`);
        if (chain.PSVWithoutParticipants > 0) chainIssues.push(`PSV-no-participants=${chain.PSVWithoutParticipants}`);
        if (chain.PSPWithoutHierarchy > 0) chainIssues.push(`PSP-no-hierarchy=${chain.PSPWithoutHierarchy}`);
        if (chain.HierarchiesWithoutParticipants > 0) chainIssues.push(`hierarchies-no-participants=${chain.HierarchiesWithoutParticipants}`);
        if (chain.HPWithoutSchedule > 0) chainIssues.push(`HP-no-schedule=${chain.HPWithoutSchedule}`);

        if (chainIssues.length > 0) {
          console.log(`  ⚠️ Chain issues: ${chainIssues.join(', ')}`);
        } else {
          console.log(`  ✓ Chain validation passed`);
        }

        // Deep content validation: check brokers and schedules match source data
        const contentCheck = await pool.request().query(`
          -- Check that source brokers and schedules are present in generated hierarchies
          WITH SourceData AS (
            SELECT DISTINCT
              LTRIM(RTRIM(r.CertificateId)) AS CertificateId,
              LTRIM(RTRIM(r.SplitBrokerId)) AS SplitBrokerId,
              LTRIM(RTRIM(r.CommissionsSchedule)) AS ScheduleCode
            FROM [etl].[raw_certificate_info] r
            WHERE LTRIM(RTRIM(r.GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
              AND LTRIM(RTRIM(r.CertStatus)) = 'A'
              AND LTRIM(RTRIM(r.RecStatus)) = 'A'
              AND LTRIM(RTRIM(r.SplitBrokerId)) IS NOT NULL
              AND LTRIM(RTRIM(r.SplitBrokerId)) <> ''
          ),
          -- Get PHA certificates (they have different hierarchy structure)
          PhaCerts AS (
            SELECT DISTINCT pha.PolicyId AS CertificateId
            FROM [etl].[stg_policy_hierarchy_assignments] pha
            INNER JOIN [etl].[stg_hierarchies] h ON h.Id = pha.HierarchyId
            WHERE h.GroupId = '${groupIdWithPrefix}'
          ),
          -- Non-PHA source data only
          NonPhaSource AS (
            SELECT s.*
            FROM SourceData s
            LEFT JOIN PhaCerts p ON p.CertificateId = s.CertificateId
            WHERE p.CertificateId IS NULL
          ),
          -- Get all brokers in hierarchies for this group (via proposals)
          HierarchyBrokers AS (
            SELECT DISTINCT hp.EntityId AS BrokerId
            FROM [etl].[stg_hierarchy_participants] hp
            INNER JOIN [etl].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
            INNER JOIN [etl].[stg_hierarchies] h ON h.Id = hv.HierarchyId
            WHERE h.GroupId = '${groupIdWithPrefix}'
          ),
          -- Get all schedules in hierarchies for this group
          HierarchySchedules AS (
            SELECT DISTINCT hp.ScheduleCode
            FROM [etl].[stg_hierarchy_participants] hp
            INNER JOIN [etl].[stg_hierarchy_versions] hv ON hv.Id = hp.HierarchyVersionId
            INNER JOIN [etl].[stg_hierarchies] h ON h.Id = hv.HierarchyId
            WHERE h.GroupId = '${groupIdWithPrefix}'
              AND hp.ScheduleCode IS NOT NULL AND hp.ScheduleCode <> ''
          ),
          -- Source brokers not found in any hierarchy
          MissingBrokers AS (
            SELECT DISTINCT nps.SplitBrokerId
            FROM NonPhaSource nps
            WHERE nps.SplitBrokerId NOT IN (SELECT BrokerId FROM HierarchyBrokers)
              AND nps.SplitBrokerId NOT IN (SELECT 'B' + BrokerId FROM HierarchyBrokers)
              AND 'B' + nps.SplitBrokerId NOT IN (SELECT BrokerId FROM HierarchyBrokers)
          ),
          -- Source schedules not found in any hierarchy
          MissingSchedules AS (
            SELECT DISTINCT nps.ScheduleCode
            FROM NonPhaSource nps
            WHERE nps.ScheduleCode IS NOT NULL AND nps.ScheduleCode <> ''
              AND nps.ScheduleCode NOT IN (SELECT ScheduleCode FROM HierarchySchedules)
          ),
          -- Count certificates with missing brokers
          CertsWithMissingBroker AS (
            SELECT DISTINCT nps.CertificateId
            FROM NonPhaSource nps
            WHERE nps.SplitBrokerId IN (SELECT SplitBrokerId FROM MissingBrokers)
          ),
          -- Count certificates with missing schedules
          CertsWithMissingSchedule AS (
            SELECT DISTINCT nps.CertificateId
            FROM NonPhaSource nps
            WHERE nps.ScheduleCode IN (SELECT ScheduleCode FROM MissingSchedules)
          )
          SELECT
            (SELECT COUNT(*) FROM NonPhaSource) AS TotalSourceRows,
            (SELECT COUNT(*) FROM MissingBrokers) AS MissingBrokerCount,
            (SELECT COUNT(*) FROM MissingSchedules) AS MissingScheduleCount,
            (SELECT COUNT(*) FROM CertsWithMissingBroker) AS CertsWithMissingBroker,
            (SELECT COUNT(*) FROM CertsWithMissingSchedule) AS CertsWithMissingSchedule,
            (SELECT STRING_AGG(SplitBrokerId, ', ') FROM (SELECT TOP 5 SplitBrokerId FROM MissingBrokers) x) AS SampleMissingBrokers,
            (SELECT STRING_AGG(ScheduleCode, ', ') FROM (SELECT TOP 5 ScheduleCode FROM MissingSchedules) x) AS SampleMissingSchedules
        `);

        const content = contentCheck.recordset[0];
        results[results.length - 1].missingBrokers = content.MissingBrokerCount || 0;
        results[results.length - 1].missingSchedules = content.MissingScheduleCount || 0;
        results[results.length - 1].certsWithMissingBroker = content.CertsWithMissingBroker || 0;
        results[results.length - 1].certsWithMissingSchedule = content.CertsWithMissingSchedule || 0;

        const contentIssues: string[] = [];
        if (content.MissingBrokerCount > 0) {
          contentIssues.push(`missing-brokers=${content.MissingBrokerCount} (${content.CertsWithMissingBroker} certs)`);
        }
        if (content.MissingScheduleCount > 0) {
          contentIssues.push(`missing-schedules=${content.MissingScheduleCount} (${content.CertsWithMissingSchedule} certs)`);
        }

        if (contentIssues.length > 0) {
          console.log(`  ⚠️ Content issues: ${contentIssues.join(', ')}`);
          if (content.SampleMissingBrokers) {
            console.log(`    Missing brokers: ${content.SampleMissingBrokers}`);
          }
          if (content.SampleMissingSchedules) {
            console.log(`    Missing schedules: ${content.SampleMissingSchedules}`);
          }
        } else {
          console.log(`  ✓ Content validation passed (brokers & schedules match)`);
        }
      }
    }
    return results;
  } finally {
    await pool.close();
  }
}

async function loadMaxProposalProductId(config: DatabaseConfig, options: BuilderOptions): Promise<number> {
  const schema = options.schema || 'etl';
  const pool = await sql.connect(config);
  try {
    const result = await pool.request().query(`
      SELECT ISNULL(MAX(Id), 0) AS MaxId FROM [${schema}].[stg_proposal_products]
    `);
    return Number(result.recordset[0]?.MaxId ?? 0);
  } finally {
    await pool.close();
  }
}

async function fixOverlappingProposals(
  config: DatabaseConfig,
  options: BuilderOptions,
  groups: string[],
  dryRun: boolean
): Promise<{ groupId: string; fixed: number }[]> {
  const schema = options.schema || 'etl';
  const pool = await sql.connect({
    ...config,
    requestTimeout: 300000,
    connectionTimeout: 30000
  });
  
  const results: { groupId: string; fixed: number }[] = [];
  
  try {
    for (const rawGroupId of groups) {
      const trimmed = rawGroupId.trim();
      const groupIdNumeric = trimmed.replace(/^[A-Za-z]+/, '');
      const groupIdWithPrefix = `G${groupIdNumeric}`;
      
      process.stdout.write(`  Processing ${groupIdWithPrefix}... `);
      
      // Find overlapping certificates for this group
      const overlappingQuery = await pool.request().query(`
        WITH Raw AS (
          SELECT
            LTRIM(RTRIM(GroupId)) AS GroupId,
            LTRIM(RTRIM(Product)) AS Product,
            LTRIM(RTRIM(PlanCode)) AS PlanCode,
            TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(CertEffectiveDate)), '')) AS CertEffectiveDate,
            LTRIM(RTRIM(CertificateId)) AS CertificateId
          FROM [${schema}].[raw_certificate_info]
          WHERE LTRIM(RTRIM(GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
            AND LTRIM(RTRIM(CertStatus)) = 'A'
            AND LTRIM(RTRIM(RecStatus)) = 'A'
        ),
        PhaCerts AS (
          SELECT DISTINCT pha.PolicyId AS CertificateId
          FROM [${schema}].[stg_policy_hierarchy_assignments] pha
          INNER JOIN [${schema}].[stg_hierarchies] h ON h.Id = pha.HierarchyId
          WHERE h.GroupId = '${groupIdWithPrefix}'
        ),
        NonPha AS (
          SELECT r.*
          FROM Raw r
          LEFT JOIN PhaCerts p ON p.CertificateId = r.CertificateId
          WHERE p.CertificateId IS NULL
        ),
        Matches AS (
          SELECT np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate, p.Id AS ProposalId
          FROM NonPha np
          INNER JOIN [${schema}].[stg_proposals] p
            ON p.GroupId = '${groupIdWithPrefix}'
           AND np.CertEffectiveDate > p.EffectiveDateFrom
           AND np.CertEffectiveDate <= p.EffectiveDateTo
           AND (
             p.ProductCodes IS NULL OR LTRIM(RTRIM(p.ProductCodes)) = '' OR LTRIM(RTRIM(p.ProductCodes)) = '*'
             OR EXISTS (
               SELECT 1 FROM STRING_SPLIT(p.ProductCodes, ',') s
               WHERE LTRIM(RTRIM(s.value)) = np.Product
             )
           )
           AND (
             p.PlanCodes IS NULL OR LTRIM(RTRIM(p.PlanCodes)) = '' OR LTRIM(RTRIM(p.PlanCodes)) = '*'
             OR EXISTS (
               SELECT 1 FROM STRING_SPLIT(p.PlanCodes, ',') s
               WHERE LTRIM(RTRIM(s.value)) = np.PlanCode
             )
           )
        ),
        Overlapping AS (
          SELECT np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate,
                 STRING_AGG(m.ProposalId, ',') AS MatchingProposalIds,
                 COUNT(DISTINCT m.ProposalId) AS ProposalCount
          FROM NonPha np
          INNER JOIN Matches m ON m.CertificateId = np.CertificateId
           AND m.Product = np.Product
           AND m.PlanCode = np.PlanCode
           AND m.CertEffectiveDate = np.CertEffectiveDate
          GROUP BY np.CertificateId, np.Product, np.PlanCode, np.CertEffectiveDate
          HAVING COUNT(DISTINCT m.ProposalId) > 1
        )
        SELECT DISTINCT CertificateId, '${groupIdWithPrefix}' AS GroupId, Product, PlanCode, CertEffectiveDate, MatchingProposalIds
        FROM Overlapping
      `);
      
      const overlappingCerts = overlappingQuery.recordset;
      
      if (overlappingCerts.length === 0) {
        console.log(`✓ no overlaps found`);
        results.push({ groupId: groupIdWithPrefix, fixed: 0 });
        continue;
      }
      
      console.log(`found ${overlappingCerts.length} overlapping certs`);
      
      if (dryRun) {
        console.log(`    [DRY RUN] Would route ${overlappingCerts.length} certs to PHA`);
        results.push({ groupId: groupIdWithPrefix, fixed: overlappingCerts.length });
        continue;
      }
      
      // Route overlapping certificates to PHA
      // First, check if a PHA hierarchy already exists for this group
      const existingHierarchy = await pool.request().query(`
        SELECT TOP 1 Id, Name FROM [${schema}].[stg_hierarchies]
        WHERE GroupId = '${groupIdWithPrefix}' AND Name LIKE '%PHA%'
      `);
      
      let hierarchyId: string;
      let hierarchyVersionId: string;
      
      if (existingHierarchy.recordset.length > 0) {
        hierarchyId = existingHierarchy.recordset[0].Id;
        // Get existing version
        const existingVersion = await pool.request().query(`
          SELECT TOP 1 Id FROM [${schema}].[stg_hierarchy_versions]
          WHERE HierarchyId = '${hierarchyId}'
        `);
        hierarchyVersionId = existingVersion.recordset[0]?.Id || `${hierarchyId}-V1`;
      } else {
        // Create new PHA hierarchy
        hierarchyId = `${groupIdWithPrefix}-PHA-OVERLAP`;
        hierarchyVersionId = `${hierarchyId}-V1`;
        
        await pool.request().query(`
          INSERT INTO [${schema}].[stg_hierarchies] (Id, Name, GroupId, Status, CurrentVersionId, CreationTime, IsDeleted)
          VALUES ('${hierarchyId}', '${groupIdWithPrefix} Overlapping Proposals PHA', '${groupIdWithPrefix}', 1, '${hierarchyVersionId}', GETUTCDATE(), 0)
        `);
        
        await pool.request().query(`
          INSERT INTO [${schema}].[stg_hierarchy_versions] (Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo, CreationTime, IsDeleted)
          VALUES ('${hierarchyVersionId}', '${hierarchyId}', 1, 1, '1900-01-01', '2099-12-31', GETUTCDATE(), 0)
        `);
      }
      
      // Insert PHA records for each overlapping certificate
      let insertedCount = 0;
      for (const cert of overlappingCerts) {
        const phaId = `PHA-OVERLAP-${cert.CertificateId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        // Check if PHA already exists for this cert
        const existingPha = await pool.request().query(`
          SELECT 1 FROM [${schema}].[stg_policy_hierarchy_assignments]
          WHERE PolicyId = '${cert.CertificateId}' AND HierarchyId = '${hierarchyId}'
        `);
        
        if (existingPha.recordset.length === 0) {
          await pool.request().query(`
            INSERT INTO [${schema}].[stg_policy_hierarchy_assignments] (
              Id, PolicyId, HierarchyId, SplitSequence, SplitPercent,
              IsNonConforming, NonConformantReason, CreationTime, IsDeleted
            )
            VALUES (
              '${phaId}', '${cert.CertificateId}', '${hierarchyId}', 1, 100,
              1, 'Overlapping proposals: ${cert.MatchingProposalIds}', GETUTCDATE(), 0
            )
          `);
          insertedCount++;
        }
      }
      
      console.log(`    ✓ Routed ${insertedCount} certs to PHA`);
      results.push({ groupId: groupIdWithPrefix, fixed: insertedCount });
    }
    
    return results;
  } finally {
    await pool.close();
  }
}

async function processCertificates(
  builder: ProposalBuilder,
  config: DatabaseConfig,
  options: BuilderOptions,
  certificates: Awaited<ReturnType<typeof loadCertificatesFromDatabase>>,
  entropyOptions: EntropyOptions
): Promise<void> {
  builder.loadCertificates(certificates);
  builder.extractSelectionCriteria();

  const phaPool = await sql.connect(config);
  try {
    await builder.identifyNonConformantCases(phaPool);
  } finally {
    await phaPool.close();
  }

  builder.applyEntropyRouting(entropyOptions);
  
  // Segment by dominant trend - detect regime changes and route non-dominant to PHA
  builder.segmentByDominantTrend();
  
  builder.buildProposals();
  
  // Route small outlier proposals to PHA (< 5% of total certificates)
  builder.routeOutliersToPA(5);
}

async function runProposalBuilderV2(
  config: DatabaseConfig,
  options: BuilderOptions,
  validateGroupsArg: string[],
  validateAllFlag: boolean,
  configPath?: string
): Promise<void> {
  const entropyOptions = loadEntropyOptions(configPath);
  if (options.verbose) {
    entropyOptions.logEntropyByGroup = true;
    entropyOptions.verbose = true;
  }

  if (options.bulkMode === 'blob') {
    const blobConfig = loadBlobBulkConfig(configPath);
    if (blobConfig) {
      options.blobConfig = {
        containerUrl: blobConfig.containerUrl,
        endpoint: blobConfig.endpoint,
        container: blobConfig.container,
        token: blobConfig.token
      };
      if (!options.bulkPrefix && blobConfig.bulkPrefix) {
        options.bulkPrefix = blobConfig.bulkPrefix;
      }
    }
  }

  const schedulePool = new sql.ConnectionPool(config);
  await schedulePool.connect();
  try {
    const processedGroups: string[] = [];

    if (options.groups && options.groups.length > 0) {
      const builder = new ProposalBuilder();
      await builder.loadSchedules(schedulePool, options.referenceSchema || 'dbo');
      try {
        const certificates = await loadCertificatesFromDatabase(config, options);
        await processCertificates(builder, config, options, certificates, entropyOptions);
      } catch (err: any) {
        console.error(`❌ Failed to load certificates for groups: ${options.groups.join(', ')}`);
        throw err;
      }

      if (!options.dryRun) {
        const maxId = await loadMaxProposalProductId(config, options);
        builder.seedProposalProductCounter(maxId);
      }

      const output = builder.generateStagingOutput();
      await writeStagingOutput(config, output, options);
      processedGroups.push(...options.groups);
    } else {
      const batchSize = options.batchSize || 200;
      const groupIds = await loadDistinctGroups(config, options);
      for (let i = 0; i < groupIds.length; i += batchSize) {
        const batchGroups = groupIds.slice(i, i + batchSize);
        const builder = new ProposalBuilder();
        await builder.loadSchedules(schedulePool, options.referenceSchema || 'dbo');
        try {
          const batchOptions = { ...options, groups: batchGroups };
          const certificates = await loadCertificatesFromDatabase(config, batchOptions);
          await processCertificates(builder, config, batchOptions, certificates, entropyOptions);
          if (!batchOptions.dryRun) {
            const maxId = await loadMaxProposalProductId(config, batchOptions);
            builder.seedProposalProductCounter(maxId);
          }
          const output = builder.generateStagingOutput();
          await writeStagingOutput(config, output, batchOptions);
          processedGroups.push(...batchGroups);
        } catch (err: any) {
          console.error(`❌ Failed to process group batch: ${batchGroups.join(', ')}`);
          console.error(`  Error: ${err.message || err}`);
          continue;
        }
      }
    }

    const groupsToValidate = validateAllFlag
      ? Array.from(new Set(processedGroups))
      : validateGroupsArg;

    if (groupsToValidate.length > 0) {
      console.log(`\nValidation: checking ${groupsToValidate.length} group(s)`);
      const results = await validateGroups(config, groupsToValidate);
      const unmatched = results.filter(r => r.unmatchedRows > 0);
      const overlapping = results.filter(r => r.overlappingRows > 0);
      const failed = results.filter(r => r.unmatchedRows > 0 || r.overlappingRows > 0);
      const passed = results.length - failed.length;
      console.log(`\nValidation summary: ${passed}/${results.length} passed`);
      if (unmatched.length > 0) {
        console.log(`Groups with unmatched certs: ${unmatched.map(r => r.groupId).join(', ')}`);
      }
      if (overlapping.length > 0) {
        console.log(`⚠️  Groups with OVERLAPPING proposals: ${overlapping.map(r => `${r.groupId}(${r.overlappingRows})`).join(', ')}`);
      }
      if (failed.length > 0) {
        throw new Error(`Validation failed for ${failed.length} group(s)`);
      }
    }
  } finally {
    await schedulePool.close();
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

if (require.main === module) {
  const args = process.argv.slice(2);

  const modeArg = args.includes('--mode')
    ? (args[args.indexOf('--mode') + 1] as ExecutionMode)
    : 'transform';

  if (!['transform', 'export', 'full', 'validate', 'fix-overlaps'].includes(modeArg)) {
    console.error(`ERROR: Invalid mode '${modeArg}'. Must be one of: transform, export, full, validate, fix-overlaps`);
    process.exit(1);
  }

  let groups: string[] | undefined;
  let validateGroupsArg: string[] = [];
  let validateAllFlag = args.includes('--full-validation');
  let validateAllGroups = args.includes('--all');
  
  // Parse --from-audit for fix-overlaps mode
  const fromAuditPath = args.includes('--from-audit')
    ? args[args.indexOf('--from-audit') + 1]
    : undefined;
  
  // Parse --deep for deep chain validation
  const deepValidation = args.includes('--deep');
  
  // Parse --offset for resuming validation or parallel execution
  const offsetArg = args.includes('--offset')
    ? Number.parseInt(args[args.indexOf('--offset') + 1], 10)
    : 0;
  
  // Parse --limit-groups for parallel execution (limits number of groups processed)
  const limitGroupsArg = args.includes('--limit-groups')
    ? Number.parseInt(args[args.indexOf('--limit-groups') + 1], 10)
    : undefined;
  
  // Parse --runner-id for parallel execution logging
  const runnerIdArg = args.includes('--runner-id')
    ? args[args.indexOf('--runner-id') + 1]
    : undefined;
  
  // Parse --experiment for parallel execution logging
  const experimentArg = args.includes('--experiment')
    ? args[args.indexOf('--experiment') + 1]
    : undefined;
  let idx = 0;
  while (idx < args.length) {
    if (args[idx] === '--groups') {
      const groupsArg = args[idx + 1];
      if (groupsArg && !groupsArg.startsWith('--')) {
        const parsed = groupsArg.split(',').map(g => g.trim()).filter(g => g.length > 0);
        groups = groups ? groups.concat(parsed) : parsed;
        idx += 2;
        continue;
      }
    } else if (args[idx] === '--validate-groups') {
      const groupsArg = args[idx + 1];
      if (groupsArg && !groupsArg.startsWith('--')) {
        const parsed = groupsArg.split(',').map(g => g.trim()).filter(g => g.length > 0);
        validateGroupsArg = validateGroupsArg.concat(parsed);
        idx += 2;
        continue;
      }
    }
    idx += 1;
  }

  const configPath = args.includes('--config')
    ? args[args.indexOf('--config') + 1]
    : undefined;

  const options: BuilderOptions = {
    mode: modeArg,
    batchSize: args.includes('--batch-size')
      ? Number.parseInt(args[args.indexOf('--batch-size') + 1], 10)
      : undefined,
    dryRun: args.includes('--dry-run'),
    verbose: args.includes('--verbose'),
    limitCertificates: args.includes('--limit')
      ? Number.parseInt(args[args.indexOf('--limit') + 1], 10)
      : undefined,
    schema: args.includes('--schema')
      ? args[args.indexOf('--schema') + 1]
      : 'etl',
    referenceSchema: args.includes('--reference-schema')
      ? args[args.indexOf('--reference-schema') + 1]
      : 'dbo',
    productionSchema: args.includes('--production-schema')
      ? args[args.indexOf('--production-schema') + 1]
      : 'dbo',
    bulkMode: args.includes('--bulk-blob') ? 'blob' : 'db',
    bulkPrefix: args.includes('--bulk-prefix')
      ? args[args.indexOf('--bulk-prefix') + 1]
      : undefined,
    groups
  };

  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    console.error('ERROR: SQLSERVER environment variable not set');
    console.error('Format: Server=host;Database=db;User Id=user;Password=pass');
    process.exit(1);
  }

  const config = parseConnectionString(connectionString);

  async function execute() {
    const mode = options.mode || 'transform';

    console.log(`\nExecution Mode: ${mode.toUpperCase()}`);
    if (options.groups && options.groups.length > 0) {
      console.log(`Groups Filter: ${options.groups.join(', ')}`);
    }
    console.log('');

    if (mode === 'validate') {
      // Validate-only mode: skip transform, just run validation
      let groupsToValidate: string[];
      
      if (validateAllGroups) {
        console.log('Loading all groups from database...');
        groupsToValidate = await loadDistinctGroups(config, options);
        console.log(`Found ${groupsToValidate.length} groups`);
      } else if (validateGroupsArg.length > 0) {
        groupsToValidate = validateGroupsArg;
      } else if (options.groups && options.groups.length > 0) {
        groupsToValidate = options.groups;
      } else {
        console.error('ERROR: --mode validate requires --all, --validate-groups, or --groups');
        process.exit(1);
      }
      
      // Apply offset and limit for parallel execution
      if (offsetArg > 0 && offsetArg < groupsToValidate.length) {
        console.log(`Skipping first ${offsetArg} groups (--offset)`);
        groupsToValidate = groupsToValidate.slice(offsetArg);
      }
      if (limitGroupsArg && limitGroupsArg < groupsToValidate.length) {
        console.log(`Limiting to ${limitGroupsArg} groups (--limit-groups)`);
        groupsToValidate = groupsToValidate.slice(0, limitGroupsArg);
      }
      
      console.log(`Validating ${groupsToValidate.length} group(s)${deepValidation ? ' (with chain validation)' : ''}`);
      const results = await validateGroups(config, groupsToValidate, deepValidation);
      const unmatched = results.filter(r => r.unmatchedRows > 0);
      const overlapping = results.filter(r => r.overlappingRows > 0);
      const failed = results.filter(r => r.unmatchedRows > 0 || r.overlappingRows > 0);
      const passed = results.length - failed.length;
      console.log(`\nValidation summary: ${passed}/${results.length} passed`);
      if (unmatched.length > 0) {
        console.log(`Groups with unmatched certs: ${unmatched.map(r => r.groupId).join(', ')}`);
      }
      if (overlapping.length > 0) {
        console.log(`⚠️  Groups with OVERLAPPING proposals: ${overlapping.map(r => `${r.groupId}(${r.overlappingRows})`).join(', ')}`);
      }
      
      // Chain validation summary (only if --deep was used)
      if (deepValidation) {
        const chainIssues = results.filter(r => 
          (r.proposalsWithoutSplitVersion || 0) > 0 ||
          (r.splitVersionsWithoutParticipants || 0) > 0 ||
          (r.participantsWithoutHierarchy || 0) > 0 ||
          (r.hierarchiesWithoutParticipants || 0) > 0 ||
          (r.hierarchyParticipantsWithoutSchedule || 0) > 0
        );
        if (chainIssues.length > 0) {
          console.log(`\n⚠️  Chain validation issues found in ${chainIssues.length} group(s):`);
          for (const r of chainIssues) {
            const issues: string[] = [];
            if ((r.proposalsWithoutSplitVersion || 0) > 0) issues.push(`proposals-no-PSV=${r.proposalsWithoutSplitVersion}`);
            if ((r.splitVersionsWithoutParticipants || 0) > 0) issues.push(`PSV-no-participants=${r.splitVersionsWithoutParticipants}`);
            if ((r.participantsWithoutHierarchy || 0) > 0) issues.push(`PSP-no-hierarchy=${r.participantsWithoutHierarchy}`);
            if ((r.hierarchiesWithoutParticipants || 0) > 0) issues.push(`hierarchies-no-participants=${r.hierarchiesWithoutParticipants}`);
            if ((r.hierarchyParticipantsWithoutSchedule || 0) > 0) issues.push(`HP-no-schedule=${r.hierarchyParticipantsWithoutSchedule}`);
            console.log(`  ${r.groupId}: ${issues.join(', ')}`);
          }
        } else {
          console.log(`\n✓ All chain validations passed`);
        }

        // Content validation summary (brokers and schedules)
        const contentIssues = results.filter(r => 
          (r.missingBrokers || 0) > 0 ||
          (r.missingSchedules || 0) > 0
        );
        if (contentIssues.length > 0) {
          console.log(`\n⚠️  Content validation issues found in ${contentIssues.length} group(s):`);
          for (const r of contentIssues) {
            const issues: string[] = [];
            if ((r.missingBrokers || 0) > 0) issues.push(`missing-brokers=${r.missingBrokers} (${r.certsWithMissingBroker} certs)`);
            if ((r.missingSchedules || 0) > 0) issues.push(`missing-schedules=${r.missingSchedules} (${r.certsWithMissingSchedule} certs)`);
            console.log(`  ${r.groupId}: ${issues.join(', ')}`);
          }
        } else {
          console.log(`\n✓ All content validations passed (brokers & schedules match)`);
        }
      }
      
      // If runner-id specified, write summary to log file
      if (runnerIdArg && experimentArg) {
        const fs = await import('fs');
        const path = await import('path');
        const logDir = path.join(process.cwd(), 'logs', experimentArg);
        if (!fs.existsSync(logDir)) {
          fs.mkdirSync(logDir, { recursive: true });
        }
        const logPath = path.join(logDir, `runner-${runnerIdArg}.json`);
        const summary = {
          runnerId: runnerIdArg,
          experiment: experimentArg,
          mode: 'validate',
          offset: offsetArg,
          limit: limitGroupsArg,
          groupsValidated: groupsToValidate.length,
          passed: results.length - failed.length,
          failed: failed.length,
          unmatched: unmatched.length,
          overlapping: overlapping.length,
          completedAt: new Date().toISOString()
        };
        fs.writeFileSync(logPath, JSON.stringify(summary, null, 2));
        console.log(`Summary written to: ${logPath}`);
      }
      
      if (failed.length > 0) {
        throw new Error(`Validation failed for ${failed.length} group(s)`);
      }
    }

    if (mode === 'fix-overlaps') {
      // Fix overlapping proposals by routing affected certs to PHA
      let groupsToFix: string[] = [];
      
      if (fromAuditPath) {
        // Parse audit log to find groups with issues
        console.log(`Parsing audit log: ${fromAuditPath}`);
        const issues = parseAuditLogForIssues(fromAuditPath);
        groupsToFix = issues.overlapping;
        console.log(`Found ${issues.overlapping.length} groups with overlapping proposals`);
        if (issues.unmatched.length > 0) {
          console.log(`Note: ${issues.unmatched.length} groups with unmatched certs (not fixed by this mode)`);
        }
      } else if (options.groups && options.groups.length > 0) {
        groupsToFix = options.groups;
      } else {
        console.error('ERROR: --mode fix-overlaps requires --from-audit <path> or --groups');
        process.exit(1);
      }
      
      if (groupsToFix.length === 0) {
        console.log('No groups to fix.');
      } else {
        console.log(`\nFixing overlapping proposals for ${groupsToFix.length} group(s):`);
        if (groupsToFix.length <= 20) {
          console.log(`  ${groupsToFix.join(', ')}`);
        } else {
          console.log(`  ${groupsToFix.slice(0, 20).join(', ')}... and ${groupsToFix.length - 20} more`);
        }
        console.log('');
        
        const fixResults = await fixOverlappingProposals(config, options, groupsToFix, options.dryRun);
        
        const totalFixed = fixResults.reduce((sum, r) => sum + r.fixed, 0);
        console.log(`\n${options.dryRun ? '[DRY RUN] Would fix' : 'Fixed'} ${totalFixed} overlapping certificates across ${fixResults.filter(r => r.fixed > 0).length} groups`);
      }
    }

    if (mode === 'transform' || mode === 'full') {
      // If --all flag is set with parallel params, load and slice groups
      if (validateAllGroups && (offsetArg > 0 || limitGroupsArg)) {
        console.log('Loading all groups from database for parallel execution...');
        const allGroups = await loadDistinctGroups(config, options);
        console.log(`Total groups: ${allGroups.length}`);
        
        // Apply offset and limit
        let slicedGroups = allGroups;
        if (offsetArg > 0) {
          console.log(`Skipping first ${offsetArg} groups (--offset)`);
          slicedGroups = slicedGroups.slice(offsetArg);
        }
        if (limitGroupsArg) {
          console.log(`Limiting to ${limitGroupsArg} groups (--limit-groups)`);
          slicedGroups = slicedGroups.slice(0, limitGroupsArg);
        }
        
        if (slicedGroups.length === 0) {
          console.log('No groups to process after offset/limit applied.');
        } else {
          console.log(`Processing ${slicedGroups.length} groups (${offsetArg} to ${offsetArg + slicedGroups.length - 1})`);
          options.groups = slicedGroups;
        }
      }
      
      await runProposalBuilderV2(config, options, validateGroupsArg, validateAllFlag, configPath);
      
      // If runner-id specified, write summary to log file
      if (runnerIdArg && experimentArg) {
        const fs = await import('fs');
        const path = await import('path');
        const logDir = path.join(process.cwd(), 'logs', experimentArg);
        if (!fs.existsSync(logDir)) {
          fs.mkdirSync(logDir, { recursive: true });
        }
        const logPath = path.join(logDir, `runner-${runnerIdArg}.json`);
        const summary = {
          runnerId: runnerIdArg,
          experiment: experimentArg,
          offset: offsetArg,
          limit: limitGroupsArg,
          groupsProcessed: options.groups?.length || 0,
          completedAt: new Date().toISOString()
        };
        fs.writeFileSync(logPath, JSON.stringify(summary, null, 2));
        console.log(`Summary written to: ${logPath}`);
      }
    }

    if (mode === 'export' || mode === 'full') {
      await exportStagingToProduction(config, options);
    }

    console.log('');
    console.log('✅ Done!');
  }

  execute()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ Error:', err.message || err);
      process.exit(1);
    });
}
