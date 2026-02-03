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
}

interface ValidationResult {
  groupId: string;
  nonPhaRows: number;
  unmatchedRows: number;
}

function loadEntropyOptions(): EntropyOptions {
  const appSettingsPath = path.join(process.cwd(), 'appsettings.json');
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
    const result = await pool.request().query(`
      SELECT DISTINCT LTRIM(RTRIM(ci.GroupId)) AS GroupId
      FROM [${schema}].[input_certificate_info] ci
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

async function validateGroups(config: DatabaseConfig, groups: string[]): Promise<ValidationResult[]> {
  if (groups.length === 0) return [];
  const pool = await sql.connect(config);
  try {
    const results: ValidationResult[] = [];
    for (const rawGroupId of groups) {
      const trimmed = rawGroupId.trim();
      const groupIdNumeric = trimmed.replace(/^[A-Za-z]+/, '');
      const groupIdWithPrefix = `G${groupIdNumeric}`;

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
           WHERE m.ProposalId IS NULL) AS UnmatchedRows;
      `);

      results.push({
        groupId: groupIdWithPrefix,
        nonPhaRows: counts.recordset[0]?.NonPhaRows || 0,
        unmatchedRows: counts.recordset[0]?.UnmatchedRows || 0
      });

      if ((counts.recordset[0]?.UnmatchedRows || 0) > 0) {
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
  builder.buildProposals();
}

async function runProposalBuilderV2(
  config: DatabaseConfig,
  options: BuilderOptions,
  validateGroupsArg: string[],
  validateAllFlag: boolean
): Promise<void> {
  const entropyOptions = loadEntropyOptions();
  if (options.verbose) {
    entropyOptions.logEntropyByGroup = true;
    entropyOptions.verbose = true;
  }

  const schedulePool = await sql.connect(config);
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
      const failed = results.filter(r => r.unmatchedRows > 0);
      for (const r of results) {
        console.log(`  ${r.groupId}: non-PHA rows=${r.nonPhaRows}, unmatched=${r.unmatchedRows}`);
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

  if (!['transform', 'export', 'full'].includes(modeArg)) {
    console.error(`ERROR: Invalid mode '${modeArg}'. Must be one of: transform, export, full`);
    process.exit(1);
  }

  let groups: string[] | undefined;
  let validateGroupsArg: string[] = [];
  let validateAllFlag = args.includes('--full-validation');
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

    if (mode === 'transform' || mode === 'full') {
      await runProposalBuilderV2(config, options, validateGroupsArg, validateAllFlag);
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
