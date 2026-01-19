/**
 * Destructive Export to Shadow Schema
 * 
 * This script creates a complete shadow copy of the production database:
 * 1. Creates a new schema (prod_shadow)
 * 2. Copies ALL table structures from dbo
 * 3. Copies Platform/ABP tables WITH data (users, roles, settings, etc.)
 * 4. Leaves Domain tables EMPTY
 * 5. Exports ETL staging data to the shadow schema
 * 
 * After validation, the shadow schema can replace the production schema.
 * 
 * Usage:
 *   npx tsx scripts/destructive-export-to-shadow.ts [options]
 * 
 * Options:
 *   --dry-run      Show what would be done without executing
 *   --step <n>     Run only step n (1-5)
 *   --truncate     Truncate domain tables before export (default: false)
 *   --schema <s>   Target schema name (default: prod_shadow)
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

const DEFAULT_SHADOW_SCHEMA = 'prod_shadow';

/**
 * Parse a SQL Server connection string into mssql config
 * Format: Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=...;Encrypt=...;
 */
function parseConnectionString(connStr: string): Partial<sql.config> {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'] || parts['user'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: parts['encrypt']?.toLowerCase() !== 'false',
      trustServerCertificate: parts['trustservercertificate']?.toLowerCase() === 'true',
    }
  };
}

/**
 * Get SQL Server configuration from environment
 * REQUIRES: $SQLSERVER connection string OR individual env vars
 * NO DEFAULTS - Will exit with error if not configured
 */
function getSqlConfig(): sql.config {
  const connStr = process.env.SQLSERVER;
  
  if (connStr) {
    const parsed = parseConnectionString(connStr);
    if (!parsed.server || !parsed.database || !parsed.user || !parsed.password) {
      console.error('❌ Invalid $SQLSERVER connection string. Expected format:');
      console.error('   Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;');
      process.exit(1);
    }
    return {
      server: parsed.server,
      database: parsed.database,
      user: parsed.user,
      password: parsed.password,
      options: {
        encrypt: parsed.options?.encrypt ?? true,
        trustServerCertificate: parsed.options?.trustServerCertificate ?? true,
        requestTimeout: 600000  // 10 minutes for large operations
      },
      pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
      }
    };
  }
  
  // Fall back to individual environment variables
  const server = process.env.SQLSERVER_HOST;
  const database = process.env.SQLSERVER_DATABASE;
  const user = process.env.SQLSERVER_USER;
  const password = process.env.SQLSERVER_PASSWORD;
  
  if (!server || !database || !user || !password) {
    console.error('');
    console.error('❌ SQL Server connection not configured!');
    console.error('');
    console.error('Please set one of the following:');
    console.error('');
    console.error('Option 1: Single connection string (recommended)');
    console.error('  export SQLSERVER="Server=<host>;Database=<db>;User Id=<user>;Password=<pwd>;TrustServerCertificate=True;Encrypt=True;"');
    console.error('');
    console.error('Option 2: Individual environment variables');
    console.error('  export SQLSERVER_HOST=<host>');
    console.error('  export SQLSERVER_DATABASE=<db>');
    console.error('  export SQLSERVER_USER=<user>');
    console.error('  export SQLSERVER_PASSWORD=<pwd>');
    console.error('');
    process.exit(1);
  }
  
  return {
    server,
    database,
    user,
    password,
    options: {
      encrypt: true,
      trustServerCertificate: true,
      requestTimeout: 600000
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000
    }
  };
}

const config: sql.config = getSqlConfig();

// Platform tables - copy WITH data
const PLATFORM_TABLES = [
  '__EFMigrationsHistory',
  'AbpAuditLogs',
  'AbpBackgroundJobs',
  'AbpDynamicEntityProperties',
  'AbpDynamicEntityPropertyValues',
  'AbpDynamicProperties',
  'AbpDynamicPropertyValues',
  'AbpEditions',
  'AbpEntityChanges',
  'AbpEntityChangeSets',
  'AbpEntityPropertyChanges',
  'AbpFeatures',
  'AbpLanguages',
  'AbpLanguageTexts',
  'AbpNotifications',
  'AbpNotificationSubscriptions',
  'AbpOrganizationUnitRoles',
  'AbpOrganizationUnits',
  'AbpPermissions',
  'AbpRoleClaims',
  'AbpRoles',
  'AbpSettings',
  'AbpTenantNotifications',
  'AbpTenants',
  'AbpUserAccounts',
  'AbpUserClaims',
  'AbpUserLoginAttempts',
  'AbpUserLogins',
  'AbpUserNotifications',
  'AbpUserOrganizationUnits',
  'AbpUserRoles',
  'AbpUsers',
  'AbpUserTokens',
  'AbpWebhookEvents',
  'AbpWebhookSendAttempts',
  'AbpWebhookSubscriptions',
  'AppBinaryObjects',
];

// Tables that should also preserve data (app config, not domain data)
const PRESERVE_DATA_TABLES = [
  'ReportCategories',
  'ReportConnections',
  'ReportDataSources',
  'ReportDataSourceFields',
  'ReportDefinitions',
  'ReportDefinitionCategories',
  'ReportDefinitionDataSources',
  'ReportDefinitionVersions',
  'Reports',
  'ReportSchedules',
  'ReportScheduleRecipients',
  'Plans',
  'FeeTypes',
  'ManualAdjustmentTypes',
  'GLAccounts',
  'GLFiscalPeriods',
  'GLSavedFilters',
  'GLUserSettings',
  'MetadataDescriptors',
  'TagDescriptorAssignments',
  'EntityTagDefinitions',
];

// ETL-managed domain tables - will be populated from staging
const ETL_DOMAIN_TABLES = [
  'Brokers',
  'BrokerLicenses',
  'BrokerEOInsurances',
  'BrokerAddresses',
  'BrokerContacts',
  'Group',
  'GroupAddresses',
  'GroupContacts',
  'Products',
  'ProductCodes',
  'Schedules',
  'ScheduleRates',
  'SpecialScheduleRates',
  'Contracts',
  'Proposals',
  'ProposalProducts',
  'PremiumSplitVersions',
  'PremiumSplitParticipants',
  'Hierarchies',
  'HierarchyVersions',
  'HierarchyParticipants',
  'StateRules',
  'HierarchySplits',
  'SplitDistributions',
  'Policies',
  'PolicyHierarchyAssignments',
  'Certificates',
];

function log(msg: string, level: 'info' | 'success' | 'warn' | 'error' = 'info'): void {
  const timestamp = new Date().toISOString();
  const prefix = {
    info: '   ',
    success: ' ✅',
    warn: ' ⚠️',
    error: ' ❌'
  }[level];
  console.log(`[${timestamp}]${prefix} ${msg}`);
}

interface RunOptions {
  dryRun: boolean;
  truncate: boolean;
  schema: string;
}

async function step1_createSchema(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log('STEP 1: Create Shadow Schema');
  log('═'.repeat(70));
  
  if (opts.dryRun) {
    log(`Would create schema [${opts.schema}]`, 'info');
    return;
  }
  
  // Drop existing shadow schema if exists
  log(`Dropping existing schema [${opts.schema}] if exists...`);
  
  // First drop all tables in the shadow schema
  const existingTables = await pool.request().query(`
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '${opts.schema}'
  `);
  
  for (const row of existingTables.recordset) {
    try {
      await pool.request().query(`DROP TABLE IF EXISTS [${opts.schema}].[${row.TABLE_NAME}]`);
      log(`  Dropped [${opts.schema}].[${row.TABLE_NAME}]`);
    } catch (e: any) {
      log(`  Could not drop [${row.TABLE_NAME}]: ${e.message}`, 'warn');
    }
  }
  
  // Drop the schema
  try {
    await pool.request().query(`DROP SCHEMA IF EXISTS [${opts.schema}]`);
  } catch (e) {
    // Schema might not exist
  }
  
  // Create fresh schema
  await pool.request().query(`CREATE SCHEMA [${opts.schema}]`);
  log(`Created schema [${opts.schema}]`, 'success');
}

async function step2_copyTableStructures(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log('STEP 2: Copy Table Structures');
  log('═'.repeat(70));
  
  // Get all tables from dbo
  const tables = await pool.request().query(`
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
  `);
  
  log(`Found ${tables.recordset.length} tables in [dbo] schema`);
  
  if (opts.dryRun) {
    log(`Would copy structure of ${tables.recordset.length} tables to [${opts.schema}]`);
    return;
  }
  
  let success = 0;
  let failed = 0;
  
  for (const row of tables.recordset) {
    const tableName = row.TABLE_NAME;
    try {
      // Use SELECT INTO with WHERE 1=0 to copy structure without data
      await pool.request().query(`
        SELECT * INTO [${opts.schema}].[${tableName}]
        FROM [dbo].[${tableName}]
        WHERE 1 = 0
      `);
      success++;
    } catch (e: any) {
      log(`  Failed to copy structure for [${tableName}]: ${e.message}`, 'warn');
      failed++;
    }
  }
  
  log(`Copied ${success} table structures, ${failed} failed`, success > 0 ? 'success' : 'error');
}

async function step3_copyPlatformData(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log('STEP 3: Copy Platform Table Data');
  log('═'.repeat(70));
  
  const allPlatformTables = [...PLATFORM_TABLES, ...PRESERVE_DATA_TABLES];
  log(`Copying data for ${allPlatformTables.length} platform/config tables`);
  
  if (opts.dryRun) {
    allPlatformTables.forEach(t => log(`  Would copy data: [${t}]`));
    return;
  }
  
  let success = 0;
  let failed = 0;
  
  for (const tableName of allPlatformTables) {
    try {
      // Check if table exists in dbo
      const exists = await pool.request().query(`
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '${tableName}'
      `);
      
      if (exists.recordset.length === 0) {
        log(`  Skipping [${tableName}] - not found in dbo`, 'warn');
        continue;
      }
      
      // Enable identity insert if needed
      const hasIdentity = await pool.request().query(`
        SELECT 1 FROM sys.identity_columns 
        WHERE OBJECT_NAME(object_id) = '${tableName}'
      `);
      
      if (hasIdentity.recordset.length > 0) {
        await pool.request().query(`SET IDENTITY_INSERT [${opts.schema}].[${tableName}] ON`);
      }
      
      // Copy data
      const result = await pool.request().query(`
        INSERT INTO [${opts.schema}].[${tableName}]
        SELECT * FROM [dbo].[${tableName}]
      `);
      
      if (hasIdentity.recordset.length > 0) {
        await pool.request().query(`SET IDENTITY_INSERT [${opts.schema}].[${tableName}] OFF`);
      }
      
      log(`  Copied [${tableName}]: ${result.rowsAffected[0]} rows`);
      success++;
    } catch (e: any) {
      log(`  Failed to copy [${tableName}]: ${e.message}`, 'warn');
      failed++;
    }
  }
  
  log(`Copied ${success} platform tables, ${failed} failed`, success > 0 ? 'success' : 'error');
}

async function step4_truncateDomainTables(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log(`STEP 4: ${opts.truncate ? 'Truncate' : 'Verify'} Domain Tables`);
  log('═'.repeat(70));
  
  if (opts.dryRun) {
    if (opts.truncate) {
      log(`Would TRUNCATE ${ETL_DOMAIN_TABLES.length} domain tables in [${opts.schema}]`, 'warn');
    } else {
      log(`Would verify ${ETL_DOMAIN_TABLES.length} domain tables are empty`);
    }
    return;
  }
  
  for (const tableName of ETL_DOMAIN_TABLES) {
    try {
      if (opts.truncate) {
        // Truncate the table
        await pool.request().query(`TRUNCATE TABLE [${opts.schema}].[${tableName}]`);
        log(`  [${tableName}]: Truncated ✓`);
      } else {
        // Just verify it's empty
        const count = await pool.request().query(`
          SELECT COUNT(*) AS cnt FROM [${opts.schema}].[${tableName}]
        `);
        
        if (count.recordset[0].cnt === 0) {
          log(`  [${tableName}]: Empty ✓`);
        } else {
          log(`  [${tableName}]: ${count.recordset[0].cnt} rows (use --truncate to clear)`, 'warn');
        }
      }
    } catch (e: any) {
      log(`  [${tableName}]: ${e.message.includes('Cannot truncate') ? 'FK constraint - using DELETE' : 'Not found'}`, 'warn');
      // Try DELETE if TRUNCATE fails due to FK constraints
      if (opts.truncate && e.message.includes('Cannot truncate')) {
        try {
          await pool.request().query(`DELETE FROM [${opts.schema}].[${tableName}]`);
          log(`  [${tableName}]: Deleted all rows ✓`);
        } catch (e2: any) {
          log(`  [${tableName}]: Failed to delete - ${e2.message}`, 'error');
        }
      }
    }
  }
  
  log(`Domain tables ${opts.truncate ? 'truncated' : 'verified'}`, 'success');
}

async function step5_exportETLData(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log(`STEP 5: Export ETL Staging Data to [${opts.schema}]`);
  log('═'.repeat(70));
  
  if (opts.dryRun) {
    log(`Would export ETL staging data to [${opts.schema}]`);
    return;
  }
  
  // Define export mappings: staging table -> shadow table
  // Order matters for FK constraints - parent tables first
  const exports = [
    { staging: 'stg_brokers', target: 'Brokers' },
    { staging: 'stg_groups', target: 'Group' },
    { staging: 'stg_products', target: 'Products' },
    { staging: 'stg_schedules', target: 'Schedules' },
    { staging: 'stg_schedule_rates', target: 'ScheduleRates' },
    { staging: 'stg_special_schedule_rates', target: 'SpecialScheduleRates' },
    { staging: 'stg_proposals', target: 'Proposals' },
    { staging: 'stg_premium_split_versions', target: 'PremiumSplitVersions' },
    { staging: 'stg_premium_split_participants', target: 'PremiumSplitParticipants' },
    { staging: 'stg_hierarchies', target: 'Hierarchies' },
    { staging: 'stg_hierarchy_versions', target: 'HierarchyVersions' },
    { staging: 'stg_hierarchy_participants', target: 'HierarchyParticipants' },
    { staging: 'stg_state_rules', target: 'StateRules' },
    { staging: 'stg_hierarchy_splits', target: 'HierarchySplits' },
    { staging: 'stg_split_distributions', target: 'SplitDistributions' },
    { staging: 'stg_policies', target: 'Policies' },
    { staging: 'stg_policy_hierarchy_assignments', target: 'PolicyHierarchyAssignments' },
  ];
  
  let totalRows = 0;
  
  for (const exp of exports) {
    try {
      // Check if staging table exists
      const stgExists = await pool.request().query(`
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = '${exp.staging}'
      `);
      
      if (stgExists.recordset.length === 0) {
        log(`  Skipping [${exp.staging}] - staging table not found`, 'warn');
        continue;
      }
      
      // Get column list from staging table (excluding computed columns)
      const columns = await pool.request().query(`
        SELECT c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = 'etl' AND c.TABLE_NAME = '${exp.staging}'
        ORDER BY c.ORDINAL_POSITION
      `);
      
      // Get columns that exist in both staging and target
      const targetColumns = await pool.request().query(`
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${opts.schema}' AND TABLE_NAME = '${exp.target}'
      `);
      
      const targetColSet = new Set(targetColumns.recordset.map((r: any) => r.COLUMN_NAME));
      const commonColumns = columns.recordset
        .map((r: any) => r.COLUMN_NAME)
        .filter((c: string) => targetColSet.has(c));
      
      if (commonColumns.length === 0) {
        log(`  Skipping [${exp.staging}] - no common columns with target`, 'warn');
        continue;
      }
      
      const colList = commonColumns.map((c: string) => `[${c}]`).join(', ');
      
      // Insert data
      const result = await pool.request().query(`
        INSERT INTO [${opts.schema}].[${exp.target}] (${colList})
        SELECT ${colList} FROM [etl].[${exp.staging}]
      `);
      
      const rowCount = result.rowsAffected[0] || 0;
      totalRows += rowCount;
      log(`  Exported [${exp.staging}] -> [${exp.target}]: ${rowCount.toLocaleString()} rows`);
    } catch (e: any) {
      log(`  Failed to export [${exp.staging}]: ${e.message}`, 'error');
    }
  }
  
  log(`ETL data export complete. Total rows: ${totalRows.toLocaleString()}`, 'success');
}

async function showSummary(pool: sql.ConnectionPool, opts: RunOptions): Promise<void> {
  log('');
  log('═'.repeat(70));
  log(`SUMMARY: [${opts.schema}] Schema Contents`);
  log('═'.repeat(70));
  
  // Count tables
  const tableCount = await pool.request().query(`
    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '${opts.schema}'
  `);
  
  log(`Total tables in [${opts.schema}]: ${tableCount.recordset[0].cnt}`);
  
  // Show key table counts
  const keyTables = ['Brokers', 'Group', 'Proposals', 'PremiumSplitVersions', 
                     'PremiumSplitParticipants', 'Hierarchies', 'Policies', 'AbpUsers'];
  
  log('');
  log('Key table row counts:');
  for (const table of keyTables) {
    try {
      const count = await pool.request().query(`
        SELECT COUNT(*) AS cnt FROM [${opts.schema}].[${table}]
      `);
      log(`  [${table}]: ${count.recordset[0].cnt.toLocaleString()}`);
    } catch (e) {
      log(`  [${table}]: Not found`, 'warn');
    }
  }
  
  log('');
  log('═'.repeat(70));
  log('NEXT STEPS');
  log('═'.repeat(70));
  log('');
  log('1. Review the shadow schema data');
  log('2. Run validation queries');
  log('3. When satisfied, run the swap script:');
  log('');
  log('   -- Swap schemas (run manually after validation)');
  log(`   EXEC sp_rename 'dbo', 'dbo_backup';`);
  log(`   EXEC sp_rename '${opts.schema}', 'dbo';`);
  log('');
  log('   -- Or use ALTER SCHEMA TRANSFER for individual tables');
  log('');
}

function parseArgs(): { opts: RunOptions; specificStep: number | null } {
  const args = process.argv.slice(2);
  
  const dryRun = args.includes('--dry-run');
  const truncate = args.includes('--truncate');
  
  const stepArg = args.indexOf('--step');
  const specificStep = stepArg !== -1 ? parseInt(args[stepArg + 1]) : null;
  
  const schemaArg = args.indexOf('--schema');
  const schema = schemaArg !== -1 ? args[schemaArg + 1] : DEFAULT_SHADOW_SCHEMA;
  
  return {
    opts: { dryRun, truncate, schema },
    specificStep
  };
}

async function main(): Promise<void> {
  const { opts, specificStep } = parseArgs();
  
  log('');
  log('═'.repeat(70));
  log('Destructive Export to Shadow Schema');
  log('═'.repeat(70));
  log(`Target Schema: [${opts.schema}]`);
  log(`Dry Run: ${opts.dryRun}`);
  log(`Truncate: ${opts.truncate}`);
  log(`Specific Step: ${specificStep || 'All'}`);
  log('');
  
  if (opts.dryRun) {
    log('DRY RUN MODE - No changes will be made', 'warn');
  }
  
  if (opts.truncate) {
    log('TRUNCATE MODE - Domain tables will be wiped before export!', 'warn');
  }
  
  const pool = await sql.connect(config);
  
  try {
    if (!specificStep || specificStep === 1) {
      await step1_createSchema(pool, opts);
    }
    
    if (!specificStep || specificStep === 2) {
      await step2_copyTableStructures(pool, opts);
    }
    
    if (!specificStep || specificStep === 3) {
      await step3_copyPlatformData(pool, opts);
    }
    
    if (!specificStep || specificStep === 4) {
      await step4_truncateDomainTables(pool, opts);
    }
    
    if (!specificStep || specificStep === 5) {
      await step5_exportETLData(pool, opts);
    }
    
    if (!opts.dryRun) {
      await showSummary(pool, opts);
    }
    
    log('');
    log('Script completed successfully!', 'success');
    
  } finally {
    await pool.close();
  }
}

main().catch(err => {
  log(`Fatal error: ${err.message}`, 'error');
  console.error(err);
  process.exit(1);
});
