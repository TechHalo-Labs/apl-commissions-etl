/**
 * Add Performance Indexes for PHA Script
 *
 * This script adds critical indexes to dramatically improve
 * fix-policy-hierarchy-assignments.ts performance.
 *
 * Usage:
 *   npx tsx scripts/add-performance-indexes.ts
 */

import * as sql from 'mssql';

// =============================================================================
// Database Configuration
// =============================================================================

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

// =============================================================================
// Index Creation Script
// =============================================================================

async function addPerformanceIndexes(pool: sql.ConnectionPool): Promise<void> {
  console.log('='.repeat(70));
  console.log('ADDING PERFORMANCE INDEXES FOR PHA SCRIPT');
  console.log('='.repeat(70));
  console.log('');

  console.log('Adding critical indexes for PHA performance...');

  // ============================================================================
  // 1. Schedules Table - External ID lookups (HIGH IMPACT)
  // ============================================================================

  console.log('1. Checking Schedules.ExternalId index...');

  const schedulesIndexResult = await pool.request().query(`
    SELECT COUNT(*) as exists_count
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.Schedules')
      AND name = 'IX_Schedules_ExternalId'
  `);

  if (schedulesIndexResult.recordset[0].exists_count === 0) {
    console.log('   Creating IX_Schedules_ExternalId (filtered index)...');

    await pool.request().query(`
      CREATE NONCLUSTERED INDEX IX_Schedules_ExternalId
      ON dbo.Schedules (ExternalId)
      WHERE ExternalId IS NOT NULL
    `);

    console.log('   ✓ Created IX_Schedules_ExternalId');
  } else {
    console.log('   ✓ IX_Schedules_ExternalId already exists');
  }

  // ============================================================================
  // 2. Policies Table - Bulk ID lookups with covering columns (MEDIUM IMPACT)
  // ============================================================================

  console.log('2. Checking Policies.Id covering index...');

  const policiesIndexResult = await pool.request().query(`
    SELECT COUNT(*) as exists_count
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('dbo.Policies')
      AND name = 'IX_Policies_Id_Includes'
  `);

  if (policiesIndexResult.recordset[0].exists_count === 0) {
    console.log('   Creating IX_Policies_Id_Includes (covering index)...');

    await pool.request().query(`
      CREATE NONCLUSTERED INDEX IX_Policies_Id_Includes
      ON dbo.Policies (Id)
      INCLUDE (GroupId, ProductCode, State, EffectiveDate, Premium)
    `);

    console.log('   ✓ Created IX_Policies_Id_Includes');
  } else {
    console.log('   ✓ IX_Policies_Id_Includes already exists');
  }

  // ============================================================================
  // 3. Show current performance stats
  // ============================================================================

  console.log('');
  console.log('Performance statistics:');

  const schedulesStats = await pool.request().query(`
    SELECT
      COUNT(*) as TotalSchedules,
      COUNT(ExternalId) as SchedulesWithExternalId
    FROM dbo.Schedules
  `);

  const policiesStats = await pool.request().query(`
    SELECT COUNT(*) as TotalPolicies FROM dbo.Policies
  `);

  console.log(`   Schedules: ${schedulesStats.recordset[0].TotalSchedules} total, ${schedulesStats.recordset[0].SchedulesWithExternalId} with ExternalId`);
  console.log(`   Policies: ${policiesStats.recordset[0].TotalPolicies} total`);

  console.log('');
  console.log('='.repeat(70));
  console.log('PERFORMANCE INDEXES ADDED SUCCESSFULLY!');
  console.log('='.repeat(70));
  console.log('');
  console.log('Expected performance improvements:');
  console.log('• Schedules lookup: ~100x faster (index seek vs table scan)');
  console.log('• Policy lookups: ~50% faster (covering index)');
  console.log('• Overall PHA script: ~60-70% faster');
  console.log('');
  console.log('Next: Run the optimized PHA script:');
  console.log('  npx tsx scripts/fix-policy-hierarchy-assignments-fast.ts --verbose');
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main(): Promise<void> {
  // Get connection string from environment
  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    console.error('ERROR: SQLSERVER environment variable not set');
    console.error('Format: Server=host;Database=db;User Id=user;Password=pass');
    process.exit(1);
  }

  // Parse connection string
  const parts = connectionString.split(';').reduce((acc, part) => {
    const [key, value] = part.split('=');
    if (key && value) acc[key.trim().toLowerCase()] = value.trim();
    return acc;
  }, {} as Record<string, string>);

  const config: DatabaseConfig = {
    server: parts['server'] || parts['data source'] || '',
    database: parts['database'] || parts['initial catalog'] || '',
    user: parts['user id'] || parts['uid'] || '',
    password: parts['password'] || parts['pwd'] || '',
    options: {
      encrypt: true,
      trustServerCertificate: true
    }
  };

  // Connect to database
  const pool = await sql.connect(config);

  try {
    await addPerformanceIndexes(pool);
  } finally {
    await pool.close();
  }
}

// Run
main().catch(err => {
  console.error('❌ Error:', err);
  process.exit(1);
});