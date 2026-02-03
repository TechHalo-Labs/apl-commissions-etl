/**
 * Assemble Report - Aggregates results from parallel runner logs
 * 
 * Usage:
 *   npx tsx scripts/new-builder/assemble-report.ts --experiment "2026-02-03-10pct"
 */

import * as fs from 'fs';
import * as path from 'path';
import * as sql from 'mssql';

interface WorkerSummary {
  runnerId: number;
  pid: number;
  offset: number;
  limit: number;
  status: 'running' | 'completed' | 'failed';
  startedAt: string;
  completedAt?: string;
  exitCode?: number;
}

interface MasterSummary {
  experiment: string;
  totalGroups: number;
  numWorkers: number;
  batchSize: number;
  startTime: string;
  endTime: string;
  elapsedSeconds: number;
  workers: WorkerSummary[];
}

interface LogStats {
  proposalsCreated: number;
  phaRecords: number;
  certificatesProcessed: number;
  regimesDetected: number;
  outliersRouted: number;
  errors: string[];
}

function parseLogFile(logPath: string): LogStats {
  const stats: LogStats = {
    proposalsCreated: 0,
    phaRecords: 0,
    certificatesProcessed: 0,
    regimesDetected: 0,
    outliersRouted: 0,
    errors: []
  };
  
  if (!fs.existsSync(logPath)) {
    return stats;
  }
  
  const content = fs.readFileSync(logPath, 'utf-8');
  const lines = content.split('\n');
  
  for (const line of lines) {
    // Match: "Proposals: 322, Proposal Products: 1021, Hierarchies: 23932, Key Mappings: 17525"
    const proposalsMatch = line.match(/Proposals: (\d+)/);
    if (proposalsMatch) {
      stats.proposalsCreated += parseInt(proposalsMatch[1], 10);
    }
    
    // Match: "✓ Generated 112810 PHA assignments with hierarchies"
    const phaMatch = line.match(/Generated (\d+) PHA assignments/);
    if (phaMatch) {
      stats.phaRecords += parseInt(phaMatch[1], 10);
    }
    
    // Match: "Processing 93189 unique certificates..."
    const certsMatch = line.match(/Processing (\d+) unique certificates/);
    if (certsMatch) {
      stats.certificatesProcessed += parseInt(certsMatch[1], 10);
    }
    
    // Match: "✓ Segmented into 4 regimes"
    const regimesMatch = line.match(/Segmented into (\d+) regimes/);
    if (regimesMatch) {
      stats.regimesDetected += parseInt(regimesMatch[1], 10);
    }
    
    // Match: "✓ Routed 906 certificates from 368 outlier proposals to PHA"
    const outliersMatch = line.match(/Routed (\d+) certificates from \d+ outlier/);
    if (outliersMatch) {
      stats.outliersRouted += parseInt(outliersMatch[1], 10);
    }
    
    // Capture errors
    if (line.includes('Error:') || line.includes('ERROR:')) {
      stats.errors.push(line.trim());
    }
  }
  
  return stats;
}

async function getDbStats(): Promise<{ proposals: number; pha: number }> {
  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    return { proposals: 0, pha: 0 };
  }
  
  try {
    const pool = await sql.connect(connectionString);
    try {
      const proposalResult = await pool.request().query(`
        SELECT COUNT(*) AS Count FROM [etl].[stg_proposals]
      `);
      const phaResult = await pool.request().query(`
        SELECT COUNT(*) AS Count FROM [etl].[stg_policy_hierarchy_assignments]
      `);
      
      return {
        proposals: proposalResult.recordset[0].Count,
        pha: phaResult.recordset[0].Count
      };
    } finally {
      await pool.close();
    }
  } catch (e) {
    console.warn('Could not connect to database for stats');
    return { proposals: 0, pha: 0 };
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  const experiment = args.includes('--experiment')
    ? args[args.indexOf('--experiment') + 1]
    : undefined;
  
  if (!experiment) {
    console.error('ERROR: --experiment <name> is required');
    process.exit(1);
  }
  
  const logDir = path.join(process.cwd(), 'logs', experiment);
  
  if (!fs.existsSync(logDir)) {
    console.error(`ERROR: Log directory not found: ${logDir}`);
    process.exit(1);
  }
  
  console.log('='.repeat(70));
  console.log('PARALLEL RUNNER REPORT');
  console.log('='.repeat(70));
  console.log(`Experiment: ${experiment}`);
  console.log(`Log directory: ${logDir}`);
  console.log('');
  
  // Read master summary
  const summaryPath = path.join(logDir, 'summary.json');
  let summary: MasterSummary | null = null;
  
  if (fs.existsSync(summaryPath)) {
    summary = JSON.parse(fs.readFileSync(summaryPath, 'utf-8'));
    console.log('Execution Summary:');
    console.log(`  Total groups: ${summary?.totalGroups}`);
    console.log(`  Workers: ${summary?.numWorkers}`);
    console.log(`  Batch size: ${summary?.batchSize}`);
    console.log(`  Duration: ${summary?.elapsedSeconds}s`);
    console.log('');
  }
  
  // Parse individual worker logs
  const logFiles = fs.readdirSync(logDir).filter(f => f.match(/^runner-\d+\.log$/));
  
  console.log('Worker Results:');
  console.log('-'.repeat(70));
  
  let totalStats: LogStats = {
    proposalsCreated: 0,
    phaRecords: 0,
    certificatesProcessed: 0,
    regimesDetected: 0,
    outliersRouted: 0,
    errors: []
  };
  
  for (const logFile of logFiles.sort()) {
    const logPath = path.join(logDir, logFile);
    const stats = parseLogFile(logPath);
    
    const runnerId = logFile.match(/runner-(\d+)/)?.[1] || '?';
    const workerStatus = summary?.workers.find(w => w.runnerId === parseInt(runnerId, 10));
    const status = workerStatus?.status || 'unknown';
    
    console.log(`  Runner ${runnerId}: ${status}`);
    console.log(`    Certificates: ${stats.certificatesProcessed}, Proposals: ${stats.proposalsCreated}, PHA: ${stats.phaRecords}`);
    if (stats.errors.length > 0) {
      console.log(`    Errors: ${stats.errors.length}`);
    }
    
    // Aggregate
    totalStats.proposalsCreated += stats.proposalsCreated;
    totalStats.phaRecords += stats.phaRecords;
    totalStats.certificatesProcessed += stats.certificatesProcessed;
    totalStats.regimesDetected += stats.regimesDetected;
    totalStats.outliersRouted += stats.outliersRouted;
    totalStats.errors.push(...stats.errors);
  }
  
  console.log('');
  console.log('='.repeat(70));
  console.log('AGGREGATE TOTALS (from logs)');
  console.log('='.repeat(70));
  console.log(`Certificates processed: ${totalStats.certificatesProcessed.toLocaleString()}`);
  console.log(`Proposals created: ${totalStats.proposalsCreated.toLocaleString()}`);
  console.log(`PHA records: ${totalStats.phaRecords.toLocaleString()}`);
  console.log(`Regimes detected: ${totalStats.regimesDetected.toLocaleString()}`);
  console.log(`Outliers routed: ${totalStats.outliersRouted.toLocaleString()}`);
  console.log(`Errors: ${totalStats.errors.length}`);
  
  // Get actual DB stats
  console.log('');
  console.log('='.repeat(70));
  console.log('DATABASE TOTALS');
  console.log('='.repeat(70));
  const dbStats = await getDbStats();
  console.log(`Proposals in staging: ${dbStats.proposals.toLocaleString()}`);
  console.log(`PHA assignments in staging: ${dbStats.pha.toLocaleString()}`);
  
  if (dbStats.proposals > 0 || dbStats.pha > 0) {
    const total = dbStats.proposals + dbStats.pha;
    // Note: This is a rough estimate since proposals cover multiple certs
    console.log('');
    console.log('Estimated distribution:');
    console.log(`  Proposals: ${dbStats.proposals} (covering majority of certs)`);
    console.log(`  PHA: ${dbStats.pha} individual assignments`);
  }
  
  // Write report file
  const reportPath = path.join(logDir, 'report.json');
  const report = {
    experiment,
    generatedAt: new Date().toISOString(),
    execution: summary,
    aggregateStats: totalStats,
    databaseStats: dbStats
  };
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log('');
  console.log(`Report saved to: ${reportPath}`);
}

main().catch(err => {
  console.error('❌ Error:', err.message || err);
  process.exit(1);
});
