/**
 * Parallel Runner - Orchestrates multiple v2.ts workers for parallel ETL processing
 * 
 * Usage:
 *   npx tsx scripts/new-builder/parallel-runner.ts --experiment "2026-02-03-10pct" --workers 4 --batch-size 100
 * 
 * Options:
 *   --experiment <name>   Name for this experiment (required)
 *   --workers <n>         Number of parallel workers (default: 4)
 *   --batch-size <n>      Groups per worker (default: auto-calculated)
 *   --spawn-delay <s>     Seconds to wait between spawning workers (default: 5)
 *   --verify-only         Run validation only, no database writes (fastest)
 *   --dry-run             Show what would be run without executing
 */

import { spawn, ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as sql from 'mssql';

interface WorkerStatus {
  runnerId: number;
  pid: number;
  offset: number;
  limit: number;
  status: 'running' | 'completed' | 'failed';
  startedAt: Date;
  completedAt?: Date;
  exitCode?: number;
}

async function getTotalGroups(): Promise<number> {
  const connectionString = process.env.SQLSERVER;
  if (!connectionString) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  const pool = await sql.connect(connectionString);
  try {
    const result = await pool.request().query(`
      SELECT COUNT(DISTINCT LTRIM(RTRIM(GroupId))) AS GroupCount
      FROM [etl].[input_certificate_info]
      WHERE CertStatus = 'A' AND RecStatus = 'A'
        AND LTRIM(RTRIM(GroupId)) <> ''
        AND LTRIM(RTRIM(GroupId)) IS NOT NULL
    `);
    return result.recordset[0].GroupCount;
  } finally {
    await pool.close();
  }
}

function spawnWorker(
  runnerId: number,
  offset: number,
  limit: number,
  experiment: string,
  logDir: string,
  verifyOnly: boolean = false
): ChildProcess {
  const logPath = path.join(logDir, `runner-${runnerId}.log`);
  const logStream = fs.createWriteStream(logPath);
  
  // Use validate mode if verify-only, otherwise transform
  const mode = verifyOnly ? 'validate' : 'transform';
  
  const args = [
    'scripts/new-builder/v2.ts',
    '--mode', mode,
    '--all',
    '--offset', String(offset),
    '--limit-groups', String(limit),
    '--runner-id', String(runnerId),
    '--experiment', experiment
  ];
  
  // Add --deep for more thorough validation
  if (verifyOnly) {
    args.push('--deep');
  }
  
  console.log(`[Runner ${runnerId}] Starting: mode=${mode}, offset=${offset}, limit=${limit}`);
  console.log(`[Runner ${runnerId}] Log: ${logPath}`);
  
  const child = spawn('npx', ['tsx', ...args], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env
  });
  
  child.stdout?.pipe(logStream);
  child.stderr?.pipe(logStream);
  
  return child;
}

async function main() {
  const args = process.argv.slice(2);
  
  const experiment = args.includes('--experiment')
    ? args[args.indexOf('--experiment') + 1]
    : undefined;
  
  const numWorkers = args.includes('--workers')
    ? Number.parseInt(args[args.indexOf('--workers') + 1], 10)
    : 4;
  
  const batchSizeArg = args.includes('--batch-size')
    ? Number.parseInt(args[args.indexOf('--batch-size') + 1], 10)
    : undefined;
  
  const dryRun = args.includes('--dry-run');
  const verifyOnly = args.includes('--verify-only');
  
  // Delay between spawning workers (in seconds) to avoid DB connection storms
  const spawnDelay = args.includes('--spawn-delay')
    ? Number.parseInt(args[args.indexOf('--spawn-delay') + 1], 10)
    : 5;
  
  if (!experiment) {
    console.error('ERROR: --experiment <name> is required');
    console.error('');
    console.error('Usage: npx tsx scripts/new-builder/parallel-runner.ts --experiment "2026-02-03-10pct" --workers 4');
    process.exit(1);
  }
  
  console.log('='.repeat(70));
  console.log('PARALLEL RUNNER');
  console.log('='.repeat(70));
  console.log(`Experiment: ${experiment}`);
  console.log(`Mode: ${verifyOnly ? 'VERIFY ONLY (no DB writes)' : 'TRANSFORM (with DB writes)'}`);
  console.log(`Workers: ${numWorkers}`);
  console.log(`Spawn delay: ${spawnDelay}s between workers`);
  console.log('');
  
  // Get total groups
  console.log('Counting total groups...');
  const totalGroups = await getTotalGroups();
  console.log(`Total groups: ${totalGroups}`);
  
  // Calculate batch size
  const batchSize = batchSizeArg || Math.ceil(totalGroups / numWorkers);
  console.log(`Batch size: ${batchSize} groups per worker`);
  console.log('');
  
  // Create log directory
  const logDir = path.join(process.cwd(), 'logs', experiment);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  console.log(`Log directory: ${logDir}`);
  console.log('');
  
  // Plan the workers
  const workers: { runnerId: number; offset: number; limit: number }[] = [];
  let offset = 0;
  let runnerId = 0;
  
  while (offset < totalGroups) {
    const limit = Math.min(batchSize, totalGroups - offset);
    workers.push({ runnerId, offset, limit });
    offset += batchSize;
    runnerId++;
  }
  
  console.log('Execution Plan:');
  for (const w of workers) {
    console.log(`  Runner ${w.runnerId}: groups ${w.offset} to ${w.offset + w.limit - 1} (${w.limit} groups)`);
  }
  console.log('');
  
  if (dryRun) {
    console.log('[DRY RUN] Would spawn the above workers. Exiting.');
    process.exit(0);
  }
  
  // Spawn workers with staggered start
  console.log(`Starting workers (${spawnDelay}s delay between spawns)...`);
  const startTime = Date.now();
  const statuses: WorkerStatus[] = [];
  const processes: ChildProcess[] = [];
  
  for (let i = 0; i < workers.length; i++) {
    const w = workers[i];
    
    // Add delay between spawns (except for the first worker)
    if (i > 0 && spawnDelay > 0) {
      console.log(`  Waiting ${spawnDelay}s before starting runner ${w.runnerId}...`);
      await new Promise(resolve => setTimeout(resolve, spawnDelay * 1000));
    }
    
    const child = spawnWorker(w.runnerId, w.offset, w.limit, experiment, logDir, verifyOnly);
    processes.push(child);
    
    const status: WorkerStatus = {
      runnerId: w.runnerId,
      pid: child.pid || 0,
      offset: w.offset,
      limit: w.limit,
      status: 'running',
      startedAt: new Date()
    };
    statuses.push(status);
    
    child.on('exit', (code) => {
      status.status = code === 0 ? 'completed' : 'failed';
      status.exitCode = code || 0;
      status.completedAt = new Date();
      console.log(`[Runner ${w.runnerId}] ${status.status} (exit code: ${code})`);
    });
  }
  
  // Wait for all workers to complete
  console.log('');
  console.log('Waiting for workers to complete...');
  
  await Promise.all(processes.map(p => new Promise<void>((resolve) => {
    p.on('exit', () => resolve());
  })));
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('');
  console.log('='.repeat(70));
  console.log('COMPLETED');
  console.log('='.repeat(70));
  console.log(`Total time: ${elapsed}s`);
  console.log(`Completed: ${statuses.filter(s => s.status === 'completed').length}/${statuses.length}`);
  console.log(`Failed: ${statuses.filter(s => s.status === 'failed').length}/${statuses.length}`);
  
  // Write master summary
  const summaryPath = path.join(logDir, 'summary.json');
  const summary = {
    experiment,
    totalGroups,
    numWorkers: workers.length,
    batchSize,
    startTime: new Date(startTime).toISOString(),
    endTime: new Date().toISOString(),
    elapsedSeconds: parseFloat(elapsed),
    workers: statuses
  };
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
  console.log(`\nSummary written to: ${summaryPath}`);
  
  // Exit with error if any workers failed
  const failedCount = statuses.filter(s => s.status === 'failed').length;
  if (failedCount > 0) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('❌ Error:', err.message || err);
  process.exit(1);
});
