/**
 * Ingest Final Data CSV Files into poc_etl Schema
 * ================================================
 * 
 * Loads CSV files from ~/Downloads/final_data into poc_etl schema
 * with corrected column names and banking columns.
 * 
 * Usage:
 *   npx tsx scripts/ingest-final-data-to-poc.ts
 *   npx tsx scripts/ingest-final-data-to-poc.ts --limit 100  # Test mode
 *   npx tsx scripts/ingest-final-data-to-poc.ts --dry-run     # Preview only
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';
import { loadConfig, getSqlConfig } from './lib/config-loader';

const args = process.argv.slice(2);
const ROW_LIMIT = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1], 10) : 0;
const DRY_RUN = args.includes('--dry-run');
const RESUME = args.includes('--resume'); // Skip tables that already have data
const ONLY_TABLE = args.includes('--only') ? args[args.indexOf('--only') + 1] : null; // Run for a single table only
const TARGET_SCHEMA = 'poc_etl';
const CSV_DATA_PATH = '/Users/kennpalm/Downloads/final_data';

interface CsvMapping {
  csvFile: string;
  tableName: string;
  columnMapping?: { [csvCol: string]: string }; // Map CSV column to table column
}

const csvMappings: CsvMapping[] = [
  {
    csvFile: 'CertificateInfo_20260131.csv',
    tableName: 'raw_certificate_info'
  },
  {
    csvFile: 'APL-Perf_Schedule_model_20260131.csv',
    tableName: 'raw_schedule_rates'
  },
  {
    csvFile: 'APL-Perf_Group_model_20260131.csv',
    tableName: 'raw_perf_groups'
  },
  {
    csvFile: 'IndividualRosterExtract_20260131.csv',
    tableName: 'raw_individual_brokers'
  },
  {
    csvFile: 'OrganizationRosterExtract_20260131.csv',
    tableName: 'raw_org_brokers'
  },
  {
    csvFile: 'BrokerLicenseExtract_20260131.csv',
    tableName: 'raw_licenses'
  },
  {
    csvFile: 'BrokerEO_20260131.csv',
    tableName: 'raw_eo_insurance'
  },
  {
    csvFile: 'Fees_20260131.csv',
    tableName: 'raw_fees'
  },
  {
    csvFile: 'CommissionsDetail_20260116_20260131.csv',
    tableName: 'raw_commissions_detail',
    columnMapping: {
      'CreditCardType': 'CreditCardType' // Maps CSV CreditCardType to table CreditCardType (fixed typo)
    }
  }
];

function log(message: string, level: 'info' | 'success' | 'warn' | 'error' = 'info') {
  const prefix = {
    info: 'ℹ️ ',
    success: '✅',
    warn: '⚠️ ',
    error: '❌'
  }[level];
  console.log(`${prefix} ${message}`);
}

async function getCsvColumns(csvFile: string): Promise<string[]> {
  const filePath = path.join(CSV_DATA_PATH, csvFile);
  return new Promise((resolve, reject) => {
    const headers: string[] = [];
    fs.createReadStream(filePath)
      .pipe(parse({ columns: false, skip_empty_lines: true, from: 1, to: 1 }))
      .on('data', (row: string[]) => {
        // Remove BOM character if present
        headers.push(...row.map(h => h.replace(/^\uFEFF/, '').trim()));
        resolve(headers);
      })
      .on('error', reject);
  });
}

async function createTable(pool: sql.ConnectionPool, tableName: string, columns: string[], skipIfExists: boolean = false): Promise<boolean> {
  // Check if table exists and has data
  if (skipIfExists) {
    const existsCheck = await pool.request().query(`
      SELECT 
        CASE WHEN OBJECT_ID('[${TARGET_SCHEMA}].[${tableName}]', 'U') IS NOT NULL THEN 1 ELSE 0 END AS table_exists,
        (SELECT COUNT(*) FROM [${TARGET_SCHEMA}].[${tableName}]) AS row_count
    `);
    
    if (existsCheck.recordset[0].table_exists === 1) {
      const rowCount = existsCheck.recordset[0].row_count;
      if (rowCount > 0) {
        return false; // Table exists with data, skip creation
      }
      // Table exists but empty, drop it
      await pool.request().query(`
        DROP TABLE [${TARGET_SCHEMA}].[${tableName}];
      `);
    }
  } else {
    // Drop table if exists (non-resume mode)
    await pool.request().query(`
      IF OBJECT_ID('[${TARGET_SCHEMA}].[${tableName}]', 'U') IS NOT NULL
        DROP TABLE [${TARGET_SCHEMA}].[${tableName}];
    `);
  }

  // Create table with all NVARCHAR(MAX) columns (dynamic from CSV)
  // This ensures we capture all columns including banking columns
  const columnDefs = columns.map(col => `[${col}] NVARCHAR(MAX)`).join(', ');
  await pool.request().query(`
    CREATE TABLE [${TARGET_SCHEMA}].[${tableName}] (${columnDefs});
  `);
  
  return true; // Table was created
}

async function loadCsvToTable(
  pool: sql.ConnectionPool,
  csvFile: string,
  tableName: string,
  columns: string[],
  columnMapping?: { [csvCol: string]: string }
): Promise<number> {
  const filePath = path.join(CSV_DATA_PATH, csvFile);
  
  if (!fs.existsSync(filePath)) {
    log(`File not found: ${csvFile}`, 'warn');
    return 0;
  }

  const fileSize = (fs.statSync(filePath).size / 1024 / 1024).toFixed(2);
  log(`Loading ${csvFile} (${fileSize} MB) into [${TARGET_SCHEMA}].[${tableName}]...`);

  // Stream parse + bulk insert in batches (avoids loading entire CSV into memory)
  const batchSize = 5000;
  let totalSeen = 0;
  let totalInserted = 0;
  let headerColumns: string[] | null = null;

  const makeBulkTable = (cols: string[]) => {
    const table = new sql.Table(`[${TARGET_SCHEMA}].[${tableName}]`);
    table.create = false;
    for (const col of cols) {
      table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
    }
    return table;
  };

  const applyMapping = (record: any) => {
    if (!columnMapping) return record;
    const mapped: any = { ...record };
    for (const [csvCol, tableCol] of Object.entries(columnMapping)) {
      if (mapped[csvCol] !== undefined) {
        mapped[tableCol] = mapped[csvCol];
        delete mapped[csvCol];
      }
    }
    return mapped;
  };

  await new Promise<void>((resolve, reject) => {
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      bom: true
    });

    let currentTable: sql.Table | null = null;
    let currentBatchCount = 0;

    const flushBatch = async () => {
      if (!currentTable || currentBatchCount === 0) return;

      if (DRY_RUN) {
        totalInserted += currentBatchCount;
        currentTable = null;
        currentBatchCount = 0;
        return;
      }

      try {
        await pool.request().bulk(currentTable);
        totalInserted += currentBatchCount;
        currentTable = null;
        currentBatchCount = 0;
      } catch (err: any) {
        reject(err);
      }
    };

    // IMPORTANT: Do NOT use async 'readable' loops here (can re-enter and double-insert).
    // Instead, serialize processing with explicit backpressure.
    let chain = Promise.resolve();

    parser.on('data', (record) => {
      parser.pause();
      chain = chain.then(async () => {
        totalSeen++;
        if (ROW_LIMIT > 0 && totalSeen > ROW_LIMIT) {
          parser.end();
          return;
        }

        const mappedRecord = applyMapping(record);

        if (!headerColumns) {
          headerColumns = Object.keys(mappedRecord);
        }

        if (!currentTable) {
          currentTable = makeBulkTable(headerColumns);
          currentBatchCount = 0;
        }

        const values: (string | null)[] = [];
        for (const col of headerColumns) {
          const val = mappedRecord[col];
          values.push(val === null || val === undefined || val === '' ? null : String(val));
        }
        currentTable.rows.add(...values);
        currentBatchCount++;

        if (currentBatchCount >= batchSize) {
          await flushBatch();
          if (totalInserted % (batchSize * 10) === 0) {
            log(`  Inserted ${totalInserted.toLocaleString()} rows...`);
          }
        }
      }).then(() => {
        parser.resume();
      }).catch((err) => {
        reject(err);
      });
    });

    parser.on('end', () => {
      chain.then(async () => {
        await flushBatch();
        resolve();
      }).catch(reject);
    });

    parser.on('error', reject);

    fs.createReadStream(filePath).pipe(parser);
  });

  if (totalSeen === 0) {
    log(`No records found in ${csvFile}`, 'warn');
    return 0;
  }

  if (DRY_RUN) {
    log(`[DRY RUN] Would load ${totalSeen.toLocaleString()} rows`, 'info');
    return totalSeen;
  }

  log(`Loaded ${totalInserted.toLocaleString()} rows into [${TARGET_SCHEMA}].[${tableName}]`, 'success');
  return totalInserted;
}

async function main() {
  console.log('\n' + '═'.repeat(70));
  console.log('  CSV Ingest: final_data → poc_etl');
  console.log('═'.repeat(70));
  console.log(`Target Schema: ${TARGET_SCHEMA}`);
  console.log(`CSV Directory: ${CSV_DATA_PATH}`);
  if (ROW_LIMIT > 0) {
    console.log(`⚠️  ROW LIMIT: ${ROW_LIMIT} rows per file (test mode)`);
  }
  if (DRY_RUN) {
    console.log(`🔍 DRY RUN MODE - No changes will be made`);
  }
  if (RESUME) {
    console.log(`▶️  RESUME MODE - Will skip tables that already have data`);
  }
  if (ONLY_TABLE) {
    console.log(`🎯 ONLY MODE - Will process only table: ${ONLY_TABLE}`);
  }
  console.log('');

  const config = loadConfig();
  const pool = await sql.connect(getSqlConfig(config));

  try {
    // Ensure schema exists
    log('Ensuring schema exists...');
    await pool.request().query(`
      IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '${TARGET_SCHEMA}')
      BEGIN
        EXEC('CREATE SCHEMA [${TARGET_SCHEMA}]');
      END
    `);
    log(`Schema [${TARGET_SCHEMA}] ready`, 'success');

    const summary: { table: string; file: string; rows: number }[] = [];
    const startTime = Date.now();

    for (const mapping of csvMappings) {
      const { csvFile, tableName, columnMapping } = mapping;

      if (ONLY_TABLE && tableName !== ONLY_TABLE) {
        continue;
      }
      
      log(`\nProcessing: ${csvFile} → ${tableName}`);
      
      try {
        // Get CSV columns
        const csvColumns = await getCsvColumns(csvFile);
        
        if (csvColumns.length === 0) {
          log(`No columns found in ${csvFile}`, 'warn');
          continue;
        }

        // Check if table already has data (resume mode)
        if (RESUME && !DRY_RUN) {
          try {
            const existingCheck = await pool.request().query(`
              SELECT COUNT(*) as cnt FROM [${TARGET_SCHEMA}].[${tableName}]
            `);
            
            const existingCount = existingCheck.recordset[0]?.cnt || 0;
            if (existingCount > 0) {
              log(`⏭️  Skipping [${tableName}] - already has ${existingCount.toLocaleString()} rows`, 'info');
              summary.push({ table: tableName, file: csvFile, rows: existingCount });
              continue;
            }
          } catch (err: any) {
            // Table doesn't exist yet, continue with creation
          }
        }

        // Create table
        if (!DRY_RUN) {
          const created = await createTable(pool, tableName, csvColumns, RESUME);
          if (created) {
            log(`Created table [${TARGET_SCHEMA}].[${tableName}]`, 'success');
          } else {
            log(`Table [${TARGET_SCHEMA}].[${tableName}] already exists with data, skipping`, 'info');
            summary.push({ table: tableName, file: csvFile, rows: 0 });
            continue;
          }
        } else {
          log(`[DRY RUN] Would create table [${TARGET_SCHEMA}].[${tableName}]`, 'info');
        }

        // Load data
        const rowCount = await loadCsvToTable(pool, csvFile, tableName, csvColumns, columnMapping);
        summary.push({ table: tableName, file: csvFile, rows: rowCount });

      } catch (error: any) {
        log(`Failed to process ${csvFile}: ${error.message}`, 'error');
        console.error(error);
      }
    }

    // Summary
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n' + '═'.repeat(70));
    console.log('  INGEST SUMMARY');
    console.log('═'.repeat(70));
    console.table(summary);
    console.log(`Total rows loaded: ${summary.reduce((sum, s) => sum + s.rows, 0).toLocaleString()}`);
    console.log(`Duration: ${duration}s`);
    console.log('═'.repeat(70) + '\n');

    // Verify counts
    if (!DRY_RUN) {
      log('Verifying row counts...');
      const verifyQueries = summary.map(s => 
        `SELECT '${s.table}' as tbl, COUNT(*) as cnt FROM [${TARGET_SCHEMA}].[${s.table}]`
      );
      const result = await pool.request().query(verifyQueries.join(' UNION ALL ') + ' ORDER BY tbl');
      
      console.log('\nRow counts in database:');
      result.recordset.forEach((row: any) => {
        console.log(`  ${row.tbl}: ${parseInt(row.cnt).toLocaleString()} rows`);
      });
    }

  } catch (error) {
    log(`Fatal error: ${error}`, 'error');
    process.exit(1);
  } finally {
    await pool.close();
  }
}

main().catch(console.error);
