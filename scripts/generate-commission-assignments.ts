/**
 * Generate Commission Assignments from Staging Data
 * ==================================================
 * 
 * This script runs the optimized SQL transform for commission assignments.
 * Uses INSERT...SELECT for maximum performance (all processing in SQL Server).
 * 
 * Usage:
 *   npx tsx scripts/generate-commission-assignments.ts
 * 
 * Alternative: Run the SQL directly:
 *   sqlcmd -S server -d database -i sql/transforms/12-commission-assignments.sql
 */

import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { loadConfig, getSqlConfig } from './lib/config-loader';

async function main() {
  console.log('='.repeat(70));
  console.log('COMMISSION ASSIGNMENT GENERATOR (Optimized SQL)');
  console.log('='.repeat(70));
  console.log('');

  const config = loadConfig();
  const sqlConfig = getSqlConfig(config);
  const pool = await sql.connect(sqlConfig);

  try {
    // Load and execute the SQL script
    const sqlPath = path.join(__dirname, '../sql/transforms/12-commission-assignments.sql');
    const sqlContent = fs.readFileSync(sqlPath, 'utf8');
    
    // Split by GO statements and execute each batch
    const batches = sqlContent.split(/^GO\s*$/im).filter(b => b.trim());
    
    console.log(`Executing ${batches.length} SQL batch(es)...`);
    console.log('');
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i].trim();
      if (!batch) continue;
      
      const result = await pool.request().query(batch);
      
      // Extract and print any PRINT messages from the batch
      // (SQL Server returns these in the messages, but mssql doesn't expose them easily)
    }
    
    // Verify results
    console.log('');
    console.log('Verifying results...');
    
    const versionCount = await pool.request().query(
      'SELECT COUNT(*) as cnt FROM [etl].[stg_commission_assignment_versions]'
    );
    const recipientCount = await pool.request().query(
      'SELECT COUNT(*) as cnt FROM [etl].[stg_commission_assignment_recipients]'
    );
    
    console.log('');
    console.log('='.repeat(70));
    console.log('RESULTS');
    console.log('='.repeat(70));
    console.log(`Commission Assignment Versions: ${versionCount.recordset[0].cnt}`);
    console.log(`Commission Assignment Recipients: ${recipientCount.recordset[0].cnt}`);
    console.log('');
    console.log('✓ Commission assignments generated successfully!');

  } finally {
    await pool.close();
  }
}

main().catch(err => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});
