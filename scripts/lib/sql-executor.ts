import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { ETLConfig } from './config-loader';
import { retryWithBackoff } from './error-handler';

export interface SQLExecutionOptions {
  config: ETLConfig;
  pool: sql.ConnectionPool;
  scriptPath: string;
  stepId?: string; // For progress tracking
  debugMode?: boolean;
  pocMode?: boolean; // For POC schema isolation
}

export interface SQLExecutionResult {
  success: boolean;
  recordsAffected?: number;
  duration: number;
  error?: Error;
}

/**
 * Execute SQL script with schema variable substitution
 * Replaces $(SCHEMA_NAME) variables with actual schema names from config
 */
export async function executeSQLScript(options: SQLExecutionOptions): Promise<SQLExecutionResult> {
  const startTime = Date.now();
  
  try {
    // Read SQL script
    const scriptContent = fs.readFileSync(options.scriptPath, 'utf-8');
    
    // Replace schema variables
    let processedSQL = substituteSchemaVariables(scriptContent, options.config);
    
    // Replace hardcoded schema references if in POC mode
    if (options.pocMode) {
      if (options.debugMode) {
        console.log(`\n🔧 POC MODE ACTIVE:`);
        console.log(`   Processing Schema: ${options.config.database.schemas.processing}`);
        console.log(`   Production Schema: ${options.config.database.schemas.production}`);
      }
      processedSQL = substitutePOCSchemas(processedSQL, options.config);
    } else if (options.debugMode) {
      console.log(`\n⚠️  POC MODE: DISABLED (pocMode flag is false)`);
    }
    
    // Replace debug mode variables if in debug mode
    const finalSQL = options.debugMode
      ? substituteDebugVariables(processedSQL, options.config)
      : processedSQL;
    
    if (options.debugMode) {
      console.log('\n🐛 DEBUG: Processed SQL (first 500 chars):');
      console.log(finalSQL.substring(0, 500) + '...\n');
      
      // Show schema references in the SQL
      const schemaRefs = finalSQL.match(/\[(poc_etl|poc_dbo|etl|dbo)\]\./g) || [];
      const uniqueRefs = Array.from(new Set(schemaRefs));
      if (uniqueRefs.length > 0) {
        console.log(`   Schema references found: ${uniqueRefs.join(', ')}`);
      }
      
      // Save full SQL for inspection in POC mode
      const scriptName = path.basename(options.scriptPath);
      if (options.pocMode && (scriptName === '03-staging-tables.sql' || scriptName === '01-brokers.sql')) {
        const fs = require('fs');
        const outputPath = `/tmp/processed-${scriptName}`;
        fs.writeFileSync(outputPath, finalSQL);
        console.log(`   📝 Saved to ${outputPath}`);
        
        // Count what we're about to execute
        const createTables = (finalSQL.match(/CREATE TABLE/gi) || []).length;
        const insertIntos = (finalSQL.match(/INSERT INTO/gi) || []).length;
        console.log(`   🔍 About to execute: ${createTables} CREATE TABLE, ${insertIntos} INSERT INTO`);
      }
    }
    
    // Split SQL by GO batch separator (SQL Server requirement)
    const batches = finalSQL
      .split(/^\s*GO\s*$/gm)
      .map(batch => batch.trim())
      .filter(batch => batch.length > 0);
    
    if (options.debugMode && batches.length > 1) {
      console.log(`   📦 SQL split into ${batches.length} batches (GO separator handling)`);
    }
    
    // Execute SQL batches with retry logic for transient failures
    console.log(`   ⚡ Executing ${batches.length} SQL batch(es)...`);
    let totalRowsAffected = 0;
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      const result = await retryWithBackoff(
        () => options.pool.request().query(batch),
        {
          maxRetries: 3,
          baseDelay: 1000,
          onRetry: (attempt, error) => {
            console.log(`    Retry attempt ${attempt} for ${path.basename(options.scriptPath)} batch ${i+1}`);
          }
        }
      );
      
      if (result.rowsAffected?.[0]) {
        totalRowsAffected += result.rowsAffected[0];
      }
    }
    
    const duration = (Date.now() - startTime) / 1000;
    
    return {
      success: true,
      recordsAffected: totalRowsAffected,
      duration
    };
  } catch (error) {
    const duration = (Date.now() - startTime) / 1000;
    return {
      success: false,
      duration,
      error: error as Error
    };
  }
}

/**
 * Substitute schema variable placeholders with actual schema names
 */
export function substituteSchemaVariables(sql: string, config: ETLConfig): string {
  return sql
    .replace(/\$\(SOURCE_SCHEMA\)/g, config.database.schemas.source)
    .replace(/\$\(TRANSITION_SCHEMA\)/g, config.database.schemas.transition)
    .replace(/\$\(ETL_SCHEMA\)/g, config.database.schemas.processing)
    .replace(/\$\(PROCESSING_SCHEMA\)/g, config.database.schemas.processing)
    .replace(/\$\(PRODUCTION_SCHEMA\)/g, config.database.schemas.production);
}

/**
 * Substitute hardcoded schema references for POC mode
 * Aggressively replaces [etl] and [dbo] with POC schema names
 * Enhanced with comprehensive patterns for complete schema isolation
 */
export function substitutePOCSchemas(sql: string, config: ETLConfig): string {
  const processingSchema = config.database.schemas.processing;
  const productionSchema = config.database.schemas.production;
  
  return sql
    // Table operations with brackets
    .replace(/\[etl\]\./g, `[${processingSchema}].`)
    .replace(/\[dbo\]\./g, `[${productionSchema}].`)
    
    // Table operations without brackets  
    .replace(/FROM etl\./g, `FROM ${processingSchema}.`)
    .replace(/INTO etl\./g, `INTO ${processingSchema}.`)
    .replace(/JOIN etl\./g, `JOIN ${processingSchema}.`)
    .replace(/UPDATE etl\./g, `UPDATE ${processingSchema}.`)
    .replace(/FROM dbo\./g, `FROM ${productionSchema}.`)
    .replace(/INTO dbo\./g, `INTO ${productionSchema}.`)
    .replace(/JOIN dbo\./g, `JOIN ${productionSchema}.`)
    .replace(/UPDATE dbo\./g, `UPDATE ${productionSchema}.`)
    
    // Schema existence checks
    .replace(/IF EXISTS \(SELECT 1 FROM sys\.schemas WHERE name = 'etl'\)/g,
      `IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '${processingSchema}')`)
    .replace(/WHERE s\.name = 'etl'/g, `WHERE s.name = '${processingSchema}'`)
    .replace(/WHERE name = 'etl'/g, `WHERE name = '${processingSchema}'`)
    .replace(/WHERE schema_name\(\) = 'etl'/g, `WHERE schema_name() = '${processingSchema}'`)
    .replace(/schema_name\(\) = 'etl'/g, `schema_name() = '${processingSchema}'`)
    
    // Schema operations (DROP/CREATE) - Critical for POC isolation
    .replace(/DROP SCHEMA \[etl\]/g, `DROP SCHEMA [${processingSchema}]`)
    .replace(/CREATE SCHEMA \[etl\]/g, `CREATE SCHEMA [${processingSchema}]`)
    .replace(/DROP SCHEMA \[dbo\]/g, `DROP SCHEMA [${productionSchema}]`)
    .replace(/CREATE SCHEMA \[dbo\]/g, `CREATE SCHEMA [${productionSchema}]`)
    
    // Print statements and comments
    .replace(/\[etl\] schema/g, `[${processingSchema}] schema`)
    .replace(/'etl' schema/g, `'${processingSchema}' schema`)
    .replace(/in \[etl\]/g, `in [${processingSchema}]`)
    .replace(/in etl schema/g, `in ${processingSchema} schema`)
    
    // Dynamic SQL building patterns
    .replace(/'DROP TABLE IF EXISTS \[etl\]\.\['/g, `'DROP TABLE IF EXISTS [${processingSchema}].[' `)
    .replace(/\+ 'DROP TABLE IF EXISTS \[etl\]\.\['/g, `+ 'DROP TABLE IF EXISTS [${processingSchema}].[' `)
    
    // Column and variable references
    .replace(/= 'etl'/g, `= '${processingSchema}'`)
    .replace(/= N'etl'/g, `= N'${processingSchema}'`);
}

/**
 * Substitute debug mode variables
 */
export function substituteDebugVariables(sql: string, config: ETLConfig): string {
  if (!config.debugMode.enabled) {
    return sql;
  }
  
  const maxRecords = config.debugMode.maxRecords;
  
  return sql
    .replace(/\$\(DEBUG_MODE\)/g, '1')
    .replace(/\$\(MAX_BROKERS\)/g, maxRecords.brokers.toString())
    .replace(/\$\(MAX_GROUPS\)/g, maxRecords.groups.toString())
    .replace(/\$\(MAX_POLICIES\)/g, maxRecords.policies.toString())
    .replace(/\$\(MAX_PREMIUMS\)/g, maxRecords.premiums.toString())
    .replace(/\$\(MAX_HIERARCHIES\)/g, maxRecords.hierarchies.toString())
    .replace(/\$\(MAX_PROPOSALS\)/g, maxRecords.proposals.toString());
}

/**
 * Execute multiple SQL scripts in sequence
 */
export async function executeSQLScripts(
  scripts: string[],
  pool: sql.ConnectionPool,
  config: ETLConfig,
  onProgress?: (scriptName: string, index: number, total: number) => void
): Promise<SQLExecutionResult[]> {
  const results: SQLExecutionResult[] = [];
  
  for (let i = 0; i < scripts.length; i++) {
    const scriptPath = scripts[i];
    const scriptName = path.basename(scriptPath);
    
    if (onProgress) {
      onProgress(scriptName, i + 1, scripts.length);
    }
    
    const result = await executeSQLScript({
      config,
      pool,
      scriptPath,
      debugMode: config.debugMode.enabled
    });
    
    results.push(result);
    
    if (!result.success) {
      throw result.error;
    }
  }
  
  return results;
}

/**
 * Batch execute SQL statements (for dynamic SQL that can't use sqlcmd)
 */
export async function executeBatchSQL(
  pool: sql.ConnectionPool,
  sqlStatements: string[]
): Promise<void> {
  for (const stmt of sqlStatements) {
    if (stmt.trim()) {
      await pool.request().query(stmt);
    }
  }
}
