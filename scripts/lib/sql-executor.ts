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
    const processedSQL = substituteSchemaVariables(scriptContent, options.config);
    
    // Replace debug mode variables if in debug mode
    const finalSQL = options.debugMode
      ? substituteDebugVariables(processedSQL, options.config)
      : processedSQL;
    
    if (options.debugMode) {
      console.log('\n🐛 DEBUG: Processed SQL (first 500 chars):');
      console.log(finalSQL.substring(0, 500) + '...\n');
    }
    
    // Execute SQL with retry logic for transient failures
    const result = await retryWithBackoff(
      () => options.pool.request().query(finalSQL),
      {
        maxRetries: 3,
        baseDelay: 1000,
        onRetry: (attempt, error) => {
          console.log(`    Retry attempt ${attempt} for ${path.basename(options.scriptPath)}`);
        }
      }
    );
    
    const duration = (Date.now() - startTime) / 1000;
    
    return {
      success: true,
      recordsAffected: result.rowsAffected?.[0],
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
