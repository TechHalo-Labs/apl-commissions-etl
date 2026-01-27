import * as fs from 'fs';
import * as path from 'path';
import * as sql from 'mssql';

export interface ETLConfig {
  database: {
    connectionString: string;
    schemas: {
      source: string;      // 'new_data'
      transition: string;  // 'raw_data'
      processing: string;  // 'etl'
      production: string;  // 'dbo'
    };
  };
  inputFiles: {
    premiums: string;
    certificateInfo: string;
    commissionsDetail: string;
    individualBrokers: string;
    orgBrokers: string;
    licenses: string;
    eo: string;
    scheduleRates: string;
    perfGroups: string;
    fees: string;
  };
  debugMode: {
    enabled: boolean;
    maxRecords: {
      brokers: number;
      groups: number;
      policies: number;
      premiums: number;
      hierarchies: number;
      proposals: number;
    };
  };
  resume: {
    enabled: boolean;
    resumeFromRunId: string | null;
  };
}

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
 * Load ETL configuration from appsettings.json and environment variables
 * 
 * Priority:
 * 1. Command-line overrides (passed as parameter)
 * 2. Environment variables (backward compatible)
 * 3. appsettings.json
 * 4. Default values
 * 
 * @param overrides Optional partial config to override loaded settings
 * @returns Complete ETL configuration
 */
export function loadConfig(overrides?: Partial<ETLConfig>): ETLConfig {
  // Try to load appsettings.json
  const configPath = path.join(process.cwd(), 'appsettings.json');
  let fileConfig: Partial<ETLConfig> = {};
  
  if (fs.existsSync(configPath)) {
    try {
      const fileContent = fs.readFileSync(configPath, 'utf-8');
      fileConfig = JSON.parse(fileContent);
    } catch (error) {
      console.warn(`⚠️  Warning: Failed to parse appsettings.json: ${error}`);
    }
  }

  // Build connection string from environment variables (backward compatible)
  let connectionString = process.env.SQLSERVER || '';
  
  if (!connectionString && (process.env.SQLSERVER_HOST || process.env.SQLSERVER_DATABASE)) {
    const server = process.env.SQLSERVER_HOST;
    const database = process.env.SQLSERVER_DATABASE;
    const user = process.env.SQLSERVER_USER;
    const password = process.env.SQLSERVER_PASSWORD;
    
    if (server && database && user && password) {
      connectionString = `Server=${server};Database=${database};User Id=${user};Password=${password};TrustServerCertificate=True;Encrypt=True;`;
    }
  }

  // Build final config with priority: overrides > env vars > file config > defaults
  const config: ETLConfig = {
    database: {
      connectionString: connectionString || fileConfig.database?.connectionString || '',
      schemas: {
        source: process.env.SOURCE_SCHEMA || fileConfig.database?.schemas?.source || 'new_data',
        transition: process.env.TRANSITION_SCHEMA || fileConfig.database?.schemas?.transition || 'raw_data',
        processing: process.env.PROCESSING_SCHEMA || fileConfig.database?.schemas?.processing || 'etl',
        production: process.env.PRODUCTION_SCHEMA || fileConfig.database?.schemas?.production || 'dbo',
      }
    },
    inputFiles: {
      premiums: process.env.INPUT_PREMIUMS || fileConfig.inputFiles?.premiums || '',
      certificateInfo: process.env.INPUT_CERTIFICATE_INFO || fileConfig.inputFiles?.certificateInfo || '',
      commissionsDetail: process.env.INPUT_COMMISSIONS_DETAIL || fileConfig.inputFiles?.commissionsDetail || '',
      individualBrokers: process.env.INPUT_INDIVIDUAL_BROKERS || fileConfig.inputFiles?.individualBrokers || '',
      orgBrokers: process.env.INPUT_ORG_BROKERS || fileConfig.inputFiles?.orgBrokers || '',
      licenses: process.env.INPUT_LICENSES || fileConfig.inputFiles?.licenses || '',
      eo: process.env.INPUT_EO || fileConfig.inputFiles?.eo || '',
      scheduleRates: process.env.INPUT_SCHEDULE_RATES || fileConfig.inputFiles?.scheduleRates || '',
      perfGroups: process.env.INPUT_PERF_GROUPS || fileConfig.inputFiles?.perfGroups || '',
      fees: process.env.INPUT_FEES || fileConfig.inputFiles?.fees || '',
    },
    debugMode: {
      enabled: process.env.DEBUG_MODE === 'true' || fileConfig.debugMode?.enabled || false,
      maxRecords: {
        brokers: parseInt(process.env.MAX_BROKERS || '') || fileConfig.debugMode?.maxRecords?.brokers || 100,
        groups: parseInt(process.env.MAX_GROUPS || '') || fileConfig.debugMode?.maxRecords?.groups || 50,
        policies: parseInt(process.env.MAX_POLICIES || '') || fileConfig.debugMode?.maxRecords?.policies || 1000,
        premiums: parseInt(process.env.MAX_PREMIUMS || '') || fileConfig.debugMode?.maxRecords?.premiums || 5000,
        hierarchies: parseInt(process.env.MAX_HIERARCHIES || '') || fileConfig.debugMode?.maxRecords?.hierarchies || 100,
        proposals: parseInt(process.env.MAX_PROPOSALS || '') || fileConfig.debugMode?.maxRecords?.proposals || 50,
      }
    },
    resume: {
      enabled: process.env.RESUME_ENABLED === 'true' || fileConfig.resume?.enabled || false,
      resumeFromRunId: process.env.RESUME_FROM_RUN_ID || fileConfig.resume?.resumeFromRunId || null,
    }
  };

  // Apply overrides
  if (overrides) {
    if (overrides.database?.connectionString) {
      config.database.connectionString = overrides.database.connectionString;
    }
    if (overrides.database?.schemas) {
      Object.assign(config.database.schemas, overrides.database.schemas);
    }
    if (overrides.inputFiles) {
      Object.assign(config.inputFiles, overrides.inputFiles);
    }
    if (overrides.debugMode) {
      config.debugMode.enabled = overrides.debugMode.enabled ?? config.debugMode.enabled;
      if (overrides.debugMode.maxRecords) {
        Object.assign(config.debugMode.maxRecords, overrides.debugMode.maxRecords);
      }
    }
    if (overrides.resume) {
      config.resume.enabled = overrides.resume.enabled ?? config.resume.enabled;
      config.resume.resumeFromRunId = overrides.resume.resumeFromRunId ?? config.resume.resumeFromRunId;
    }
  }

  return config;
}

/**
 * Convert ETL config to mssql config
 */
export function getSqlConfig(config: ETLConfig): sql.config {
  if (!config.database.connectionString) {
    throw new Error('Database connection string is required');
  }

  const parsed = parseConnectionString(config.database.connectionString);
  
  if (!parsed.server || !parsed.database || !parsed.user || !parsed.password) {
    throw new Error('Invalid connection string. Expected format: Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;Encrypt=True;');
  }

  return {
    server: parsed.server,
    database: parsed.database,
    user: parsed.user,
    password: parsed.password,
    options: {
      encrypt: parsed.options?.encrypt ?? true,
      trustServerCertificate: parsed.options?.trustServerCertificate ?? true,
    },
    requestTimeout: 300000,
    connectionTimeout: 30000,
  };
}

/**
 * Validate configuration
 */
export function validateConfig(config: ETLConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!config.database.connectionString) {
    errors.push('Database connection string is required');
  }

  // Validate schemas are not empty
  const schemas = config.database.schemas;
  if (!schemas.source) errors.push('Source schema name is required');
  if (!schemas.transition) errors.push('Transition schema name is required');
  if (!schemas.processing) errors.push('Processing schema name is required');
  if (!schemas.production) errors.push('Production schema name is required');

  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Print configuration (for debugging, masks sensitive data)
 */
export function printConfig(config: ETLConfig): void {
  const maskedConfig = JSON.parse(JSON.stringify(config));
  
  // Mask password in connection string
  if (maskedConfig.database.connectionString) {
    maskedConfig.database.connectionString = maskedConfig.database.connectionString
      .replace(/Password=[^;]+/i, 'Password=***');
  }

  console.log('\n📋 ETL Configuration:');
  console.log('════════════════════════════════════════════════════════════════');
  console.log(JSON.stringify(maskedConfig, null, 2));
  console.log('════════════════════════════════════════════════════════════════\n');
}
