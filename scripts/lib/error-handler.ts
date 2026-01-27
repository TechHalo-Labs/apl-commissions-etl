/**
 * Error Handler for ETL Pipeline
 * Classifies errors and provides retry logic for transient failures
 */

export interface ErrorClassification {
  isTransient: boolean;
  isRecoverable: boolean;
  category: 'connection' | 'timeout' | 'deadlock' | 'constraint' | 'syntax' | 'unknown';
  message: string;
  suggestion: string;
}

/**
 * Classify an error to determine if it's transient and recoverable
 */
export function classifyError(error: any): ErrorClassification {
  const message = error.message || String(error);
  const code = error.code || error.number;
  
  // SQL Server connection errors (transient)
  if (
    code === 'ECONNRESET' ||
    code === 'ETIMEDOUT' ||
    code === 'ENOTFOUND' ||
    code === 'ECONNREFUSED' ||
    message.includes('Connection lost') ||
    message.includes('socket hang up')
  ) {
    return {
      isTransient: true,
      isRecoverable: true,
      category: 'connection',
      message: 'Database connection error',
      suggestion: 'Retrying with exponential backoff'
    };
  }
  
  // SQL Server timeout errors (transient)
  if (
    code === -2 || // SQL Server timeout
    code === 'ETIMEOUT' ||
    message.includes('Timeout') ||
    message.includes('timeout')
  ) {
    return {
      isTransient: true,
      isRecoverable: true,
      category: 'timeout',
      message: 'Query timeout',
      suggestion: 'Consider increasing requestTimeout or optimizing query'
    };
  }
  
  // SQL Server deadlock errors (transient)
  if (
    code === 1205 || // Deadlock victim
    message.includes('deadlock')
  ) {
    return {
      isTransient: true,
      isRecoverable: true,
      category: 'deadlock',
      message: 'Transaction deadlock detected',
      suggestion: 'Retrying transaction'
    };
  }
  
  // SQL Server constraint violations (not transient, but may be fixable)
  if (
    code === 547 || // Foreign key constraint
    code === 2627 || // Unique constraint
    code === 2601 || // Duplicate key
    message.includes('FOREIGN KEY constraint') ||
    message.includes('PRIMARY KEY constraint') ||
    message.includes('UNIQUE constraint')
  ) {
    return {
      isTransient: false,
      isRecoverable: true,
      category: 'constraint',
      message: 'Database constraint violation',
      suggestion: 'Check data integrity and fix source data'
    };
  }
  
  // SQL Server syntax errors (not transient, not recoverable without code change)
  if (
    code === 102 || // Syntax error
    code === 156 || // Incorrect syntax
    code === 208 || // Invalid object name
    message.includes('Incorrect syntax') ||
    message.includes('Invalid object name')
  ) {
    return {
      isTransient: false,
      isRecoverable: false,
      category: 'syntax',
      message: 'SQL syntax or schema error',
      suggestion: 'Fix SQL script or verify database schema'
    };
  }
  
  // Unknown errors
  return {
    isTransient: false,
    isRecoverable: true,
    category: 'unknown',
    message: message,
    suggestion: 'Review error details and logs'
  };
}

/**
 * Retry a function with exponential backoff
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: {
    maxRetries?: number;
    baseDelay?: number; // milliseconds
    maxDelay?: number; // milliseconds
    onRetry?: (attempt: number, error: any) => void;
  } = {}
): Promise<T> {
  const {
    maxRetries = 3,
    baseDelay = 1000,
    maxDelay = 30000,
    onRetry
  } = options;
  
  let lastError: any;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      
      const classification = classifyError(error);
      
      // Don't retry if error is not transient
      if (!classification.isTransient) {
        throw error;
      }
      
      // Don't retry on last attempt
      if (attempt === maxRetries) {
        throw error;
      }
      
      // Calculate delay with exponential backoff and jitter
      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay);
      const jitter = Math.random() * 0.3 * exponentialDelay; // ±30% jitter
      const delay = exponentialDelay + jitter;
      
      if (onRetry) {
        onRetry(attempt, error);
      }
      
      console.log(`  ⚠️  ${classification.message} (attempt ${attempt}/${maxRetries})`);
      console.log(`     ${classification.suggestion}`);
      console.log(`     Retrying in ${(delay / 1000).toFixed(1)}s...`);
      
      await sleep(delay);
    }
  }
  
  throw lastError;
}

/**
 * Execute with transaction wrapper and error handling
 */
export async function executeWithTransaction<T>(
  pool: any,
  fn: (transaction: any) => Promise<T>,
  options: {
    maxRetries?: number;
    onRetry?: (attempt: number, error: any) => void;
  } = {}
): Promise<T> {
  return retryWithBackoff(async () => {
    const transaction = pool.transaction();
    
    try {
      await transaction.begin();
      const result = await fn(transaction);
      await transaction.commit();
      return result;
    } catch (error) {
      try {
        await transaction.rollback();
      } catch (rollbackError) {
        console.error('  ⚠️  Failed to rollback transaction:', rollbackError);
      }
      throw error;
    }
  }, options);
}

/**
 * Sleep for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Format error for logging
 */
export function formatError(error: any): string {
  const classification = classifyError(error);
  
  let formatted = `\n╔════════════════════════════════════════════════════════════════╗\n`;
  formatted += `║  ERROR DETAILS                                                 ║\n`;
  formatted += `╚════════════════════════════════════════════════════════════════╝\n`;
  formatted += `  Category:    ${classification.category}\n`;
  formatted += `  Transient:   ${classification.isTransient ? 'Yes' : 'No'}\n`;
  formatted += `  Recoverable: ${classification.isRecoverable ? 'Yes' : 'No'}\n`;
  formatted += `  Message:     ${classification.message}\n`;
  formatted += `  Suggestion:  ${classification.suggestion}\n`;
  
  if (error.code) {
    formatted += `  Error Code:  ${error.code}\n`;
  }
  
  if (error.number) {
    formatted += `  SQL Number:  ${error.number}\n`;
  }
  
  if (error.lineNumber) {
    formatted += `  Line:        ${error.lineNumber}\n`;
  }
  
  if (error.procName) {
    formatted += `  Procedure:   ${error.procName}\n`;
  }
  
  formatted += `\n  Stack Trace:\n`;
  formatted += `  ${(error.stack || '').split('\n').join('\n  ')}\n`;
  
  return formatted;
}

/**
 * Check if error allows resume
 */
export function canResumeAfterError(error: any): boolean {
  const classification = classifyError(error);
  
  // Can resume if error is recoverable (not a syntax/schema error)
  return classification.isRecoverable;
}
