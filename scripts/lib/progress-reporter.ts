/**
 * Progress Reporter for ETL Pipeline
 * Provides formatted console output for tracking ETL progress
 */

export interface ProgressStats {
  processed: number;
  total: number;
  rate?: number; // records per second
  duration?: number; // seconds
}

export class ProgressReporter {
  private startTime: Date | null = null;
  private currentPhase: string | null = null;
  private currentStep: string | null = null;

  /**
   * Log the start of an ETL run
   */
  logRunStart(runName: string, runType: string, totalSteps: number): void {
    this.startTime = new Date();
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log(`║  ETL Pipeline Run Started                                      ║`);
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log(`  Run Name:    ${runName}`);
    console.log(`  Run Type:    ${runType}`);
    console.log(`  Total Steps: ${totalSteps}`);
    console.log(`  Started:     ${this.startTime.toISOString()}`);
    console.log('');
  }

  /**
   * Log the start of a phase
   */
  logPhase(phase: string, phaseNumber: number, totalPhases: number): void {
    this.currentPhase = phase;
    console.log('');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`📦 Phase ${phaseNumber}/${totalPhases}: ${phase}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('');
  }

  /**
   * Log the start of a step
   */
  logStep(
    step: string,
    currentStep: number,
    totalSteps: number,
    scriptName?: string
  ): void {
    this.currentStep = step;
    const percent = ((currentStep / totalSteps) * 100).toFixed(1);
    console.log(`  [${currentStep}/${totalSteps}] ${step} (${percent}%)`);
    if (scriptName) {
      console.log(`    Script: ${scriptName}`);
    }
  }

  /**
   * Log progress with record counts and rate
   */
  logRecords(stats: ProgressStats): void {
    const { processed, total, rate, duration } = stats;
    
    let progressLine = `    Processed: ${this.formatNumber(processed)}`;
    
    if (total > 0) {
      const percent = ((processed / total) * 100).toFixed(1);
      progressLine += ` / ${this.formatNumber(total)} (${percent}%)`;
    }
    
    if (rate) {
      progressLine += ` | ${this.formatNumber(rate)} rec/sec`;
    }
    
    if (duration) {
      progressLine += ` | ${this.formatDuration(duration)}`;
    }
    
    console.log(progressLine);
  }

  /**
   * Log step completion
   */
  logStepComplete(stepName: string, duration: number, recordsProcessed?: number): void {
    let message = `    ✅ ${stepName} completed`;
    
    if (recordsProcessed !== undefined) {
      message += ` (${this.formatNumber(recordsProcessed)} records)`;
    }
    
    message += ` in ${this.formatDuration(duration)}`;
    console.log(message);
    console.log('');
  }

  /**
   * Log step failure
   */
  logStepFailure(stepName: string, error: Error): void {
    console.log(`    ❌ ${stepName} FAILED`);
    console.log(`       Error: ${error.message}`);
    console.log('');
  }

  /**
   * Log phase completion
   */
  logPhaseComplete(phase: string, duration: number): void {
    console.log('');
    console.log(`✅ Phase "${phase}" completed in ${this.formatDuration(duration)}`);
    console.log('');
  }

  /**
   * Log run completion
   */
  logRunComplete(totalSteps: number, totalDuration: number): void {
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log(`║  ETL Pipeline Run Completed                                    ║`);
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log(`  Steps Completed: ${totalSteps}`);
    console.log(`  Total Duration:  ${this.formatDuration(totalDuration)}`);
    if (this.startTime) {
      console.log(`  Started:         ${this.startTime.toISOString()}`);
      console.log(`  Completed:       ${new Date().toISOString()}`);
    }
    console.log('');
  }

  /**
   * Log run failure
   */
  logRunFailure(error: Error, canResume: boolean): void {
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log(`║  ETL Pipeline Run FAILED                                       ║`);
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log(`  Error: ${error.message}`);
    console.log(`  Can Resume: ${canResume ? 'YES' : 'NO'}`);
    if (canResume) {
      console.log('');
      console.log('  To resume this run:');
      console.log('    npx tsx scripts/run-pipeline.ts --resume');
    }
    console.log('');
  }

  /**
   * Log resume operation
   */
  logResume(runName: string, fromStep: number, totalSteps: number): void {
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log(`║  Resuming ETL Pipeline Run                                     ║`);
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log(`  Run Name:        ${runName}`);
    console.log(`  Resuming from:   Step ${fromStep}/${totalSteps}`);
    console.log(`  Remaining steps: ${totalSteps - fromStep + 1}`);
    console.log('');
  }

  /**
   * Log warning message
   */
  logWarning(message: string): void {
    console.log(`  ⚠️  ${message}`);
  }

  /**
   * Log info message
   */
  logInfo(message: string): void {
    console.log(`  ℹ️  ${message}`);
  }

  /**
   * Log debug message (only if debug mode enabled)
   */
  logDebug(message: string, debugMode: boolean = false): void {
    if (debugMode) {
      console.log(`  🐛 DEBUG: ${message}`);
    }
  }

  /**
   * Format a number with thousand separators
   */
  private formatNumber(num: number): string {
    return num.toLocaleString('en-US');
  }

  /**
   * Format duration in human-readable format
   */
  private formatDuration(seconds: number): string {
    if (seconds < 60) {
      return `${seconds.toFixed(1)}s`;
    } else if (seconds < 3600) {
      const minutes = Math.floor(seconds / 60);
      const secs = seconds % 60;
      return `${minutes}m ${secs.toFixed(0)}s`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      return `${hours}h ${minutes}m`;
    }
  }

  /**
   * Clear current phase and step
   */
  reset(): void {
    this.startTime = null;
    this.currentPhase = null;
    this.currentStep = null;
  }
}
