# Production-Ready ETL Pipeline

## Overview

This ETL pipeline has been upgraded with production-ready features including state persistence, resume capability, schema flexibility, progress tracking, and comprehensive error handling.

## ✨ New Features

### 1. **State Persistence**
- All run and step progress tracked in database
- Full audit trail of execution history
- View run history with status, duration, and error messages

### 2. **Resume Capability**
- Automatically resume from last failed step
- No data loss - continue exactly where you left off
- Intelligent error classification determines if resume is safe

### 3. **Schema Flexibility**
- Configure schema names via `appsettings.json`
- Support for multiple environments (dev, staging, prod)
- Backward compatible with environment variables

### 4. **Progress Tracking**
- Real-time phase and step progress
- Records processed and processing rate
- Estimated completion time

### 5. **Debug Mode**
- Limit records per entity for fast testing
- Test single-record workflows
- Configurable record limits per table

### 6. **Backup & Restore**
- Quick backup of raw data schema
- Fast restore without re-ingesting CSV files
- Timestamped backup schemas

### 7. **Enhanced Error Handling**
- Automatic retry for transient failures (connection, timeout, deadlock)
- Error classification and helpful suggestions
- Formatted error output with recovery instructions

## 🚀 Quick Start

### Installation

```bash
cd apl-commissions-etl
npm install
```

### Configuration

1. Copy example configuration:
```bash
cp appsettings.example.json appsettings.json
```

2. Edit `appsettings.json` with your settings:
```json
{
  "database": {
    "connectionString": "Server=localhost;Database=mydb;User Id=sa;Password=***;TrustServerCertificate=True;Encrypt=True;",
    "schemas": {
      "source": "new_data",
      "transition": "raw_data",
      "processing": "etl",
      "production": "dbo"
    }
  },
  "debugMode": {
    "enabled": false,
    "maxRecords": {
      "brokers": 100,
      "groups": 50,
      "policies": 1000,
      "premiums": 5000
    }
  }
}
```

3. Or use environment variables (backward compatible):
```bash
export SQLSERVER="Server=localhost;Database=mydb;User Id=sa;Password=***;TrustServerCertificate=True;Encrypt=True;"
```

### Initialize State Management

First time only - create state tracking tables:

```bash
npx tsx scripts/run-pipeline.ts --skip-transform --skip-export
```

This will run schema setup including the new state management tables.

## 📖 Usage

### Run Full Pipeline

```bash
npx tsx scripts/run-pipeline.ts
```

### Run with Debug Mode

Test with limited records:

```bash
npx tsx scripts/run-pipeline.ts --debug
```

This limits processing to:
- 100 brokers
- 50 groups  
- 1,000 policies
- 5,000 premiums

### Resume from Failure

If a run fails, resume automatically:

```bash
npx tsx scripts/run-pipeline.ts --resume
```

Or resume from specific run:

```bash
npx tsx scripts/run-pipeline.ts --resume-from <run-id>
```

### Skip Phases

Run only specific phases:

```bash
# Transforms only
npx tsx scripts/run-pipeline.ts --transforms-only

# Export only
npx tsx scripts/run-pipeline.ts --export-only

# Skip specific phases
npx tsx scripts/run-pipeline.ts --skip-schema --skip-ingest
```

## 🔄 Backup & Restore

### Create Backup

Backup raw data for quick restore:

```bash
sqlcmd -S server -d database -i sql/backup-raw-data.sql
```

This creates a timestamped backup schema: `etl_backup_YYYYMMDD_HHMMSS`

### List Backups

```bash
sqlcmd -S server -d database -i sql/list-backups.sql
```

### Restore from Backup

Restore from most recent backup:

```bash
sqlcmd -S server -d database -i sql/restore-raw-data.sql
```

Or restore specific backup:

```bash
sqlcmd -S server -d database -v BACKUP_SCHEMA=etl_backup_20260127_143022 -i sql/restore-raw-data.sql
```

## 📊 Monitoring

### View Run History

```sql
SELECT 
  RunId,
  RunName,
  RunType,
  Status,
  CAST(ProgressPercent AS DECIMAL(5,1)) AS Progress,
  StartTime,
  EndTime,
  DATEDIFF(MINUTE, StartTime, COALESCE(EndTime, GETUTCDATE())) AS DurationMin
FROM [etl].[etl_run_state]
ORDER BY StartTime DESC;
```

### View Step Details

```sql
SELECT 
  s.StepNumber,
  s.ScriptName,
  s.Status,
  s.RecordsProcessed,
  s.DurationSeconds,
  s.ErrorMessage
FROM [etl].[etl_step_state] s
WHERE s.RunId = '<run-id>'
ORDER BY s.StepNumber;
```

### View Current Progress

```sql
SELECT TOP 1
  RunName,
  Status,
  CurrentPhase,
  CurrentStep,
  CONCAT(CompletedSteps, '/', TotalSteps) AS Steps,
  CAST(ProgressPercent AS DECIMAL(5,1)) AS Progress
FROM [etl].[etl_run_state]
WHERE Status = 'running'
ORDER BY StartTime DESC;
```

## 🏗️ Architecture

### State Management Tables

- `[etl].[etl_run_state]` - Run-level tracking
- `[etl].[etl_step_state]` - Step-level tracking

### TypeScript Modules

- `scripts/lib/config-loader.ts` - Configuration management
- `scripts/lib/state-manager.ts` - State persistence
- `scripts/lib/progress-reporter.ts` - Progress display
- `scripts/lib/sql-executor.ts` - SQL execution with schema substitution
- `scripts/lib/error-handler.ts` - Error classification and retry logic

### Schema Variables

All SQL scripts use configurable schema placeholders:

- `$(SOURCE_SCHEMA)` - Source data (default: new_data)
- `$(TRANSITION_SCHEMA)` - Transition/raw data (default: raw_data)
- `$(ETL_SCHEMA)` - Processing/staging (default: etl)
- `$(PRODUCTION_SCHEMA)` - Production tables (default: dbo)

These are substituted at runtime based on configuration.

## 🧪 Testing

### Test with Debug Mode

```bash
npx tsx scripts/run-pipeline.ts --debug --transforms-only
```

This runs transforms with limited records for fast validation.

### Test Resume Capability

1. Start a run:
```bash
npx tsx scripts/run-pipeline.ts
```

2. Kill it mid-execution (Ctrl+C)

3. Resume:
```bash
npx tsx scripts/run-pipeline.ts --resume
```

## 🔧 Troubleshooting

### Connection Errors

Connection errors automatically retry with exponential backoff (up to 3 attempts).

### Timeout Errors

If queries timeout, increase timeout in config:

```json
{
  "database": {
    "requestTimeout": 600000
  }
}
```

### Failed Run - Cannot Resume

If error classification determines run cannot be resumed (e.g., syntax error):

1. Fix the issue (SQL script or data)
2. Start a new run (don't use `--resume`)

### View Error Details

```sql
SELECT 
  RunId,
  RunName,
  Status,
  ErrorMessage,
  CanResume
FROM [etl].[etl_run_state]
WHERE Status = 'failed'
ORDER BY StartTime DESC;
```

## 📈 Performance

### Optimization Tips

1. **Use Backup/Restore**: Instead of re-ingesting CSV files, backup raw data after first ingest
2. **Skip Phases**: Use `--transforms-only` or `--export-only` when iterating
3. **Debug Mode**: Test changes with `--debug` before full run
4. **Resume**: Don't restart from scratch - use `--resume`

### Typical Performance

- Full pipeline: 30-45 minutes (3.6M+ records)
- Transforms only: 15-20 minutes
- Export only: 10-15 minutes
- Debug mode (100 brokers): 2-3 minutes

## 🔒 Security

- `appsettings.json` is excluded from git (.gitignore)
- Use environment variables in CI/CD
- Connection strings are masked in logs
- State management table stores config snapshot (without passwords)

## 📝 Migration from Legacy

The legacy `run-pipeline.ts` is backed up as `run-pipeline-legacy.ts`.

To migrate:

1. Create `appsettings.json` from `appsettings.example.json`
2. Update connection string
3. Run new pipeline with `--debug` to test
4. Run full pipeline

All environment variables still work for backward compatibility.

## 🆘 Support

For issues or questions:

1. Check run history in `[etl].[etl_run_state]`
2. Review step errors in `[etl].[etl_step_state]`
3. Check error classification in console output
4. Review stack trace in error message

## 🎯 Best Practices

1. **Always backup before major changes**: `sql/backup-raw-data.sql`
2. **Test with debug mode first**: `--debug`
3. **Use resume for long-running pipelines**: `--resume`
4. **Monitor progress**: Query state tables during execution
5. **Review error classifications**: Check if error allows resume
6. **Keep config in appsettings.json**: Don't hardcode connection strings

## 📚 Additional Documentation

- `docs/etl-architecture.md` - Detailed architecture
- `docs/data-migration-guide.md` - Data migration guide
- `sql/00a-state-management-tables.sql` - State management schema
