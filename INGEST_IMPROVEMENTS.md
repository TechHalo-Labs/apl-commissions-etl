# Raw Data Ingest Improvements

## Overview

The new `ingest-raw-data.ts` script provides a comprehensive solution for ingesting raw CSV data from ZIP files into SQL Server.

## Features Implemented

### ✅ 1. ZIP File Support
- **User-specified**: Use `--zip <path>` to specify a ZIP file
- **Auto-detect**: Automatically finds the most recent ZIP file in `~/Downloads`

### ✅ 2. File Matching by Prefix
- Finds CSV files matching expected prefixes:
  - `brokers*` → `raw_brokers`
  - `CertificateInfo*` → `raw_certificate_info`
  - `perf*` → `raw_schedule_rates`
  - `premiums*` → `raw_premiums`
  - `CommissionsDetail*` → `raw_commissions_detail`
  - `licenses*` → `raw_broker_licenses`
  - `EO*` → `raw_broker_eo`

### ✅ 3. Column Header Validation
- Validates CSV headers against expected table structure
- Reports missing and extra columns
- Can be skipped with `--skip-validation`

### ✅ 4. Auto Schema Selection
- Automatically finds next available schema: `raw_data1`, `raw_data2`, etc.
- User can override with `--schema <name>`

### ✅ 5. Preview Mode
- `--preview`: Loads only 10 records per table for testing
- Perfect for validating transformations before full load

### ✅ 6. Dry Run Mode
- `--dry-run`: Shows what would be done without executing
- Useful for testing and validation

## Usage

### Basic Usage (Auto-detect ZIP, Auto-schema)
```bash
npx tsx scripts/ingest-raw-data.ts
```

### Specify ZIP File
```bash
npx tsx scripts/ingest-raw-data.ts --zip ~/Downloads/data-2026-01-19.zip
```

### Specify Schema
```bash
npx tsx scripts/ingest-raw-data.ts --schema raw_data5
```

### Preview Mode (10 records per table)
```bash
npx tsx scripts/ingest-raw-data.ts --preview
```

### Dry Run (see what would happen)
```bash
npx tsx scripts/ingest-raw-data.ts --dry-run
```

### Combined Options
```bash
# Preview with specific ZIP and schema
npx tsx scripts/ingest-raw-data.ts --zip ~/Downloads/data.zip --schema raw_data3 --preview

# Dry run first, then preview
npx tsx scripts/ingest-raw-data.ts --dry-run
npx tsx scripts/ingest-raw-data.ts --preview
```

## Testing Workflow

As recommended, test in this order:

1. **Dry Run** - See what would happen:
   ```bash
   npx tsx scripts/ingest-raw-data.ts --dry-run
   ```

2. **Preview Mode** - Load 10 records to test transformations:
   ```bash
   npx tsx scripts/ingest-raw-data.ts --preview
   ```

3. **Full Load** - Load all data:
   ```bash
   npx tsx scripts/ingest-raw-data.ts
   ```

## File Mappings

The script maps CSV file prefixes to table names:

| Prefix | Table Name | Key Columns |
|--------|------------|-------------|
| `brokers` | `raw_brokers` | BrokerId, Name, Status, Type |
| `CertificateInfo` | `raw_certificate_info` | Company, GroupId, Product, CertificateId, CertEffectiveDate, CertStatus, RecStatus |
| `perf` | `raw_schedule_rates` | ScheduleName, ProductCode, State, Level |
| `premiums` | `raw_premiums` | Company, GroupNumber, Policy, Product, Amount |
| `CommissionsDetail` | `raw_commissions_detail` | CertificateId, SplitBrokerId, PaidAmount, CommissionRate |
| `licenses` | `raw_broker_licenses` | BrokerId, State, LicenseNumber, EffectiveDate, ExpirationDate |
| `EO` | `raw_broker_eo` | BrokerId, PolicyNumber, Carrier, EffectiveDate, ExpirationDate |

## Schema Management

### Auto-Detection
The script automatically finds the next available schema:
- Checks for existing schemas: `raw_data1`, `raw_data2`, etc.
- Creates the next available schema (e.g., if `raw_data3` exists, creates `raw_data4`)

### Manual Override
Specify a schema name:
```bash
npx tsx scripts/ingest-raw-data.ts --schema my_custom_schema
```

## Column Validation

The script validates CSV headers against expected columns. If validation fails:
- Missing columns are reported
- Extra columns are reported
- Data still loads (with warnings)
- Use `--skip-validation` to skip validation entirely

## Next Steps (To Be Implemented)

### 🔲 Transform Script Schema Selection
- Update transform scripts to accept `--schema` parameter
- Auto-detect most recent schema (highest number) if not specified
- Update `run-pipeline.ts` to support schema selection

### 🔲 Enhanced Error Handling
- Better error messages for missing files
- Validation summary report
- Rollback on failure option

## Dependencies

New dependencies added:
- `yauzl`: ZIP file extraction
- `@types/yauzl`: TypeScript types

## Notes

- ZIP files are extracted to a temporary directory (`/tmp/etl-extract-*`)
- Temporary files are automatically cleaned up after processing
- All CSV columns are loaded as NVARCHAR (as per raw table schema)
- Batch inserts are performed in groups of 100 rows to avoid query size limits
