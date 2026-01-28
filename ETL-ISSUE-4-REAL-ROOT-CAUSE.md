# Issue 4: NULL ScheduleId - Real Root Cause Found

## User's Insight

The user pointed out that non-conformant certificates in raw data DO have valid schedules:

```sql
SELECT TOP 10 * 
FROM poc_etl.raw_certificate_info c 
INNER JOIN poc_etl.raw_schedule_rates s ON c.CommissionsSchedule = s.ScheduleName
WHERE GroupId LIKE '0%'
-- Returns matches! Schedule data exists!
```

## Previous Incorrect Diagnosis

**What I thought:** Schedule linking logic was broken, needed fallback matching strategies

**What I created:**
- Fallback matching by `ScheduleName LIKE`
- Partial matching on first 4 characters
- Complex audit script with multiple strategies

**Why it was wrong:** I was treating symptoms, not the root cause

## Actual Root Cause

**The raw schedule data was never copied to the ETL working schema:**

```
Source Data (poc_etl schema):     Transform Expects (etl schema):
- raw_schedule_rates: 1.1M rows → raw_schedule_rates: 0 rows ❌
- raw_certificate_info: 1.5M   → raw_certificate_info: 0 rows ❌  
- input_certificate_info: N/A  → input_certificate_info: 0 rows ❌
```

**Result:** Transform script `04-schedules.sql` found 0 schedules, created 0 staging records, and hierarchy linking had nothing to link to.

## Real Fix Applied

### 1. Copy Raw Data from poc_etl to etl (NEW)

**Created:** `sql/fix/copy-all-raw-from-poc-etl.sql`

```sql
INSERT INTO [etl].[raw_certificate_info] SELECT * FROM [poc_etl].[raw_certificate_info];
INSERT INTO [etl].[raw_schedule_rates] SELECT * FROM [poc_etl].[raw_schedule_rates];
-- + other raw tables
```

**Results:**
- 1,550,752 certificate records copied
- 1,133,420 schedule rates copied (51,073 unique schedules)
- 32,753 perf groups copied

### 2. Populate input_certificate_info

```sql
INSERT INTO [etl].[input_certificate_info] SELECT * FROM [etl].[raw_certificate_info];
```

**Result:** 1.5M rows populated

### 3. Use Permanent Tables Instead of Temp Tables

**Modified:** `sql/transforms/04-schedules.sql`

**Before (broken):**
```sql
DROP TABLE IF EXISTS #active_schedules;  -- Temp table
SELECT ... INTO #active_schedules ...
```

**After (fixed):**
```sql
DROP TABLE IF EXISTS [etl].[work_active_schedules];  -- Permanent work table
SELECT ... INTO [etl].[work_active_schedules] ...
```

**Why this matters:** Temp tables can have scope issues across GO batches in sqlcmd

### 4. Transform Success

After fixes:
- ✅ 688 active schedules identified
- ✅ 686 schedules created in staging
- ✅ 10,090 schedule rates populated

## Lessons Learned

1. **Always verify source data location** before diagnosing transform logic
2. **Check the full data pipeline** (raw → input → staging)
3. **Test queries outside the script** to isolate variable substitution vs logic issues
4. **Use permanent work tables** for better batch persistence in sqlcmd
5. **Listen to the user** - they knew the source data was good!

## Next Steps

Now that schedules exist in staging:
1. Re-run hierarchy transforms (07-hierarchies.sql)
2. Verify ScheduleId links successfully to stg_schedules.ExternalId
3. Check PolicyHierarchyAssignments get schedules linked
4. The original fix logic (multiple hierarchies per policy) will now work

## Files Modified

### Created:
- `sql/fix/copy-schedules-from-poc-etl.sql` - Initial schedule copy (superseded)
- `sql/fix/copy-all-raw-from-poc-etl.sql` - ✅ Comprehensive raw data copy

### Modified:
- `sql/transforms/04-schedules.sql` - Changed to permanent work tables

### Previously Created (Still Relevant):
- `tools/v5-etl/sql/transforms/99-audit-and-cleanup.sql` - Schedule fallback strategies (may not be needed now)
- Hierarchy consolidation fixes - Still needed for multiple PHA per policy
