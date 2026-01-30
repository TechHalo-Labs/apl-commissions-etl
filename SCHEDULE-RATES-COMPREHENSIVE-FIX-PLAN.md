# 🔴 CRITICAL: Schedule Rates Comprehensive Fix Plan

**Date:** 2026-01-29  
**Status:** 🔴 CRITICAL MULTI-ISSUE PROBLEM  
**Priority:** IMMEDIATE FIX REQUIRED

---

## Executive Summary

Three critical, interconnected issues with schedule rates:

1. **Wrong Rate Values** - Production rates don't match source (e.g., 20.00 vs. 4.00, 5.00, 41.40)
2. **Missing Schedules** - Only active schedules imported (35-68% data loss per product)
3. **Schedule ID Coordination** - TypeScript ETL needs schedule IDs before creating hierarchies

**Root Cause:** Multi-layer aggregation/consolidation destroying rate granularity:
```
new_data.PerfScheduleModel (810 records, diverse rates) 
  → poc_etl.raw_schedule_rates (18 records, aggregated to 20.00) ❌ AGGREGATION LAYER 1
    → etl.raw_schedule_rates (18 records) 
      → etl.stg_schedule_rates (consolidation disabled, but data already wrong)
        → dbo.ScheduleRates (18 records, all 20.00) ❌
```

---

## Issue #1: Wrong Rate Values (CRITICAL)

### Example: GA508 LEVB / BIC Schedule / Arkansas

**Source (`new_data.PerfScheduleModel`):**
```
State | Level (Rate) | Count
AR    | 4.00         | 1
AR    | 5.00         | 1
AR    | 6.00         | 1
AR    | 7.00         | 1
AR    | 8.00         | 1
AR    | 9.00         | 1
AR    | 38.40        | 3
AR    | 39.40        | 3
AR    | 41.40        | 3
```

**Production (`ScheduleRates`):**
```
State | FirstYearRate | RenewalRate | Level
AR    | 20.00         | 20.00       | 20.00  ❌ WRONG!
```

### Root Cause

The data flow has TWO aggregation points:

**Aggregation Point 1:** `new_data.PerfScheduleModel` → `poc_etl.raw_schedule_rates`
- **Unknown script** consolidates 810 diverse records → 18 generic records
- Averages/selects single rate per (Schedule, Product, State)
- **This is where the rate diversity is lost!**

**Aggregation Point 2:** Step 6 consolidation (NOW DISABLED)
- Was consolidating state-specific rows → catch-all rows with State = NULL
- **Now fixed** - but data is already wrong from Point 1

### Impact

**Commission Calculation Breaks:**
```csharp
// Broker needs 41.40% rate for their tier
var rate = GetScheduleRate("GA508 LEVB", "AR", "BIC");
// Returns: 20.00% ❌ (should be 41.40%)

var commission = $1000 * 20.00% = $200  ❌ WRONG!
// Should be: $1000 * 41.40% = $414  ✅
```

**Impact:** Brokers are being under/overpaid based on incorrect rates!

---

## Issue #2: Missing Schedules (Data Loss)

### Current Behavior

The transform script (Step 2) filters schedules:
```sql
-- Only import schedules used by certificates
WHERE EXISTS (
  SELECT 1 FROM input_certificate_info 
  WHERE CommissionsSchedule = ScheduleName
)
```

**Effect:**
- GA508 LEVB: 45 unique schedules → 29 imported (35.6% loss)
- Unused schedules (BIC00, BIC01, BIC02, etc.) are excluded

### Why This Is A Problem

1. **Future Assignments** - Cannot assign certificates to these schedules
2. **Reference Data** - No complete schedule library
3. **Reporting** - Cannot show full rate card

### User Requirement

> "We need to copy all rates and we do not care if they are used"

**Solution:** Remove the filter, import ALL schedules from source.

---

## Issue #3: Schedule ID Coordination

### Current Problem

**TypeScript ETL (proposal-builder.ts):**
1. Generates hierarchies with `ScheduleCode` (string like "BIC", "HOME")
2. Creates split distributions with `ScheduleId` (needs numeric like 1, 2, 3)
3. **But:** Schedules don't exist yet! IDs are unknown.

**Current workaround:**
- `loadSchedules()` reads existing schedules from production
- If schedules haven't been imported yet → IDs not found

### User Requirement

> "We will need to first copy the schedules, have the schedules get their sequential ids, 
> then read in that schedule into memory when generating hierarchies"

### Correct Flow

```
1. INGEST: new_data.PerfScheduleModel → etl.raw_schedule_rates (ALL records)
2. TRANSFORM: etl.raw_schedule_rates → etl.stg_schedules + etl.stg_schedule_rates
3. EXPORT: etl.stg_* → dbo.Schedules + dbo.ScheduleRates (get sequential IDs)
4. TYPESCRIPT ETL: loadSchedules() reads dbo.Schedules (IDs now available)
5. TYPESCRIPT ETL: Creates hierarchies/split distributions with correct numeric IDs
```

---

## Comprehensive Fix Plan

### Phase 1: Fix Ingest Layer (CRITICAL)

**Problem:** Data aggregated in `poc_etl.raw_schedule_rates`

**Solution:** Create NEW ingest script that reads DIRECTLY from `new_data.PerfScheduleModel`

**New File:** `sql/ingest/01-ingest-schedules-from-source.sql`

```sql
-- Ingest schedules DIRECTLY from source (no aggregation)
TRUNCATE TABLE [etl].[raw_schedule_rates];

INSERT INTO [etl].[raw_schedule_rates] (
    ScheduleName, ScheduleDescription, Category, ProductCode,
    OffGroupLetterDescription, [State], GroupSizeFrom, GroupSizeTo, GroupSize,
    EffectiveStartDate, EffectiveEndDate, SeriesType, SpecialOffer,
    Year1, Year2, Year3, Year4, Year5, Year6, Year7, Year8, Year9, Year10,
    Year11, Year12, Year13, Year14, Year15, Year16, Year66, Year99, [Level]
)
SELECT 
    ScheduleName, ScheduleDescription, Category, ProductCode,
    OffGroupLetterDescription, [State], GroupSizeFrom, GroupSizeTo, GroupSize,
    EffectiveStartDate, EffectiveEndDate, SeriesType, SpecialOffer,
    Year1, Year2, Year3, Year4, Year5, Year6, Year7, Year8, Year9, Year10,
    Year11, Year12, Year13, Year14, Year15, Year16, Year66, Year99, [Level]
FROM [new_data].[PerfScheduleModel]
WHERE ProductCode IS NOT NULL AND ProductCode <> '';

-- Result: 100% of source data preserved (no aggregation!)
```

### Phase 2: Fix Transform Layer

**File:** `sql/transforms/04-schedules.sql`

**Changes:**

1. **Remove Active Schedule Filter (Step 2):**
```sql
-- OLD (Lines 69-70):
WHERE LTRIM(RTRIM(r.ScheduleName)) <> ''
  AND EXISTS (SELECT 1 FROM work_active_schedules WHERE ScheduleName = r.ScheduleName)

-- NEW:
WHERE LTRIM(RTRIM(r.ScheduleName)) <> ''
  AND LTRIM(RTRIM(r.ScheduleName)) IS NOT NULL
-- Import ALL schedules, not just active ones
```

2. **Keep Consolidation Disabled (Already Fixed in Step 6)**
- Already done ✅

### Phase 3: Export Schedules First

**Execution Order:**
```bash
# 1. Ingest from source (new script)
sqlcmd -i sql/ingest/01-ingest-schedules-from-source.sql

# 2. Transform schedules (fixed script)
sqlcmd -i sql/transforms/04-schedules.sql

# 3. Export schedules to production (gets sequential IDs)
sqlcmd -i sql/export/01-export-schedules.sql

# 4. NOW run TypeScript ETL (schedules available)
npx tsx scripts/proposal-builder.ts
```

### Phase 4: Update TypeScript ETL (Already Done ✅)

The `loadSchedules()` method we added for Issue #1 already handles this:
```typescript
async loadSchedules(pool: any): Promise<void> {
  const result = await pool.request().query(`
    SELECT Id, ExternalId FROM dbo.Schedules WHERE ExternalId IS NOT NULL
  `);
  for (const row of result.recordset) {
    this.scheduleIdByExternalId.set(row.ExternalId, row.Id);
  }
}
```

---

## Expected Results After Fix

### GA508 LEVB / BIC Schedule / Arkansas

**Current (WRONG):**
```
State | FirstYearRate | RenewalRate | Count
AR    | 20.00         | 20.00       | 1  ❌
```

**After Fix (CORRECT):**
```
State | FirstYearRate | RenewalRate | Count
AR    | 4.00          | 4.00        | 1  ✅
AR    | 5.00          | 5.00        | 1  ✅
AR    | 6.00          | 6.00        | 1  ✅
AR    | 7.00          | 7.00        | 1  ✅
AR    | 8.00          | 8.00        | 1  ✅
AR    | 9.00          | 9.00        | 1  ✅
AR    | 38.40         | 38.40       | 1  ✅
AR    | 39.40         | 39.40       | 3  ✅
AR    | 41.40         | 41.40       | 3  ✅
```

### System-Wide

| Metric | Before Fix | After Fix | Change |
|--------|------------|-----------|--------|
| **Total Schedule Rates** | 175,828 | ~500,000+ | **3x+ increase** |
| **GA508 LEVB Records** | 522 | 810 | **Full coverage** ✅ |
| **Rate Diversity** | Aggregated | Full granularity | **Accurate** ✅ |
| **Schedule Coverage** | 64% | 100% | **All schedules** ✅ |

---

## Implementation Steps

1. ✅ **Disable Step 6 consolidation** - DONE (Issue #2 fix)
2. ⏳ **Create ingest script** - Read DIRECTLY from `new_data.PerfScheduleModel`
3. ⏳ **Remove schedule filter** - Import ALL schedules (Step 2 of transform)
4. ⏳ **Test with GA508 LEVB** - Verify all 810 records imported correctly
5. ⏳ **Export to production** - Get sequential schedule IDs
6. ✅ **TypeScript ETL loadSchedules()** - DONE (Issue #1 fix)
7. ⏳ **Full ETL run** - Verify complete end-to-end flow

---

## Critical Questions

**Q1:** Where is `poc_etl.raw_schedule_rates` populated from?
- Need to find the script that aggregates `new_data.PerfScheduleModel` → `poc_etl.raw_schedule_rates`
- This is where rate diversity is being lost

**Q2:** Should we bypass `poc_etl` entirely?
- Option A: Fix `poc_etl` aggregation script
- Option B: Read directly from `new_data.PerfScheduleModel` → `etl.raw_schedule_rates` (RECOMMENDED)

**Q3:** What determines rate granularity?
- Multiple rates per (Schedule, Product, State, ???)
- Need to understand what makes each rate unique
- Likely: Level/Tier, GroupSize, EffectiveDate, or other dimensions

---

## Recommended Approach

**OPTION A: Quick Fix (Bypass poc_etl)**

```sql
-- New file: sql/ingest/01-ingest-schedules-from-source.sql
TRUNCATE TABLE [etl].[raw_schedule_rates];

INSERT INTO [etl].[raw_schedule_rates]
SELECT * FROM [new_data].[PerfScheduleModel]
WHERE ProductCode IS NOT NULL;

-- Result: Import ALL 810 records for GA508 LEVB (not just 18)
```

**OPTION B: Proper Fix (Fix poc_etl aggregation)**

Find and fix the script that populates `poc_etl.raw_schedule_rates`:
- Remove aggregation
- Preserve all rate records
- Maintain rate diversity

---

## Verification Queries

### After Fix - Verify Rate Diversity

```sql
-- Should show diverse rates (not all 20.00)
SELECT ProductCode, State, [Level], COUNT(*) as RecordCount
FROM ScheduleRates
WHERE ProductCode = 'GA508 LEVB'
  AND ScheduleName = 'BIC'
  AND State = 'AR'
GROUP BY ProductCode, State, [Level]
ORDER BY [Level];

-- Expected: Multiple rows with different Level values (4.00, 5.00, 9.00, 41.40, etc.)
```

### Verify All Schedules Imported

```sql
-- Should show 45 schedules (not 29)
SELECT COUNT(DISTINCT ScheduleName)
FROM raw_schedule_rates
WHERE ProductCode = 'GA508 LEVB';

-- Expected: 45 (all schedules from source)
```

---

## Next Steps

**Which approach do you prefer?**

**OPTION A: Quick Fix** (RECOMMENDED)
- Create new ingest script reading from `new_data.PerfScheduleModel`
- Bypass `poc_etl` entirely
- Fastest solution

**OPTION B: Thorough Fix**
- Find and fix `poc_etl` aggregation script
- Maintain `poc_etl` as intermediate layer
- More work, but keeps existing architecture

Please confirm approach, and I'll implement immediately.
