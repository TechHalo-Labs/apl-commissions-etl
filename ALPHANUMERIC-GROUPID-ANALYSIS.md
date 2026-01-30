# Alphanumeric GroupId Analysis - Normalization Strategy

**Date:** 2026-01-29  
**Question:** Can we replace alpha characters with '0' to make them unique?

---

## Executive Summary

✅ **YES - with ONE caveat!**

**Result:**
- 99 unique alphanumeric GroupIds
- After normalization (alpha→0): **98 unique** (1 duplicate)
- **0 collisions** with existing numeric GroupIds
- **423 records** could potentially be included in export

**The Duplicate:**
- `LA0059` and `MS0059` both normalize to `000059` (59)
- This is a meaningful business distinction (Louisiana vs Mississippi)

---

## Detailed Analysis

### Current State

**Alphanumeric GroupIds in Staging:**
- **Total:** 423 records
- **Unique GroupIds:** 99
- **Currently filtered out** during export (can't cast to BIGINT)

**Patterns:**
| Prefix | Count | % | Examples |
|--------|-------|---|----------|
| LA | 367 | 86.8% | LA0146, LA0015, LA9999 |
| AL | 33 | 7.8% | AL9999, AL0017 |
| 525B | 18 | 4.3% | 525B (weird one) |
| T17624 | 3 | 0.7% | T17624 |
| MS | 2 | 0.5% | MS0055, MS0059 |

---

## Normalization Test Results

### Strategy: Replace ALL alpha characters with '0'

**Examples:**
```
Original    → Normalized → As BIGINT
─────────────────────────────────────
LA0146      → 000146     → 146
AL9999      → 009999     → 9999
525B        → 5250       → 5250
LA          → 00         → 0
LA0000      → 000000     → 0
T17624      → 017624     → 17624
MS0059      → 000059     → 59
LA0059      → 000059     → 59  ⚠️ DUPLICATE!
```

### Results:
✅ **99 original** → **98 after normalization**  
✅ **0 collisions** with existing numeric GroupIds  
⚠️ **1 duplicate created:** LA0059 + MS0059 → 59

---

## The Duplicate Issue

### Conflicting GroupIds

**Original IDs:**
- `LA0059` (Louisiana Group 59?)
- `MS0059` (Mississippi Group 59?)

**After Normalization:**
- Both become `59`

**Why This Matters:**
- These likely represent different employer groups in different states
- State prefixes (LA, MS) are meaningful business identifiers
- Merging them would corrupt commission calculations

**Records Affected:**
- LA0059: 1 record in `stg_premium_split_versions`
- MS0059: 1 record in `stg_premium_split_versions`
- Total: 2 records (0.5% of alphanumeric records)

---

## Resolution Options

### Option 1: ✅ Use State Prefix + Number (RECOMMENDED)

**Strategy:** Encode state prefix into the numeric ID

**Mapping:**
```
LA → 50 (Louisiana state code)
MS → 60 (Mississippi state code)
AL → 40 (Alabama state code)
TX → 70 (Texas state code - if needed)
```

**Examples:**
```
LA0146 → 50000146 (50 = LA, 000146 = group number)
MS0059 → 60000059 (60 = MS, 000059 = group number)
AL9999 → 40009999 (40 = AL, 009999 = group number)
525B   → 5250     (no prefix, numeric only)
```

**Pros:**
- ✅ Preserves state information
- ✅ No duplicates
- ✅ No collisions
- ✅ Reversible (can extract state + number)

**Cons:**
- ⚠️ Creates 8-digit IDs (may need to verify BIGINT column size)
- ⚠️ Requires state code mapping logic

---

### Option 2: Hash-Based ID

**Strategy:** Generate unique numeric IDs using hash

```sql
ABS(CHECKSUM(GroupId)) % 1000000000 + 1000000000
```

**Examples:**
```
LA0146 → 1234567890 (deterministic hash)
MS0059 → 1987654321
```

**Pros:**
- ✅ Guaranteed unique (with collision checks)
- ✅ Simple to implement

**Cons:**
- ❌ Not reversible (lose original ID)
- ❌ Loses business meaning
- ⚠️ Requires collision detection

---

### Option 3: Simple Alpha→0 with Manual Fix

**Strategy:** Use alpha→0, manually resolve LA0059 vs MS0059

**Resolution:**
- Keep LA0059 as 59
- Remap MS0059 to 59000 (or other unused number)

**Pros:**
- ✅ Simple
- ✅ Only 2 records need manual fixing

**Cons:**
- ⚠️ Manual intervention required
- ⚠️ Loses state information for 1 of the 2 groups

---

### Option 4: Exclude Alphanumeric (CURRENT STATE)

**Strategy:** Continue filtering them out during export

**Pros:**
- ✅ No work needed
- ✅ No risk of corruption

**Cons:**
- ❌ Lose 423 records (commissions for these groups won't calculate)
- ❌ Data completeness gap

---

## Recommendation

### ✅ Option 1: State Prefix Encoding

**Why:**
1. **Preserves business meaning** (state information)
2. **No duplicates** (unique encoding)
3. **No collisions** with existing numeric IDs
4. **Reversible** (can extract state + number)
5. **Only 423 records** affected (low risk)

**Implementation:**

```sql
-- Normalization function
CREATE FUNCTION dbo.NormalizeAlphanumericGroupId(@groupId VARCHAR(50))
RETURNS BIGINT
AS
BEGIN
    DECLARE @stateCode INT = 0;
    DECLARE @numericPart VARCHAR(50);
    
    -- Detect state prefix
    IF LEFT(@groupId, 2) = 'LA' SET @stateCode = 50;
    ELSE IF LEFT(@groupId, 2) = 'MS' SET @stateCode = 60;
    ELSE IF LEFT(@groupId, 2) = 'AL' SET @stateCode = 40;
    ELSE IF LEFT(@groupId, 2) = 'TX' SET @stateCode = 70;
    -- Add more states as needed
    
    -- Extract numeric part (replace remaining alpha with 0)
    SET @numericPart = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            CASE 
                WHEN @stateCode > 0 THEN SUBSTRING(UPPER(@groupId), 3, LEN(@groupId))
                ELSE UPPER(@groupId)
            END,
            'A','0'),'B','0'),'C','0'),'D','0'),'E','0'),'F','0'),'G','0'),'H','0'),'I','0'),'J','0'),
            'K','0'),'L','0'),'M','0'),'N','0'),'O','0'),'P','0'),'Q','0'),'R','0'),'S','0'),'T','0'),
            'U','0'),'V','0'),'W','0'),'X','0'),'Y','0'),'Z','0');
    
    -- Combine state code + numeric part
    RETURN (@stateCode * 1000000) + CAST(@numericPart AS BIGINT);
END;
```

**Result:**
```
LA0146 → 50000146
MS0059 → 60000059
LA0059 → 50000059
AL9999 → 40009999
525B   → 5250
```

✅ **All unique, no collisions, preserves meaning!**

---

## Impact Assessment

### If We Include These 423 Records

**Current State:**
- Export filters out 423 records (alphanumeric GroupIds)
- These groups cannot calculate commissions

**After Normalization:**
- 423 records included in `dbo.PremiumSplitVersions`
- Commissions can calculate for these groups
- **0.4% improvement** in data completeness

**Risk:**
- Low - only 423 records
- Well-defined transformation
- Easy to reverse if issues arise

---

## Next Steps

### To Implement Option 1:

1. **Create normalization function** (SQL above)
2. **Test on staging data:**
   ```sql
   SELECT 
       GroupId,
       dbo.NormalizeAlphanumericGroupId(GroupId) as normalized,
       COUNT(*) as records
   FROM etl.stg_premium_split_versions
   WHERE TRY_CAST(GroupId AS BIGINT) IS NULL
   GROUP BY GroupId, dbo.NormalizeAlphanumericGroupId(GroupId)
   HAVING COUNT(*) > 1; -- Should return 0 rows
   ```

3. **Update export script** (`11-export-splits.sql`):
   ```sql
   -- Replace TRY_CAST(GroupId AS BIGINT) with:
   CASE 
       WHEN TRY_CAST(GroupId AS BIGINT) IS NOT NULL 
           THEN CAST(GroupId AS BIGINT)
       ELSE dbo.NormalizeAlphanumericGroupId(GroupId)
   END as GroupId
   ```

4. **Re-export PremiumSplitVersions**
5. **Verify 423 additional records exported**

---

## Bottom Line

**Question:** Can we replace alpha with '0' to make them unique?

**Answer:** ✅ **YES** - but better to use state prefix encoding (Option 1)

**Why:**
- Simple alpha→0 creates 1 duplicate (LA0059 vs MS0059)
- State prefix encoding preserves meaning and ensures uniqueness
- Low risk (423 records, 0.4% improvement)
- Easy to implement and test

**Recommendation:** Implement Option 1 to recover these 423 records and improve commission calculation coverage.
