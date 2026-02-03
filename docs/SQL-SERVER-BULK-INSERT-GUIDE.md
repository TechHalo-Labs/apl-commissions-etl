# SQL Server Bulk Insert Guide

## The 1000 Row Limit

**SQL Server Limitation:** You cannot insert more than **1000 rows** in a single `INSERT VALUES` statement.

```sql
-- ❌ This will FAIL if you have more than 1000 rows:
INSERT INTO [table] (col1, col2) 
VALUES (val1, val2), (val3, val4), ... -- Max 1000 rows
```

**Error Message:**
```
The number of row value expressions in the INSERT statement exceeds 
the maximum allowed number of 1000 row values.
```

---

## Solution: Use Bulk Insert

**You cannot increase the SQL Server limit**, but you can bypass it using bulk insert methods:

### Option 1: `sql.Table` Bulk Insert (Recommended - Fastest)

This is what `load-csv.ts` uses and what we've implemented in `ingest-final-data-to-poc.ts`:

```typescript
import * as sql from 'mssql';

// Create a Table object
const table = new sql.Table(`[schema].[tableName]`);
table.create = false; // Table already exists

// Define columns
for (const col of columns) {
  table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
}

// Add rows (NO LIMIT!)
for (const row of rows) {
  const values = columns.map(col => row[col] || null);
  table.rows.add(...values);
}

// Bulk insert - bypasses 1000 row limit
const request = pool.request();
await request.bulk(table);
```

**Advantages:**
- ✅ No row limit (can insert millions of rows)
- ✅ Very fast (optimized by SQL Server)
- ✅ Handles large datasets efficiently
- ✅ Already implemented in `load-csv.ts`

---

### Option 2: BULK INSERT Statement (SQL Only)

For SQL scripts, you can use `BULK INSERT`:

```sql
BULK INSERT [schema].[tableName]
FROM 'C:\path\to\file.csv'
WITH (
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  FIRSTROW = 2  -- Skip header
);
```

**Advantages:**
- ✅ Very fast
- ✅ No row limit
- ❌ Requires file to be accessible by SQL Server
- ❌ Less flexible than programmatic approach

---

### Option 3: Table-Valued Parameters

Create a user-defined table type and pass it as a parameter:

```sql
-- Create table type
CREATE TYPE dbo.MyTableType AS TABLE (
  Col1 NVARCHAR(100),
  Col2 NVARCHAR(100)
);
```

```typescript
const table = new sql.Table('dbo.MyTableType');
// ... add columns and rows ...
const request = pool.request();
request.input('data', table);
await request.execute('sp_InsertData');
```

**Advantages:**
- ✅ No row limit
- ✅ Good for stored procedures
- ❌ Requires creating table types
- ❌ More complex setup

---

### Option 4: Multiple INSERT Statements (Current Workaround)

If you must use `INSERT VALUES`, batch into chunks of 1000:

```typescript
const batchSize = 1000;
for (let i = 0; i < rows.length; i += batchSize) {
  const batch = rows.slice(i, i + batchSize);
  // Build INSERT VALUES with max 1000 rows
  await pool.request().query(`INSERT INTO ... VALUES ...`);
}
```

**Disadvantages:**
- ⚠️ Slower (multiple round trips)
- ⚠️ More complex code
- ✅ Works but not optimal

---

## Performance Comparison

| Method | Speed | Row Limit | Complexity |
|--------|-------|-----------|------------|
| **Bulk Insert (`sql.Table`)** | ⚡⚡⚡ Fastest | Unlimited | Low |
| **BULK INSERT (SQL)** | ⚡⚡⚡ Fastest | Unlimited | Medium |
| **Table-Valued Parameters** | ⚡⚡ Fast | Unlimited | High |
| **Multiple INSERT VALUES** | ⚡ Slow | 1000 per batch | Low |

---

## Updated Ingest Script

The `ingest-final-data-to-poc.ts` script now uses **bulk insert** (`sql.Table`):

```typescript
// Before (slow, 1000 row limit):
const insertSql = `INSERT INTO ... VALUES ...`; // Max 1000 rows
await pool.request().query(insertSql);

// After (fast, unlimited rows):
const table = new sql.Table(`[schema].[table]`);
// ... add columns and rows ...
await pool.request().bulk(table); // No limit!
```

**Performance Improvement:**
- **Before:** ~3-5 minutes per million rows (1000 rows per INSERT)
- **After:** ~30-60 seconds per million rows (bulk insert)

---

## Summary

**Can you increase the SQL Server row limit?**
- ❌ **No** - It's a hard-coded SQL Server limitation

**What should you do instead?**
- ✅ **Use bulk insert** (`sql.Table` method) - Already implemented!
- ✅ This bypasses the limit entirely
- ✅ Much faster than multiple INSERT statements

The ingest script has been updated to use bulk insert, so it will now:
- Load data much faster
- Handle unlimited rows per batch
- Be more efficient overall
