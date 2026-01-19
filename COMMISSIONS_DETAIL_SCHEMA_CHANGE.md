# CommissionsDetail CSV Schema Change - January 2026

## Summary

The `CommissionsDetail_20260101_20260115.csv` file has a **completely different schema** than what the `raw_commissions_detail` table expects.

## Table Schema (What the table expects)

The `[etl].[raw_commissions_detail]` table was designed for the **old CSV format** with these columns:

| Column | Type | Purpose |
|--------|------|---------|
| `Company` | NVARCHAR(100) | Company identifier |
| `CertificateId` | NVARCHAR(100) | Certificate/policy ID |
| `CertEffectiveDate` | NVARCHAR(50) | Certificate effective date |
| `SplitBrokerId` | NVARCHAR(50) | Broker receiving split commission |
| `PmtPostedDate` | NVARCHAR(50) | Payment posted date |
| `PaidToDate` | NVARCHAR(50) | Paid through date |
| `PaidAmount` | NVARCHAR(50) | Amount paid |
| `TransActionType` | NVARCHAR(100) | Transaction type |
| `InvoiceNumber` | NVARCHAR(100) | Invoice number |
| `CertInForceMonths` | NVARCHAR(50) | Months certificate in force |
| `CommissionRate` | NVARCHAR(50) | Commission rate percentage |
| `RealCommissionRate` | NVARCHAR(50) | Actual commission rate |
| `PaidBrokerId` | NVARCHAR(50) | Broker who was paid |
| `CreaditCardType` | NVARCHAR(100) | Credit card type (typo in original) |
| `TransactionId` | NVARCHAR(100) | Transaction identifier |

**Total: 15 columns**

## Actual CSV Schema (What the file contains)

The `CommissionsDetail_20260101_20260115.csv` file contains these columns:

| Column | Type | Purpose |
|--------|------|---------|
| `Company` | string | Company identifier |
| `WritingBrokerId` | string | Broker who wrote the policy |
| `HierDriver` | string | Hierarchy driver/type |
| `HierVersion` | string | Hierarchy version identifier |
| `SplitBrokerSeq` | string | Split broker sequence number |
| `ContractEffectiveDate` | string | Contract effective date |
| `SplitBrokerId` | string | Broker receiving split commission |
| `ContractId` | string | Contract identifier |
| `CommissionsSchedule` | string | Commission schedule name |

**Total: 9 columns**

## Comparison

### Columns Removed (No longer in CSV)
- ❌ `CertificateId` - Certificate/policy ID
- ❌ `CertEffectiveDate` - Certificate effective date
- ❌ `PmtPostedDate` - Payment posted date
- ❌ `PaidToDate` - Paid through date
- ❌ `PaidAmount` - Amount paid
- ❌ `TransActionType` - Transaction type
- ❌ `InvoiceNumber` - Invoice number
- ❌ `CertInForceMonths` - Months certificate in force
- ❌ `CommissionRate` - Commission rate percentage
- ❌ `RealCommissionRate` - Actual commission rate
- ❌ `PaidBrokerId` - Broker who was paid
- ❌ `CreaditCardType` - Credit card type
- ❌ `TransactionId` - Transaction identifier

### Columns Added (New in CSV)
- ✅ `WritingBrokerId` - Broker who wrote the policy (replaces PaidBrokerId?)
- ✅ `HierDriver` - Hierarchy driver/type
- ✅ `HierVersion` - Hierarchy version identifier
- ✅ `SplitBrokerSeq` - Split broker sequence number
- ✅ `ContractEffectiveDate` - Contract effective date (replaces CertEffectiveDate?)
- ✅ `ContractId` - Contract identifier (replaces CertificateId?)
- ✅ `CommissionsSchedule` - Commission schedule name

### Columns Kept (In both)
- ✅ `Company` - Company identifier
- ✅ `SplitBrokerId` - Broker receiving split commission

## Impact

### Data Loss
The new CSV format **does not contain**:
- Payment transaction details (dates, amounts)
- Commission rate information
- Invoice numbers
- Transaction IDs

### New Data Available
The new CSV format **provides**:
- Hierarchy structure information (HierDriver, HierVersion)
- Contract-level information (ContractId, ContractEffectiveDate)
- Commission schedule references
- Split broker sequencing

## Possible Relationships

| Old Field | Possible New Equivalent | Notes |
|-----------|------------------------|-------|
| `CertificateId` | `ContractId` | Both identify the policy/contract |
| `CertEffectiveDate` | `ContractEffectiveDate` | Both are effective dates |
| `PaidBrokerId` | `WritingBrokerId` | Both identify a broker |
| `CommissionRate` | `CommissionsSchedule` | Schedule contains rate info |

## Recommendation

**Option 1: Keep old table schema, map new CSV columns**
- Map new CSV columns to old table columns where possible
- Leave unmapped columns as NULL
- Requires transform logic to handle mapping

**Option 2: Create new table for new schema**
- Create `raw_commissions_detail_v2` with new schema
- Keep old table for legacy data
- Update transforms to use appropriate table

**Option 3: Update table schema to match new CSV**
- Modify `raw_commissions_detail` to match new CSV
- Update all transform scripts that use this table
- May break existing transforms

## Related Files

- **CommHierarchy_20260116.csv** - Contains similar hierarchy data:
  - Company, WritingBrokerId, HierDriver, HierVersion, SplitBrokerSeq, ContractEffectiveDate, SplitBrokerId, ContractId, CommissionsSchedule
  
  **Note:** CommHierarchy has the same columns as CommissionsDetail! They may be duplicates or one may be a subset of the other.

## Questions to Resolve

1. Is `CommissionsDetail` the same as `CommHierarchy`?
2. Where did the payment/transaction data go?
3. Is this a permanent schema change or temporary?
4. Should we process both files or just one?
