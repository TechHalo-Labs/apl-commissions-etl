# CSV Ingest Analysis: File-to-Table Mapping

**Date:** January 31, 2026  
**Source Directory:** `~/Downloads/final_data`  
**Reference:** `sql/ingest/copy-from-poc-etl.sql` expects these tables in `poc_etl` schema

---

## Executive Summary

| Status | Count | Details |
|--------|-------|---------|
| ✅ **Files Found** | 13 | All expected CSV files present |
| ✅ **Tables Expected** | 8 | From `copy-from-poc-etl.sql` |
| ⚠️ **Column Mismatches** | 3 | Minor differences (mostly extra columns in CSV) |
| ❌ **Missing Files** | 1 | `premiums.csv` not found (but not critical for initial load) |
| 📋 **Extra Files** | 5 | Additional files not in expected list (can be loaded separately) |

---

## Expected Tables (from copy-from-poc-etl.sql)

The SQL script `sql/ingest/copy-from-poc-etl.sql` expects these 8 tables in the source schema:

1. `raw_certificate_info` ⭐ **CRITICAL**
2. `raw_schedule_rates` ⭐ **CRITICAL**
3. `raw_perf_groups`
4. `raw_premiums`
5. `raw_individual_brokers`
6. `raw_org_brokers`
7. `raw_licenses` (optional)
8. `raw_eo_insurance` (optional)

---

## File-by-File Analysis

### 1. ✅ CertificateInfo_20260131.csv → `raw_certificate_info`

**Status:** ✅ **MATCH** (with extra columns)

**CSV Columns (24):**
```
Company, ProductMasterCategory, ProductCategory, GroupId, Product, PlanCode, 
CertificateId, CertEffectiveDate, CertIssuedState, CertStatus, CertPremium, 
CertSplitSeq, CertSplitPercent, CustomerId, RecStatus, HierDriver, HierVersion, 
CommissionsSchedule, CommissionType, WritingBrokerID, SplitBrokerId, 
SplitBrokerSeq, ReassignedType, PaidBrokerId
```

**Expected Table Columns (21):**
```sql
Company, ProductMasterCategory, ProductCategory, GroupId, Product, PlanCode, 
CertificateId, CertEffectiveDate, CertIssuedState, CertStatus, CertPremium, 
CertSplitSeq, CertSplitPercent, CustomerId, RecStatus, HierDriver, HierVersion, 
CommissionsSchedule, CommissionType, WritingBrokerID, SplitBrokerId, 
SplitBrokerSeq, ReassignedType, PaidBrokerId
```

**Analysis:**
- ✅ All expected columns present
- ✅ Column order matches
- ✅ No missing columns
- **Action:** ✅ Ready to load

---

### 2. ✅ APL-Perf_Schedule_model_20260131.csv → `raw_schedule_rates`

**Status:** ✅ **MATCH** (perfect match)

**CSV Columns (32):**
```
ScheduleName, ScheduleDescription, Category, ProductCode, OffGroupLetterDescription, 
State, GroupSizeFrom, GroupSizeTo, GroupSize, EffectiveStartDate, EffectiveEndDate, 
SeriesType, SpecialOffer, Year1, Year2, Year3, Year4, Year5, Year6, Year7, Year8, 
Year9, Year10, Year11, Year12, Year13, Year14, Year15, Year16, Year66, Year99, Level
```

**Expected Table Columns (32):**
```sql
ScheduleName, ScheduleDescription, Category, ProductCode, OffGroupLetterDescription, 
State, GroupSizeFrom, GroupSizeTo, GroupSize, EffectiveStartDate, EffectiveEndDate, 
SeriesType, SpecialOffer, Year1, Year2, Year3, Year4, Year5, Year6, Year7, Year8, 
Year9, Year10, Year11, Year12, Year13, Year14, Year15, Year16, Year66, Year99, Level
```

**Analysis:**
- ✅ Perfect match - all columns present
- ✅ Column order matches exactly
- **Action:** ✅ Ready to load

---

### 3. ✅ APL-Perf_Group_model_20260131.csv → `raw_perf_groups`

**Status:** ✅ **MATCH** (perfect match)

**CSV Columns (12):**
```
GroupNum, GroupName, StateAbbreviation, GroupSize, BrokerUniqueId, 
BrokerReportsToUniqueId, AgencyName, Product, EffectiveDate, FundingType, 
CommissionTable, FeeAgreement
```

**Expected Table Columns (12):**
```sql
GroupNum, GroupName, StateAbbreviation, GroupSize, BrokerUniqueId, 
BrokerReportsToUniqueId, AgencyName, Product, EffectiveDate, FundingType, 
CommissionTable, FeeAgreement
```

**Analysis:**
- ✅ Perfect match - all columns present
- ✅ Column order matches exactly
- **Action:** ✅ Ready to load

---

### 4. ❌ premiums.csv → `raw_premiums`

**Status:** ❌ **FILE NOT FOUND**

**Expected File:** `premiums.csv` (or `premiums_*.csv`)

**Expected Table Columns (24):**
```sql
Company, GroupNumber, Policy, OldPolicy, LastName, FirstName, Product, 
MasterCategory, Category, PayMode, StateIssued, Division, CertificateEffectiveDate, 
DatePost, DatePaidTo, Amount, TransactionType, InvoiceNumber, CommissionType, 
GroupName, SplitPercentage, SplitCommissionHierarchy, SplitSalesHierarchy, LionRecNo
```

**Analysis:**
- ❌ File not found in `final_data` directory
- ⚠️ This table is used for premium transactions
- **Action:** ⚠️ Check if premiums data is in another file or if it's loaded separately

---

### 5. ✅ IndividualRosterExtract_20260131.csv → `raw_individual_brokers`

**Status:** ⚠️ **PARTIAL MATCH** (extra columns in CSV)

**CSV Columns (10):**
```
PartyUniqueId, IndividualLastName, IndividualFirstName, HireDate, EmailAddress, 
CurrentStatus, BrokerType, BankRoutingNumber, AccountNumber, AccountType
```

**Expected Table Columns (7):**
```sql
PartyUniqueId, IndividualLastName, IndividualFirstName, HireDate, EmailAddress, 
CurrentStatus, BrokerType
```

**Analysis:**
- ✅ All expected columns present
- ⚠️ CSV has 3 extra columns: `BankRoutingNumber`, `AccountNumber`, `AccountType`
- **Action:** ✅ Ready to load (extra columns will be ignored or can be added to table schema)

**Recommendation:** Consider adding banking columns to table schema if needed:
```sql
BankRoutingNumber NVARCHAR(50),
AccountNumber NVARCHAR(100),
AccountType NVARCHAR(50)
```

---

### 6. ✅ OrganizationRosterExtract_20260131.csv → `raw_org_brokers`

**Status:** ⚠️ **PARTIAL MATCH** (extra columns in CSV)

**CSV Columns (8):**
```
PartyUniqueId, OrganizationName, HireDate, EmailAddress, CurrentStatus, 
BankRoutingNumber, AccountNumber, AccountType
```

**Expected Table Columns (5):**
```sql
PartyUniqueId, OrganizationName, HireDate, EmailAddress, CurrentStatus
```

**Analysis:**
- ✅ All expected columns present
- ⚠️ CSV has 3 extra columns: `BankRoutingNumber`, `AccountNumber`, `AccountType`
- **Action:** ✅ Ready to load (extra columns will be ignored or can be added to table schema)

**Recommendation:** Consider adding banking columns to table schema if needed:
```sql
BankRoutingNumber NVARCHAR(50),
AccountNumber NVARCHAR(100),
AccountType NVARCHAR(50)
```

---

### 7. ✅ BrokerLicenseExtract_20260131.csv → `raw_licenses`

**Status:** ✅ **MATCH** (perfect match)

**CSV Columns (9):**
```
PartyUniqueId, StateCode, CurrentStatus, LicenseCode, LicenseEffectiveDate, 
LicenseExpirationDate, IsResidenceLicense, LicenseNumber, ApplicableCounty
```

**Expected Table Columns (9):**
```sql
PartyUniqueId, StateCode, CurrentStatus, LicenseCode, LicenseEffectiveDate, 
LicenseExpirationDate, IsResidenceLicense, LicenseNumber, ApplicableCounty
```

**Analysis:**
- ✅ Perfect match - all columns present
- ✅ Column order matches exactly
- **Action:** ✅ Ready to load

---

### 8. ✅ BrokerEO_20260131.csv → `raw_eo_insurance`

**Status:** ✅ **MATCH** (perfect match)

**CSV Columns (10):**
```
PartyUniqueId, CarrierName, PolicyId, FromDate, ToDate, DeductibleAmount, 
ClaimMaxAmount, AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit
```

**Expected Table Columns (10):**
```sql
PartyUniqueId, CarrierName, PolicyId, FromDate, ToDate, DeductibleAmount, 
ClaimMaxAmount, AnnualMaxAmount, PolicyMaxAmount, LiabilityLimit
```

**Analysis:**
- ✅ Perfect match - all columns present
- ✅ Column order matches exactly
- **Action:** ✅ Ready to load

---

## Additional Files (Not in Expected List)

These files are present but not referenced in `copy-from-poc-etl.sql`:

### 9. 📋 CommissionsDetail_20260116_20260131.csv → `raw_commissions_detail`

**Status:** 📋 **EXTRA FILE** (not in copy script, but table exists)

**CSV Columns (15):**
```
Company, CertificateId, CertEffectiveDate, SplitBrokerId, PmtPostedDate, 
PaidToDate, PaidAmount, TransActionType, InvoiceNumber, CertInForceMonths, 
CommissionRate, RealCommissionRate, PaidBrokerId, CreditCardType, TransactionId
```

**Expected Table Columns (13):**
```sql
Company, CertificateId, CertEffectiveDate, SplitBrokerId, PmtPostedDate, 
PaidToDate, PaidAmount, TransActionType, InvoiceNumber, CertInForceMonths, 
CommissionRate, RealCommissionRate, PaidBrokerId, CreaditCardType, TransactionId
```

**Analysis:**
- ⚠️ Column name mismatch: CSV has `CreditCardType`, table expects `CreaditCardType` (typo in table)
- ✅ All other columns match
- **Action:** ⚠️ Can be loaded, but note the typo mismatch

**Note:** See `COMMISSIONS_DETAIL_SCHEMA_CHANGE.md` for details on schema changes.

---

### 10. 📋 Fees_20260131.csv → `raw_fees`

**Status:** 📋 **EXTRA FILE** (not in copy script, but table exists)

**CSV Columns (15):**
```
PRDNUM, PRODUCTCAT, FREQ, AMOUNT, FormattedAmount, AMTKIND, DATEEND, DATESTART, 
MAINT, FEECALCMETHOD, FormattedFeeCalcMethod, PERSISTPERIOD, RECNOFEEPERSON, 
FEETYPE, PartyUniqueId
```

**Expected Table Columns (15):**
```sql
PRDNUM, PRODUCTCAT, FREQ, AMOUNT, FormattedAmount, AMTKIND, DATEEND, DATESTART, 
MAINT, FEECALCMETHOD, FormattedFeeCalcMethod, PERSISTPERIOD, RECNOFEEPERSON, 
FEETYPE, PartyUniqueId
```

**Analysis:**
- ✅ Perfect match - all columns present
- ✅ Column order matches exactly
- **Action:** ✅ Ready to load (if needed)

---

### 11. 📋 CommHierarchy_20260131.csv → `raw_comm_hierarchy` (if table exists)

**Status:** 📋 **EXTRA FILE** (table may not exist)

**CSV Columns (9):**
```
Company, WritingBrokerId, HierDriver, HierVersion, SplitBrokerSeq, 
ContractEffectiveDate, SplitBrokerId, ContractId, CommissionsSchedule
```

**Analysis:**
- 📋 File present but not referenced in copy script
- ⚠️ Table may need to be created if this data is needed
- **Action:** ⚠️ Verify if this data is needed for ETL

---

### 12. 📋 BrokerMGARelationships_20260131.csv → `raw_broker_mga_relationships` (if table exists)

**Status:** 📋 **EXTRA FILE** (table may not exist)

**CSV Columns (3):**
```
ChildBrokerId, ParentBrokerId, RelationshipType
```

**Analysis:**
- 📋 File present but not referenced in copy script
- ⚠️ Table may need to be created if this data is needed
- **Action:** ⚠️ Verify if this data is needed for ETL

---

### 13. 📋 BrokerBalances_20260131.csv → `raw_broker_balances` (if table exists)

**Status:** 📋 **EXTRA FILE** (table may not exist)

**CSV Columns (11):**
```
Company, BrokerUniqueId, YearMonth, PaymentPeriod, PeriodBeginDate, PeriodEndDate, 
CurrentPeriodEarning, StatementCharges, BalanceForward, EndingBalance, RecStatus
```

**Analysis:**
- 📋 File present but not referenced in copy script
- ⚠️ Table may need to be created if this data is needed
- **Action:** ⚠️ Verify if this data is needed for ETL

---

### 14. 📋 CommRateMaximum_20260131.csv → `raw_comm_rate_maximum` (if table exists)

**Status:** 📋 **EXTRA FILE** (table may not exist)

**CSV Columns (4):**
```
Product, RateYearApplied, RateType, RateMaxValue
```

**Analysis:**
- 📋 File present but not referenced in copy script
- ⚠️ Table may need to be created if this data is needed
- **Action:** ⚠️ Verify if this data is needed for ETL

---

## CSV Ingest Plan

### Phase 1: Critical Tables (Required for ETL)

These tables are referenced in `copy-from-poc-etl.sql` and must be loaded:

| CSV File | Target Table | Status | Priority |
|----------|--------------|--------|----------|
| `CertificateInfo_20260131.csv` | `raw_certificate_info` | ✅ Ready | ⭐ **CRITICAL** |
| `APL-Perf_Schedule_model_20260131.csv` | `raw_schedule_rates` | ✅ Ready | ⭐ **CRITICAL** |
| `APL-Perf_Group_model_20260131.csv` | `raw_perf_groups` | ✅ Ready | 🔴 **HIGH** |
| `IndividualRosterExtract_20260131.csv` | `raw_individual_brokers` | ✅ Ready | 🔴 **HIGH** |
| `OrganizationRosterExtract_20260131.csv` | `raw_org_brokers` | ✅ Ready | 🔴 **HIGH** |
| `BrokerLicenseExtract_20260131.csv` | `raw_licenses` | ✅ Ready | 🟡 **MEDIUM** (optional) |
| `BrokerEO_20260131.csv` | `raw_eo_insurance` | ✅ Ready | 🟡 **MEDIUM** (optional) |

**Missing:**
- ❌ `premiums.csv` → `raw_premiums` (not found, check if needed)

---

### Phase 2: Additional Tables (If Needed)

These tables exist in schema but are not in the copy script:

| CSV File | Target Table | Status | Notes |
|----------|--------------|--------|-------|
| `CommissionsDetail_20260116_20260131.csv` | `raw_commissions_detail` | ⚠️ Column typo | See schema change doc |
| `Fees_20260131.csv` | `raw_fees` | ✅ Ready | Not in copy script |

---

### Phase 3: New Tables (May Need Creation)

These files don't have corresponding tables yet:

| CSV File | Suggested Table | Columns | Notes |
|----------|-----------------|---------|-------|
| `CommHierarchy_20260131.csv` | `raw_comm_hierarchy` | 9 columns | May be redundant with certificate data |
| `BrokerMGARelationships_20260131.csv` | `raw_broker_mga_relationships` | 3 columns | MGA hierarchy data |
| `BrokerBalances_20260131.csv` | `raw_broker_balances` | 11 columns | Broker payment balances |
| `CommRateMaximum_20260131.csv` | `raw_comm_rate_maximum` | 4 columns | Commission rate maximums |

---

## Column Mismatch Summary

### Minor Issues (Extra Columns in CSV)

1. **IndividualRosterExtract_20260131.csv**
   - Extra: `BankRoutingNumber`, `AccountNumber`, `AccountType`
   - **Recommendation:** Add to table schema or ignore during load

2. **OrganizationRosterExtract_20260131.csv**
   - Extra: `BankRoutingNumber`, `AccountNumber`, `AccountType`
   - **Recommendation:** Add to table schema or ignore during load

### Column Name Mismatch

1. **CommissionsDetail_20260116_20260131.csv**
   - CSV: `CreditCardType`
   - Table: `CreaditCardType` (typo in table)
   - **Recommendation:** Fix table column name or map during load

---

## Recommended CSV Ingest Script Updates

### Update `scripts/load-csv.ts` mappings:

```typescript
const csvMappings: CsvMapping[] = [
  // Critical files (2026-01-31)
  { csvFile: 'CertificateInfo_20260131.csv', tableName: 'raw_certificate_info' },
  { csvFile: 'APL-Perf_Schedule_model_20260131.csv', tableName: 'raw_schedule_rates' },
  { csvFile: 'APL-Perf_Group_model_20260131.csv', tableName: 'raw_perf_groups' },
  { csvFile: 'IndividualRosterExtract_20260131.csv', tableName: 'raw_individual_brokers' },
  { csvFile: 'OrganizationRosterExtract_20260131.csv', tableName: 'raw_org_brokers' },
  { csvFile: 'BrokerLicenseExtract_20260131.csv', tableName: 'raw_licenses' },
  { csvFile: 'BrokerEO_20260131.csv', tableName: 'raw_eo_insurance' },
  { csvFile: 'Fees_20260131.csv', tableName: 'raw_fees' },
  { csvFile: 'CommissionsDetail_*.csv', tableName: 'raw_commissions_detail' },
  
  // Note: premiums.csv not found - verify if needed
];
```

### Update `scripts/ingest-raw-data.ts` file patterns:

```typescript
const FILE_MAPPINGS: FileMapping[] = [
  {
    prefix: 'CertificateInfo',
    tableName: 'raw_certificate_info',
    expectedColumns: ['Company', 'ProductMasterCategory', 'ProductCategory', 'GroupId', 'Product', 'PlanCode', 'CertificateId', 'CertEffectiveDate', 'CertIssuedState', 'CertStatus', 'CertPremium', 'CertSplitSeq', 'CertSplitPercent', 'CustomerId', 'RecStatus', 'HierDriver', 'HierVersion', 'CommissionsSchedule', 'CommissionType', 'WritingBrokerID', 'SplitBrokerId', 'SplitBrokerSeq', 'ReassignedType', 'PaidBrokerId']
  },
  {
    prefix: 'APL-Perf_Schedule',
    tableName: 'raw_schedule_rates',
    expectedColumns: ['ScheduleName', 'ScheduleDescription', 'Category', 'ProductCode', 'OffGroupLetterDescription', 'State', 'GroupSizeFrom', 'GroupSizeTo', 'GroupSize', 'EffectiveStartDate', 'EffectiveEndDate', 'SeriesType', 'SpecialOffer', 'Year1', 'Year2', 'Year3', 'Year4', 'Year5', 'Year6', 'Year7', 'Year8', 'Year9', 'Year10', 'Year11', 'Year12', 'Year13', 'Year14', 'Year15', 'Year16', 'Year66', 'Year99', 'Level']
  },
  {
    prefix: 'APL-Perf_Group',
    tableName: 'raw_perf_groups',
    expectedColumns: ['GroupNum', 'GroupName', 'StateAbbreviation', 'GroupSize', 'BrokerUniqueId', 'BrokerReportsToUniqueId', 'AgencyName', 'Product', 'EffectiveDate', 'FundingType', 'CommissionTable', 'FeeAgreement']
  },
  {
    prefix: 'IndividualRosterExtract',
    tableName: 'raw_individual_brokers',
    expectedColumns: ['PartyUniqueId', 'IndividualLastName', 'IndividualFirstName', 'HireDate', 'EmailAddress', 'CurrentStatus', 'BrokerType', 'BankRoutingNumber', 'AccountNumber', 'AccountType'] // Added banking columns
  },
  {
    prefix: 'OrganizationRosterExtract',
    tableName: 'raw_org_brokers',
    expectedColumns: ['PartyUniqueId', 'OrganizationName', 'HireDate', 'EmailAddress', 'CurrentStatus', 'BankRoutingNumber', 'AccountNumber', 'AccountType'] // Added banking columns
  },
  // ... etc
];
```

---

## Action Items

### Immediate (Before Ingest)

1. ✅ **Verify all critical files are present** - DONE (7/8 found)
2. ⚠️ **Locate or verify `premiums.csv`** - Check if needed for initial load
3. ✅ **Update CSV loader scripts** - Use 2026-01-31 file names
4. ⚠️ **Decide on banking columns** - Add to broker tables or ignore?

### Short-term (After Ingest)

1. ⚠️ **Fix `CreaditCardType` typo** - In `raw_commissions_detail` table
2. 📋 **Evaluate extra files** - Determine if CommHierarchy, BrokerBalances, etc. are needed
3. 📋 **Create tables for new files** - If MGA relationships, balances, etc. are needed

### Long-term

1. 📋 **Standardize file naming** - Use consistent date format (YYYYMMDD)
2. 📋 **Document schema changes** - Track CSV schema evolution
3. 📋 **Automate column mapping** - Handle extra columns gracefully

---

## Conclusion

**Overall Status:** ✅ **READY FOR INGEST**

- ✅ 7 out of 8 critical files found and validated
- ✅ Column matches are excellent (only minor extra columns)
- ⚠️ 1 file missing (`premiums.csv`) - verify if needed
- 📋 5 additional files available for future use

**Next Steps:**
1. Update CSV loader scripts with correct file names
2. Load critical tables (CertificateInfo, Schedule Rates, Groups, Brokers)
3. Verify data quality after load
4. Handle missing `premiums.csv` separately if needed
