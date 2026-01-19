# CSV Schema Changes - January 2026

## Summary

The CSV files from the January 2026 extract have different column names than expected. This document tracks the changes.

## Field Name Mappings

### 1. IndividualRosterExtract → raw_individual_brokers

**Old Schema:**
- `BrokerId`, `Name`, `Status`, `Type`

**New Schema:**
- `PartyUniqueId` (was BrokerId)
- `IndividualLastName` + `IndividualFirstName` (was Name)
- `CurrentStatus` (was Status)
- `HireDate`, `EmailAddress`, `BankRoutingNumber`, `AccountNumber`, `AccountType` (new fields)

**Mapping:**
- `BrokerId` → `PartyUniqueId`
- `Name` → `IndividualLastName` + `IndividualFirstName`
- `Status` → `CurrentStatus`
- `Type` → (removed, not in new schema)

### 2. OrganizationRosterExtract → raw_org_brokers

**Old Schema:**
- `BrokerId`, `Name`, `Status`, `Type`

**New Schema:**
- `PartyUniqueId` (was BrokerId)
- `OrganizationName` (was Name)
- `CurrentStatus` (was Status)
- `HireDate`, `EmailAddress`, `BankRoutingNumber`, `AccountNumber`, `AccountType` (new fields)

**Mapping:**
- `BrokerId` → `PartyUniqueId`
- `Name` → `OrganizationName`
- `Status` → `CurrentStatus`
- `Type` → (removed, not in new schema)

### 3. CommissionsDetail → raw_commissions_detail

**⚠️ MAJOR SCHEMA CHANGE**

**Old Schema (Expected):**
- `Company`, `CertificateId`, `CertEffectiveDate`, `SplitBrokerId`, `PmtPostedDate`, `PaidToDate`, `PaidAmount`, `TransActionType`, `InvoiceNumber`, `CertInForceMonths`, `CommissionRate`, `RealCommissionRate`, `PaidBrokerId`, `CreaditCardType`, `TransactionId`

**New Schema (Actual):**
- `WritingBrokerId`
- `HierDriver`
- `HierVersion`
- `SplitBrokerSeq`
- `ContractEffectiveDate`
- `ContractId`
- `CommissionsSchedule`

**Impact:** This is a **complete schema change**. The transform scripts that process `raw_commissions_detail` will need to be updated to use the new column names.

### 4. BrokerLicenseExtract → raw_broker_licenses

**Old Schema:**
- `BrokerId`, `State`, `LicenseNumber`, `Type`, `Status`, `EffectiveDate`, `ExpirationDate`

**New Schema:**
- `PartyUniqueId` (was BrokerId)
- `StateCode` (was State)
- `CurrentStatus` (was Status)
- `LicenseCode` (new)
- `LicenseEffectiveDate` (was EffectiveDate)
- `LicenseExpirationDate` (was ExpirationDate)
- `IsResidenceLicense` (new)
- `ApplicableCounty` (new)

**Mapping:**
- `BrokerId` → `PartyUniqueId`
- `State` → `StateCode`
- `Status` → `CurrentStatus`
- `EffectiveDate` → `LicenseEffectiveDate`
- `ExpirationDate` → `LicenseExpirationDate`
- `LicenseNumber` → (removed, use LicenseCode?)
- `Type` → (removed)

### 5. BrokerEO → raw_broker_eo

**Old Schema:**
- `BrokerId`, `PolicyNumber`, `Carrier`, `CoverageAmount`, `EffectiveDate`, `ExpirationDate`

**New Schema:**
- `PartyUniqueId` (was BrokerId)
- `CarrierName` (was Carrier)
- `PolicyId` (was PolicyNumber)
- `FromDate` (was EffectiveDate)
- `ToDate` (was ExpirationDate)
- `DeductibleAmount` (new)
- `ClaimMaxAmount` (new)
- `AnnualMaxAmount` (new)
- `PolicyMaxAmount` (new)
- `LiabilityLimit` (new)

**Mapping:**
- `BrokerId` → `PartyUniqueId`
- `Carrier` → `CarrierName`
- `PolicyNumber` → `PolicyId`
- `EffectiveDate` → `FromDate`
- `ExpirationDate` → `ToDate`
- `CoverageAmount` → (removed, use new coverage fields?)

## Files That Match Expected Schema

✅ **CertificateInfo** - No changes, matches expected schema
✅ **APL-Perf_Schedule** - No changes, matches expected schema

## Files Without Expected Schema

- `APL-Perf_Group_model_20260116.csv` → `raw_perf_groups` (new file)
- `Fees_20260116.csv` → `raw_fees` (new file)
- `CommHierarchy_20260116.csv` → `raw_comm_hierarchy` (new file)
- `BrokerMGARelationships_20260116.csv` → `raw_broker_mga_relationships` (new file)

## Action Items

1. ✅ **Update ingest script** - Expected columns updated to match new schemas
2. 🔲 **Update transform scripts** - Transform scripts need to be updated to use new column names:
   - `01-brokers.sql` - Use `PartyUniqueId` instead of `BrokerId`
   - `13-export-licenses.sql` - Use new license column names
   - `13-export-licenses.sql` - Use new EO column names
   - Commission detail transforms - **MAJOR UPDATE NEEDED** for new schema
3. 🔲 **Review new files** - Determine how to process:
   - `APL-Perf_Group_model` - Group performance data?
   - `Fees` - Fee schedules?
   - `CommHierarchy` - Commission hierarchy data?
   - `BrokerMGARelationships` - MGA relationship mappings?

## Notes

- All broker-related files now use `PartyUniqueId` instead of `BrokerId` - this is a consistent change across all files
- Status fields are consistently renamed to `CurrentStatus`
- Date fields often have more specific names (e.g., `LicenseEffectiveDate` instead of `EffectiveDate`)
- The `CommissionsDetail` schema change is the most significant and will require careful review
