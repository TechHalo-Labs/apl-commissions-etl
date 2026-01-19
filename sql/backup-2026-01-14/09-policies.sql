-- =============================================================================
-- Transform: Policies (T-SQL)
-- Creates one policy per CertificateId from input_certificate_info
-- Usage: sqlcmd -S server -d database -i sql/transforms/09-policies.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Policies';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Find the minimum CertSplitSeq for each certificate
-- =============================================================================
PRINT 'Step 1: Finding minimum split sequence per certificate...';

DROP TABLE IF EXISTS #tmp_min_seq;

SELECT 
    CAST(CertificateId AS BIGINT) AS CertificateId, 
    MIN(CertSplitSeq) AS MinSeq
INTO #tmp_min_seq
FROM [etl].[input_certificate_info]
WHERE CertificateId IS NOT NULL 
  AND TRY_CAST(CertificateId AS BIGINT) > 0
GROUP BY CAST(CertificateId AS BIGINT);

PRINT 'Certificates found: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 2: Build policy data from input_certificate_info
-- =============================================================================
PRINT '';
PRINT 'Step 2: Building policy data...';

DROP TABLE IF EXISTS #tmp_policy_data;

SELECT
    CAST(ici.CertificateId AS BIGINT) AS CertificateId,
    MAX(ici.Company) AS Company,
    MAX(ici.ProductMasterCategory) AS ProductMasterCategory,
    MAX(ici.ProductCategory) AS ProductCategory,
    -- Normalize empty GroupId to '00000' (Direct-to-Consumer)
    IIF(LTRIM(RTRIM(MAX(ici.GroupId))) = '' OR MAX(ici.GroupId) IS NULL, '00000', MAX(ici.GroupId)) AS GroupId,
    MAX(ici.Product) AS Product,
    MAX(ici.PlanCode) AS PlanCode,
    MAX(ici.CertEffectiveDate) AS CertEffectiveDate,
    MAX(ici.CertIssuedState) AS CertIssuedState,
    MAX(ici.CertStatus) AS CertStatus,
    MAX(ici.CertPremium) AS CertPremium,
    MAX(ici.CustomerId) AS CustomerId,
    TRY_CAST(REPLACE(MAX(ici.WritingBrokerID), 'P', '') AS BIGINT) AS WritingBrokerId
INTO #tmp_policy_data
FROM [etl].[input_certificate_info] ici
INNER JOIN #tmp_min_seq ms ON ms.CertificateId = CAST(ici.CertificateId AS BIGINT) 
                           AND ici.CertSplitSeq = ms.MinSeq
WHERE CAST(ici.CertificateId AS BIGINT) > 0
  AND ici.SplitBrokerSeq = 1
GROUP BY CAST(ici.CertificateId AS BIGINT);

PRINT 'Policy data rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Truncate and populate stg_policies
-- =============================================================================
PRINT '';
PRINT 'Step 3: Populating stg_policies...';

TRUNCATE TABLE [etl].[stg_policies];

INSERT INTO [etl].[stg_policies] (
    Id, PolicyNumber, CertificateNumber, PolicyType, [Status], BrokerId, GroupId,
    CarrierName, ProductCode, ProductName, PlanCode, MasterCategory, Category,
    InsuredName, Premium, EffectiveDate, [State], CompanyCode,
    CustomerId, ProposalId, CreationTime, IsDeleted
)
SELECT
    CertificateId AS Id,
    CAST(CertificateId AS NVARCHAR(50)) AS PolicyNumber,
    CAST(CertificateId AS NVARCHAR(50)) AS CertificateNumber,
    0 AS PolicyType,
    CASE CertStatus
        WHEN 'Active' THEN 0
        WHEN 'A' THEN 0
        WHEN 'Terminated' THEN 1
        WHEN 'T' THEN 1
        WHEN 'Cancelled' THEN 2
        WHEN 'C' THEN 2
        WHEN 'L' THEN 3  -- Lapsed
        ELSE 0
    END AS [Status],
    WritingBrokerId AS BrokerId,
    CONCAT('G', GroupId) AS GroupId,  -- Canonical G-prefixed GroupId
    Company AS CarrierName,
    Product AS ProductCode,
    CONCAT(COALESCE(ProductCategory, ''), ' - ', COALESCE(Product, '')) AS ProductName,
    PlanCode,
    ProductMasterCategory AS MasterCategory,
    ProductCategory AS Category,
    COALESCE(NULLIF(CustomerId, ''), CONCAT('Insured-', CAST(CertificateId AS VARCHAR))) AS InsuredName,
    COALESCE(TRY_CAST(CertPremium AS DECIMAL(18,2)), 0.00) AS Premium,
    COALESCE(TRY_CAST(CertEffectiveDate AS DATE), '2020-01-01') AS EffectiveDate,
    CertIssuedState AS [State],
    Company AS CompanyCode,
    NULLIF(LTRIM(RTRIM(CustomerId)), '') AS CustomerId,
    NULL AS ProposalId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_policy_data
WHERE CertificateId > 0;

DECLARE @policy_count INT = @@ROWCOUNT;
PRINT 'Policies created: ' + CAST(@policy_count AS VARCHAR);

-- =============================================================================
-- Step 4: Link Policies to Proposals by GroupId and Effective Date
-- For each policy, find the proposal whose effective date range contains the policy's date
-- If no date match, fall back to active/most recent proposal
-- =============================================================================
PRINT '';
PRINT 'Step 4: Linking policies to proposals (with date matching)...';

-- First, try to match policies to proposals by GroupId AND EffectiveDate range
-- A policy matches a proposal if:
--   1. GroupId matches
--   2. Policy.EffectiveDate >= Proposal.EffectiveDateFrom
--   3. Policy.EffectiveDate <= Proposal.EffectiveDateTo (or EffectiveDateTo is NULL = open-ended)
UPDATE pol
SET pol.ProposalId = matched.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-DateMatch'
FROM [etl].[stg_policies] pol
INNER JOIN (
    SELECT 
        pol.Id AS PolicyId,
        prop.Id AS ProposalId,
        ROW_NUMBER() OVER (
            PARTITION BY pol.Id 
            ORDER BY 
                -- Prefer exact date range match
                CASE WHEN pol.EffectiveDate >= prop.EffectiveDateFrom 
                     AND (prop.EffectiveDateTo IS NULL OR pol.EffectiveDate <= prop.EffectiveDateTo)
                     THEN 0 ELSE 1 END,
                -- Then prefer most specific (has end date) over open-ended
                CASE WHEN prop.EffectiveDateTo IS NOT NULL THEN 0 ELSE 1 END,
                -- Then by recency
                prop.EffectiveDateFrom DESC
        ) AS rn
    FROM [etl].[stg_policies] pol
    INNER JOIN [etl].[stg_proposals] prop ON prop.GroupId = pol.GroupId
    WHERE pol.ProposalId IS NULL
      AND prop.EnableEffectiveDateFiltering = 0  -- Non-filtering proposals match any date
      OR (
          prop.EnableEffectiveDateFiltering = 1
          AND pol.EffectiveDate >= prop.EffectiveDateFrom
          AND (prop.EffectiveDateTo IS NULL OR pol.EffectiveDate <= prop.EffectiveDateTo)
      )
) matched ON matched.PolicyId = pol.Id AND matched.rn = 1
WHERE pol.ProposalId IS NULL;

DECLARE @date_matched INT = @@ROWCOUNT;
PRINT 'Policies linked by date match: ' + CAST(@date_matched AS VARCHAR);

-- Fallback: For remaining unmatched policies, use the most recent/active proposal for the group
-- This handles cases where no proposal date range matches the policy
DROP TABLE IF EXISTS #tmp_best_proposal;

;WITH ranked_proposals AS (
    SELECT 
        GroupId,
        Id AS ProposalId,
        EffectiveDateFrom,
        EffectiveDateTo,
        ROW_NUMBER() OVER (
            PARTITION BY GroupId 
            ORDER BY 
                CASE WHEN EffectiveDateTo IS NULL THEN 0 ELSE 1 END,  -- Active first
                EffectiveDateFrom DESC  -- Most recent
        ) AS rn
    FROM [etl].[stg_proposals]
)
SELECT GroupId, ProposalId
INTO #tmp_best_proposal
FROM ranked_proposals
WHERE rn = 1;

-- Update remaining unlinked policies with fallback proposal
UPDATE pol
SET pol.ProposalId = bp.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-GroupFallback'
FROM [etl].[stg_policies] pol
INNER JOIN #tmp_best_proposal bp ON bp.GroupId = pol.GroupId
WHERE pol.ProposalId IS NULL;

DECLARE @fallback_count INT = @@ROWCOUNT;
PRINT 'Policies linked by group fallback: ' + CAST(@fallback_count AS VARCHAR);

DECLARE @total_linked INT = @date_matched + @fallback_count;
PRINT 'Total policies linked to proposals: ' + CAST(@total_linked AS VARCHAR);

-- Report unlinked policies (excluding Direct-to-Consumer G00000)
DECLARE @unlinked_count INT = (
    SELECT COUNT(*) FROM [etl].[stg_policies] 
    WHERE ProposalId IS NULL AND GroupId <> 'G00000'
);
IF @unlinked_count > 0
    PRINT 'WARNING: ' + CAST(@unlinked_count AS VARCHAR) + ' policies have no proposal (excluding DTC)';

-- Cleanup temp table
DROP TABLE IF EXISTS #tmp_best_proposal;

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Total policies' AS metric, COUNT(*) AS cnt FROM [etl].[stg_policies];

SELECT 'Policies by status' AS metric, [Status], COUNT(*) AS cnt
FROM [etl].[stg_policies]
GROUP BY [Status]
ORDER BY [Status];

SELECT 'Policies with NULL BrokerId' AS metric, COUNT(*) AS cnt
FROM [etl].[stg_policies]
WHERE BrokerId IS NULL OR BrokerId = 0;

SELECT 'Policies with G00000 (Direct)' AS metric, COUNT(*) AS cnt
FROM [etl].[stg_policies]
WHERE GroupId = 'G00000';

-- Cleanup
DROP TABLE IF EXISTS #tmp_min_seq;
DROP TABLE IF EXISTS #tmp_policy_data;

PRINT '';
PRINT '============================================================';
PRINT 'POLICIES TRANSFORM COMPLETED';
PRINT '============================================================';

GO

