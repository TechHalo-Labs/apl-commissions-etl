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
FROM [$(ETL_SCHEMA)].[input_certificate_info]
WHERE CertificateId IS NOT NULL 
  AND TRY_CAST(CertificateId AS BIGINT) > 0
  AND LTRIM(RTRIM(RecStatus)) = 'A'  -- Only active split configurations (trim trailing spaces)
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
FROM [$(ETL_SCHEMA)].[input_certificate_info] ici
INNER JOIN #tmp_min_seq ms ON ms.CertificateId = CAST(ici.CertificateId AS BIGINT) 
                           AND ici.CertSplitSeq = ms.MinSeq
WHERE CAST(ici.CertificateId AS BIGINT) > 0
  AND ici.SplitBrokerSeq = 1
  AND LTRIM(RTRIM(ici.RecStatus)) = 'A'  -- Only active split configurations (trim trailing spaces)
GROUP BY CAST(ici.CertificateId AS BIGINT);

PRINT 'Policy data rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 3: Truncate and populate stg_policies
-- =============================================================================
PRINT '';
PRINT 'Step 3: Populating stg_policies...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_policies];

INSERT INTO [$(ETL_SCHEMA)].[stg_policies] (
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
    CASE LTRIM(RTRIM(CertStatus))
        WHEN 'Active' THEN 0
        WHEN 'A' THEN 0
        WHEN 'Terminated' THEN 1
        WHEN 'T' THEN 1
        WHEN 'Cancelled' THEN 2
        WHEN 'C' THEN 2
        WHEN 'L' THEN 3  -- Lapsed
        ELSE 0
    END AS [Status],
    COALESCE(WritingBrokerId, 0) AS BrokerId,  -- Default to 0 if NULL
    'G' + GroupId AS GroupId,  -- Add G-prefix to match proposal key mapping
    COALESCE(NULLIF(LTRIM(RTRIM(Company)), ''), 'APL') AS CarrierName,  -- Default to 'APL' if NULL
    Product AS ProductCode,
    CONCAT(COALESCE(ProductCategory, ''), ' - ', COALESCE(Product, '')) AS ProductName,
    PlanCode,
    ProductMasterCategory AS MasterCategory,
    ProductCategory AS Category,
    COALESCE(NULLIF(CustomerId, ''), CONCAT('Insured-', CAST(CertificateId AS VARCHAR))) AS InsuredName,
    COALESCE(TRY_CAST(CertPremium AS DECIMAL(18,2)), 0.00) AS Premium,
    COALESCE(TRY_CAST(CertEffectiveDate AS DATE), '2020-01-01') AS EffectiveDate,
    CertIssuedState AS [State],
    COALESCE(NULLIF(LTRIM(RTRIM(Company)), ''), 'APL') AS CompanyCode,  -- Default to 'APL' if NULL
    NULLIF(LTRIM(RTRIM(CustomerId)), '') AS CustomerId,
    NULL AS ProposalId,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_policy_data
WHERE CertificateId > 0;

DECLARE @policy_count INT = @@ROWCOUNT;
PRINT 'Policies created: ' + CAST(@policy_count AS VARCHAR);

-- =============================================================================
-- Step 4: Link Policies to Proposals using Proposal Key Mapping Table
-- Deterministic lookup: F(Group, Year, Product, Plan) -> ProposalId
-- =============================================================================
PRINT '';
PRINT 'Step 4: Linking policies to proposals (using key mapping)...';

-- Primary match: Use the proposal key mapping table for deterministic lookup
-- This matches based on (GroupId, EffectiveYear, ProductCode, PlanCode)
UPDATE pol
SET pol.ProposalId = m.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-KeyMapping'
FROM [$(ETL_SCHEMA)].[stg_policies] pol
INNER JOIN [$(ETL_SCHEMA)].[stg_proposal_key_mapping] m
    ON m.GroupId = pol.GroupId
    AND m.EffectiveYear = YEAR(pol.EffectiveDate)
    AND m.ProductCode = pol.ProductCode
    AND (m.PlanCode = COALESCE(pol.PlanCode, '') OR m.PlanCode = '*');

DECLARE @key_matched INT = @@ROWCOUNT;
PRINT 'Policies linked by key mapping (exact): ' + CAST(@key_matched AS VARCHAR);

-- Secondary match: Try with wildcard product for remaining unmatched
UPDATE pol
SET pol.ProposalId = m.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-KeyMapping-ProductWildcard'
FROM [$(ETL_SCHEMA)].[stg_policies] pol
INNER JOIN [$(ETL_SCHEMA)].[stg_proposal_key_mapping] m
    ON m.GroupId = pol.GroupId
    AND m.EffectiveYear = YEAR(pol.EffectiveDate)
    AND m.ProductCode = '*'
    AND m.PlanCode = '*'
WHERE pol.ProposalId IS NULL;

DECLARE @product_wildcard_matched INT = @@ROWCOUNT;
PRINT 'Policies linked by product wildcard: ' + CAST(@product_wildcard_matched AS VARCHAR);

-- Fallback: For remaining unmatched policies, try year-adjacent match
-- Find the closest year in the mapping for the same Group+Product+Plan
UPDATE pol
SET pol.ProposalId = adj.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-KeyMapping-YearAdjacent'
FROM [$(ETL_SCHEMA)].[stg_policies] pol
INNER JOIN (
    SELECT 
        pol.Id AS PolicyId,
        m.ProposalId,
        ROW_NUMBER() OVER (
            PARTITION BY pol.Id 
            ORDER BY ABS(m.EffectiveYear - YEAR(pol.EffectiveDate))
        ) AS rn
    FROM [$(ETL_SCHEMA)].[stg_policies] pol
    INNER JOIN [$(ETL_SCHEMA)].[stg_proposal_key_mapping] m
        ON m.GroupId = pol.GroupId
        AND m.ProductCode = pol.ProductCode
        AND (m.PlanCode = COALESCE(pol.PlanCode, '') OR m.PlanCode = '*')
    WHERE pol.ProposalId IS NULL
) adj ON adj.PolicyId = pol.Id AND adj.rn = 1
WHERE pol.ProposalId IS NULL;

DECLARE @year_adjacent_matched INT = @@ROWCOUNT;
PRINT 'Policies linked by year-adjacent: ' + CAST(@year_adjacent_matched AS VARCHAR);

-- Final fallback: Use the most recent/active proposal for the group (legacy behavior)
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
    FROM [$(ETL_SCHEMA)].[stg_proposals]
)
SELECT GroupId, ProposalId
INTO #tmp_best_proposal
FROM ranked_proposals
WHERE rn = 1;

UPDATE pol
SET pol.ProposalId = bp.ProposalId,
    pol.ProposalAssignedAt = GETUTCDATE(),
    pol.ProposalAssignmentSource = 'ETL-GroupFallback'
FROM [$(ETL_SCHEMA)].[stg_policies] pol
INNER JOIN #tmp_best_proposal bp ON bp.GroupId = pol.GroupId
WHERE pol.ProposalId IS NULL;

DECLARE @group_fallback INT = @@ROWCOUNT;
PRINT 'Policies linked by group fallback: ' + CAST(@group_fallback AS VARCHAR);

DECLARE @total_linked INT = @key_matched + @product_wildcard_matched + @year_adjacent_matched + @group_fallback;
PRINT 'Total policies linked to proposals: ' + CAST(@total_linked AS VARCHAR);

-- =============================================================================
-- Step 5: Populate PaidThroughDate from commission details
-- =============================================================================
PRINT '';
PRINT 'Step 5: Populating PaidThroughDate from commission details...';

;WITH max_paid_dates AS (
    SELECT 
        CertificateId,
        MAX(PaidToDate) AS LatestPaidToDate
    FROM [$(ETL_SCHEMA)].[input_commission_details]
    WHERE PaidToDate IS NOT NULL
    GROUP BY CertificateId
)
UPDATE pol
SET pol.PaidThroughDate = mpd.LatestPaidToDate
FROM [$(ETL_SCHEMA)].[stg_policies] pol
INNER JOIN max_paid_dates mpd ON mpd.CertificateId = pol.Id;

DECLARE @paid_through_count INT = @@ROWCOUNT;
PRINT 'Policies with PaidThroughDate: ' + CAST(@paid_through_count AS VARCHAR);

-- Report assignment source distribution
SELECT 'Assignment sources' AS metric, ProposalAssignmentSource, COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_policies]
WHERE ProposalAssignmentSource IS NOT NULL
GROUP BY ProposalAssignmentSource
ORDER BY cnt DESC;

-- Report unlinked policies (excluding Direct-to-Consumer 00000)
DECLARE @unlinked_count INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_policies] 
    WHERE ProposalId IS NULL AND GroupId <> '00000'
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

SELECT 'Total policies' AS metric, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_policies];

SELECT 'Policies by status' AS metric, [Status], COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_policies]
GROUP BY [Status]
ORDER BY [Status];

SELECT 'Policies with NULL BrokerId' AS metric, COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_policies]
WHERE BrokerId IS NULL OR BrokerId = 0;

SELECT 'Policies with 00000 (Direct)' AS metric, COUNT(*) AS cnt
FROM [$(ETL_SCHEMA)].[stg_policies]
WHERE GroupId = '00000';

-- Cleanup
DROP TABLE IF EXISTS #tmp_min_seq;
DROP TABLE IF EXISTS #tmp_policy_data;

PRINT '';
PRINT '============================================================';
PRINT 'POLICIES TRANSFORM COMPLETED';
PRINT '============================================================';

GO

