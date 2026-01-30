-- =============================================================================
-- Find Conformant Groups from new_data.CertificateInfo
-- =============================================================================
-- Analyzes certificates from new_data.CertificateInfo and identifies groups
-- where ALL certificates map to exactly ONE proposal (100% conformant).
--
-- Conformant Certificate: Maps to exactly ONE proposal via key mapping
-- Conformant Group: 100% of certificates are conformant
-- =============================================================================

SET NOCOUNT ON;

-- Clean up temp tables if they exist
DROP TABLE IF EXISTS #cert_keys;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #conformant_groups;

-- =============================================================================
-- Step 1: Extract certificate keys from new_data.CertificateInfo
-- =============================================================================
SELECT DISTINCT
    CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
    YEAR(TRY_CAST(ci.CertEffectiveDate AS DATE)) AS EffectiveYear,
    LTRIM(RTRIM(ci.Product)) AS ProductCode,
    -- Sanitize PlanCode: NULL, empty, 'NULL', or 'N/A' → '*'
    CASE 
        WHEN ci.PlanCode IS NULL THEN '*'
        WHEN LTRIM(RTRIM(ci.PlanCode)) = '' THEN '*'
        WHEN LTRIM(RTRIM(ci.PlanCode)) = 'NULL' THEN '*'
        WHEN LTRIM(RTRIM(ci.PlanCode)) = 'N/A' THEN '*'
        ELSE LTRIM(RTRIM(ci.PlanCode))
    END AS PlanCode,
    TRY_CAST(ci.CertificateId AS BIGINT) AS CertificateId,
    TRY_CAST(ci.CertEffectiveDate AS DATE) AS CertEffectiveDate
INTO #cert_keys
FROM new_data.CertificateInfo ci
WHERE ci.CertStatus = 'A'
  AND ci.RecStatus = 'A'
  AND ci.GroupId IS NOT NULL
  AND LTRIM(RTRIM(ci.GroupId)) <> ''
  AND ci.Product IS NOT NULL
  AND LTRIM(RTRIM(ci.Product)) <> '';

DECLARE @total_certs INT = @@ROWCOUNT;
PRINT 'Total active certificates analyzed: ' + CAST(@total_certs AS VARCHAR);

-- =============================================================================
-- Step 2: Map certificates to proposals
-- =============================================================================
SELECT 
    c.GroupId,
    c.EffectiveYear,
    c.ProductCode,
    c.PlanCode,
    c.CertificateId,
    c.CertEffectiveDate,
    -- Count matching proposals
    COUNT(pkm.ProposalId) AS MatchCount,
    -- Capture matched proposal IDs (for debugging)
    STRING_AGG(pkm.ProposalId, ', ') AS MatchedProposalIds
INTO #cert_proposal_matches
FROM #cert_keys c
LEFT JOIN [poc_etl].[stg_proposal_key_mapping] pkm
    ON pkm.GroupId = c.GroupId
    AND pkm.EffectiveYear = c.EffectiveYear
    AND pkm.ProductCode = c.ProductCode
    AND pkm.PlanCode = c.PlanCode
GROUP BY 
    c.GroupId, c.EffectiveYear, c.ProductCode, c.PlanCode,
    c.CertificateId, c.CertEffectiveDate;

-- =============================================================================
-- Step 3: Classify certificates as conformant or non-conformant
-- =============================================================================
SELECT
    GroupId,
    CertificateId,
    ProductCode,
    PlanCode,
    EffectiveYear,
    MatchCount,
    MatchedProposalIds,
    CASE
        WHEN MatchCount = 1 THEN 'Conformant'
        WHEN MatchCount = 0 THEN 'Non-Conformant (No Match)'
        WHEN MatchCount > 1 THEN 'Non-Conformant (Multiple Matches)'
    END AS ConformanceStatus
INTO #cert_classification
FROM #cert_proposal_matches;

DECLARE @conformant_certs INT = (SELECT COUNT(*) FROM #cert_classification WHERE ConformanceStatus = 'Conformant');
DECLARE @nonconformant_certs INT = (SELECT COUNT(*) FROM #cert_classification WHERE ConformanceStatus LIKE 'Non-Conformant%');

PRINT 'Conformant certificates: ' + CAST(@conformant_certs AS VARCHAR) + 
      ' (' + CAST(CAST(@conformant_certs * 100.0 / NULLIF(@total_certs, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT 'Non-conformant certificates: ' + CAST(@nonconformant_certs AS VARCHAR) + 
      ' (' + CAST(CAST(@nonconformant_certs * 100.0 / NULLIF(@total_certs, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';

-- =============================================================================
-- Step 4: Aggregate by group and identify conformant groups
-- =============================================================================
WITH GroupStats AS (
    SELECT 
        cc.GroupId,
        g.Name AS GroupName,
        g.[State] AS SitusState,
        COUNT(*) AS TotalCertificates,
        SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) AS ConformantCertificates,
        SUM(CASE WHEN cc.ConformanceStatus LIKE 'Non-Conformant%' THEN 1 ELSE 0 END) AS NonConformantCertificates,
        CAST(SUM(CASE WHEN cc.ConformanceStatus = 'Conformant' THEN 1 ELSE 0 END) * 100.0 / 
             NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS ConformancePercentage
    FROM #cert_classification cc
    LEFT JOIN [poc_etl].[stg_groups] g ON g.Id = cc.GroupId
    WHERE 1=1
        -- Exclude Direct-to-Consumer (no group or special patterns)
        AND cc.GroupId IS NOT NULL
        AND cc.GroupId <> ''
        AND cc.GroupId <> 'G'
        -- Exclude 5-digit groups starting with 7 (special non-conformant case)
        AND NOT (LEN(REPLACE(cc.GroupId, 'G', '')) = 5 AND LEFT(REPLACE(cc.GroupId, 'G', ''), 1) = '7')
        -- Exclude Universal Trucking groups
        AND (g.Name IS NULL OR g.Name NOT LIKE 'Universal Trucking%')
    GROUP BY cc.GroupId, g.Name, g.[State]
)
SELECT 
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    CASE
        WHEN ConformancePercentage = 100.0 THEN 'Conformant'
        WHEN ConformancePercentage >= 95.0 THEN 'Nearly Conformant (>=95%)'
        ELSE 'Non-Conformant'
    END AS GroupClassification
INTO #conformant_groups
FROM GroupStats;

-- =============================================================================
-- Step 5: Return ONLY Conformant Groups (100% conformance)
-- =============================================================================
SELECT 
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification
FROM #conformant_groups
WHERE GroupClassification = 'Conformant'
ORDER BY TotalCertificates DESC;

-- =============================================================================
-- Summary Statistics
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';

SELECT 
    COUNT(*) AS TotalConformantGroups,
    SUM(TotalCertificates) AS TotalCertificatesInConformantGroups,
    CAST(AVG(ConformancePercentage) AS DECIMAL(5,2)) AS AvgConformancePercentage
FROM #conformant_groups
WHERE GroupClassification = 'Conformant';

-- =============================================================================
-- Cleanup
-- =============================================================================
DROP TABLE IF EXISTS #cert_keys;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #conformant_groups;

GO
