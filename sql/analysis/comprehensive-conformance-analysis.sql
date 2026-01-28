-- =============================================================================
-- Comprehensive Group Conformance Analysis
-- =============================================================================
-- Analyzes ALL active certificates (not just remainder tables)
-- Maps from source new_data.CertificateInfo to proposals
--
-- Certificate Flow:
-- - Source: 148K active certificates
-- - Remainder tables: 96K "hard cases" 
-- - Missing: 52K certificates (successfully mapped in early stages, not in remainders)
--
-- This analysis covers ALL certificates for complete picture
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COMPREHENSIVE GROUP CONFORMANCE ANALYSIS (ALL CERTIFICATES)';
PRINT '============================================================';
PRINT '';

-- Clean up temp tables if they exist
DROP TABLE IF EXISTS #all_certs;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

-- =============================================================================
-- Step 1: Get ALL active certificates from source
-- =============================================================================
PRINT 'Step 1: Loading all active certificates from new_data.CertificateInfo...';

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
INTO #all_certs
FROM new_data.CertificateInfo ci
WHERE ci.CertStatus = 'A'
  AND ci.RecStatus = 'A'
  AND ci.GroupId IS NOT NULL
  AND LTRIM(RTRIM(ci.GroupId)) <> ''
  AND ci.Product IS NOT NULL
  AND LTRIM(RTRIM(ci.Product)) <> '';

DECLARE @total_source_certs INT = @@ROWCOUNT;
PRINT 'Total active certificates from source: ' + CAST(@total_source_certs AS VARCHAR);

-- =============================================================================
-- Step 2: Map certificates to proposals
-- =============================================================================
PRINT '';
PRINT 'Step 2: Mapping certificates to proposals...';

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
FROM #all_certs c
LEFT JOIN [poc_etl].[stg_proposal_key_mapping] pkm
    ON pkm.GroupId = c.GroupId
    AND pkm.EffectiveYear = c.EffectiveYear
    AND pkm.ProductCode = c.ProductCode
    AND pkm.PlanCode = c.PlanCode
GROUP BY 
    c.GroupId, c.EffectiveYear, c.ProductCode, c.PlanCode,
    c.CertificateId, c.CertEffectiveDate;

DECLARE @mapped_certs INT = @@ROWCOUNT;
PRINT 'Total certificates analyzed: ' + CAST(@mapped_certs AS VARCHAR);

-- =============================================================================
-- Step 3: Classify certificates as conformant or non-conformant
-- =============================================================================
PRINT '';
PRINT 'Step 3: Classifying certificates...';

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
      ' (' + CAST(CAST(@conformant_certs * 100.0 / NULLIF(@mapped_certs, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT 'Non-conformant certificates: ' + CAST(@nonconformant_certs AS VARCHAR) + 
      ' (' + CAST(CAST(@nonconformant_certs * 100.0 / NULLIF(@mapped_certs, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';

-- =============================================================================
-- Step 4: Aggregate by group (exclude special cases)
-- =============================================================================
PRINT '';
PRINT 'Step 4: Aggregating by group...';

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
INTO #group_conformance
FROM GroupStats;

-- =============================================================================
-- Step 5: Display Results
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY STATISTICS';
PRINT '============================================================';

SELECT 
    COUNT(*) AS TotalGroups,
    SUM(CASE WHEN GroupClassification = 'Conformant' THEN 1 ELSE 0 END) AS ConformantGroups,
    SUM(CASE WHEN GroupClassification = 'Nearly Conformant (>=95%)' THEN 1 ELSE 0 END) AS NearlyConformantGroups,
    SUM(CASE WHEN GroupClassification = 'Non-Conformant' THEN 1 ELSE 0 END) AS NonConformantGroups,
    SUM(TotalCertificates) AS TotalCertificates,
    SUM(ConformantCertificates) AS TotalConformantCerts,
    SUM(NonConformantCertificates) AS TotalNonConformantCerts,
    CAST(SUM(ConformantCertificates) * 100.0 / NULLIF(SUM(TotalCertificates), 0) AS DECIMAL(5,2)) AS OverallConformancePercentage
FROM #group_conformance;

PRINT '';
PRINT '============================================================';
PRINT 'DATA COVERAGE ANALYSIS';
PRINT '============================================================';

SELECT 
    'Source (new_data.CertificateInfo)' AS DataSet,
    (SELECT COUNT(DISTINCT CertificateId) FROM new_data.CertificateInfo WHERE CertStatus='A' AND RecStatus='A') AS CertCount,
    'Active certificates from source' AS Notes

UNION ALL

SELECT 
    'Analyzed in conformance report' AS DataSet,
    @total_source_certs AS CertCount,
    'Certificates included in this analysis' AS Notes

UNION ALL

SELECT 
    'Gap (not analyzed)' AS DataSet,
    (SELECT COUNT(DISTINCT CertificateId) FROM new_data.CertificateInfo WHERE CertStatus='A' AND RecStatus='A') - @total_source_certs AS CertCount,
    'Certificates missing from analysis (investigate)' AS Notes;

PRINT '';
PRINT '============================================================';
PRINT 'TOP 10 CONFORMANT GROUPS (100%)';
PRINT '============================================================';

SELECT TOP 10
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification
FROM #group_conformance
WHERE GroupClassification = 'Conformant'
ORDER BY TotalCertificates DESC;

PRINT '';
PRINT '============================================================';
PRINT 'TOP 20 NON-CONFORMANT GROUPS';
PRINT '============================================================';

SELECT TOP 20
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification
FROM #group_conformance
WHERE GroupClassification = 'Non-Conformant'
ORDER BY TotalCertificates DESC;

PRINT '';
PRINT '============================================================';
PRINT 'NEARLY CONFORMANT GROUPS (>=95%)';
PRINT '============================================================';

SELECT 
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification
FROM #group_conformance
WHERE GroupClassification = 'Nearly Conformant (>=95%)'
ORDER BY NonConformantCertificates DESC;

-- =============================================================================
-- Step 6: Create permanent table with results
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'CREATING PERMANENT TABLE';
PRINT '============================================================';

DROP TABLE IF EXISTS [poc_etl].[ComprehensiveGroupConformanceStatistics];

SELECT 
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification,
    GETUTCDATE() AS AnalysisDate
INTO [poc_etl].[ComprehensiveGroupConformanceStatistics]
FROM #group_conformance;

PRINT 'Created table: poc_etl.ComprehensiveGroupConformanceStatistics';
PRINT 'Rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Cleanup
-- =============================================================================
DROP TABLE IF EXISTS #all_certs;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

PRINT '';
PRINT '============================================================';
PRINT 'COMPREHENSIVE ANALYSIS COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'NOTE: This analysis covers ALL active certificates from source,';
PRINT 'not just the remainder certificates used in ETL pipeline.';
PRINT '';

GO
