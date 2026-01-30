-- =============================================================================
-- Group Conformance Analysis - ALL CERTIFICATES
-- =============================================================================
-- Analyzes which groups have certificates that map cleanly to proposals
-- (conformant) vs. those that don't (non-conformant).
--
-- Conformant Certificate: Maps to exactly ONE proposal via key mapping
-- Non-Conformant Certificate: Maps to 0 or >1 proposals
--
-- NOTE: This version analyzes ALL certificates from cert_split_configs,
-- not just the remainder tables. This includes certificates that were
-- processed by simple groups, plan-differentiated, year-differentiated, etc.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'GROUP CONFORMANCE ANALYSIS - ALL CERTIFICATES';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build certificate key to proposal mapping
-- =============================================================================
PRINT 'Step 1: Building certificate to proposal mappings...';

-- Clean up temp tables if they exist from a previous failed run
DROP TABLE IF EXISTS #cert_keys;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

-- Build certificate keys from the appropriate source
-- Try cert_split_configs first, fallback to input_certificate_info
IF OBJECT_ID('poc_etl.cert_split_configs', 'U') IS NOT NULL 
   AND EXISTS (SELECT 1 FROM [poc_etl].[cert_split_configs] WHERE GroupId IS NOT NULL AND LTRIM(RTRIM(GroupId)) <> '')
BEGIN
    PRINT 'Using poc_etl.cert_split_configs';
    
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(GroupId))) AS GroupId,
        YEAR(TRY_CAST(EffectiveDate AS DATE)) AS EffectiveYear,
        LTRIM(RTRIM(ProductCode)) AS ProductCode,
        CASE 
            WHEN PlanCode IS NULL THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = '' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'NULL' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'N/A' THEN '*'
            ELSE LTRIM(RTRIM(PlanCode))
        END AS PlanCode,
        TRY_CAST(CertificateId AS BIGINT) AS CertificateId,
        TRY_CAST(EffectiveDate AS DATE) AS CertEffectiveDate
    INTO #cert_keys
    FROM [poc_etl].[cert_split_configs]
    WHERE GroupId IS NOT NULL
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND ProductCode IS NOT NULL
      AND LTRIM(RTRIM(ProductCode)) <> '';
END
ELSE IF OBJECT_ID('poc_etl.input_certificate_info', 'U') IS NOT NULL
BEGIN
    PRINT 'Using poc_etl.input_certificate_info (cert_split_configs not available)';
    
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(GroupId))) AS GroupId,
        YEAR(TRY_CAST(CertEffectiveDate AS DATE)) AS EffectiveYear,
        LTRIM(RTRIM(Product)) AS ProductCode,
        CASE 
            WHEN PlanCode IS NULL THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = '' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'NULL' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'N/A' THEN '*'
            ELSE LTRIM(RTRIM(PlanCode))
        END AS PlanCode,
        TRY_CAST(CertificateId AS BIGINT) AS CertificateId,
        TRY_CAST(CertEffectiveDate AS DATE) AS CertEffectiveDate
    INTO #cert_keys
    FROM [poc_etl].[input_certificate_info]
    WHERE GroupId IS NOT NULL 
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND CertStatus IN ('A', 'Active')
      AND RecStatus = 'A'
      AND Product IS NOT NULL
      AND LTRIM(RTRIM(Product)) <> '';
END
ELSE
BEGIN
    PRINT 'ERROR: Neither poc_etl.cert_split_configs nor poc_etl.input_certificate_info found!';
    PRINT 'Please run the diagnostic query: sql/analysis/diagnose-cert-tables.sql';
    RETURN;
END

DECLARE @key_count INT = (SELECT COUNT(*) FROM #cert_keys);
PRINT 'Certificate keys extracted: ' + CAST(@key_count AS VARCHAR);
PRINT '';

WITH CertificateKeys AS (
    SELECT * FROM #cert_keys
)
SELECT 
    ck.GroupId,
    ck.EffectiveYear,
    ck.ProductCode,
    ck.PlanCode,
    ck.CertificateId,
    ck.CertEffectiveDate,
    -- Count matching proposals
    COUNT(pkm.ProposalId) AS MatchCount,
    -- Capture matched proposal IDs (for debugging)
    STRING_AGG(pkm.ProposalId, ', ') AS MatchedProposalIds
INTO #cert_proposal_matches
FROM CertificateKeys ck
LEFT JOIN [poc_etl].[stg_proposal_key_mapping] pkm
    ON pkm.GroupId = ck.GroupId
    AND pkm.EffectiveYear = ck.EffectiveYear
    AND pkm.ProductCode = ck.ProductCode
    AND pkm.PlanCode = ck.PlanCode
GROUP BY 
    ck.GroupId, ck.EffectiveYear, ck.ProductCode, ck.PlanCode,
    ck.CertificateId, ck.CertEffectiveDate;

DECLARE @total_certs INT = @@ROWCOUNT;
PRINT 'Total certificates analyzed: ' + CAST(@total_certs AS VARCHAR);

-- =============================================================================
-- Step 2: Classify certificates as conformant or non-conformant
-- =============================================================================
PRINT '';
PRINT 'Step 2: Classifying certificates...';

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

PRINT 'Conformant certificates: ' + CAST(@conformant_certs AS VARCHAR);
PRINT 'Non-conformant certificates: ' + CAST(@nonconformant_certs AS VARCHAR);

-- =============================================================================
-- Step 3: Aggregate by group with exclusions
-- =============================================================================
PRINT '';
PRINT 'Step 3: Aggregating by group...';

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
FROM GroupStats
ORDER BY ConformancePercentage DESC, TotalCertificates DESC;

-- =============================================================================
-- Step 4: Display Results
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
-- Step 5: Create permanent table with results
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'CREATING PERMANENT TABLE';
PRINT '============================================================';

DROP TABLE IF EXISTS [poc_etl].[GroupConformanceStatistics_AllCertificates];

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
INTO [poc_etl].[GroupConformanceStatistics_AllCertificates]
FROM #group_conformance;

PRINT 'Created table: poc_etl.GroupConformanceStatistics_AllCertificates';
PRINT 'Rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Cleanup
-- =============================================================================
DROP TABLE IF EXISTS #cert_keys;
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

PRINT '';
PRINT '============================================================';
PRINT 'ANALYSIS COMPLETE';
PRINT '============================================================';

GO
