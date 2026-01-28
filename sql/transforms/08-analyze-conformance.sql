-- =============================================================================
-- Analyze Group Conformance
-- =============================================================================
-- Analyzes which groups have certificates that map cleanly to proposals
-- Results guide export filtering (only export conformant/nearly conformant)
--
-- Conformant: 100% of certificates map to exactly one proposal
-- Nearly Conformant: >=95% of certificates map correctly
-- Non-Conformant: <95% conformance
-- =============================================================================

SET NOCOUNT ON;

PRINT '';
PRINT '════════════════════════════════════════════════════════════';
PRINT 'STEP: Analyze Group Conformance';
PRINT '════════════════════════════════════════════════════════════';
PRINT '';

-- Clean up temp tables if they exist
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

-- =============================================================================
-- Step 1: Build certificate to proposal mapping (deduplicate across remainder tables)
-- =============================================================================
PRINT 'Building certificate to proposal mappings...';

WITH CertificateKeys AS (
    -- Union certificates from both remainder tables (UNION deduplicates)
    SELECT
        CONCAT('G', LTRIM(RTRIM(GroupId))) AS GroupId,
        YEAR(TRY_CAST(EffectiveDate AS DATE)) AS EffectiveYear,
        LTRIM(RTRIM(ProductCode)) AS ProductCode,
        -- Sanitize PlanCode: NULL, empty, 'NULL', or 'N/A' → '*'
        CASE 
            WHEN PlanCode IS NULL THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = '' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'NULL' THEN '*'
            WHEN LTRIM(RTRIM(PlanCode)) = 'N/A' THEN '*'
            ELSE LTRIM(RTRIM(PlanCode))
        END AS PlanCode,
        TRY_CAST(CertificateId AS BIGINT) AS CertificateId,
        TRY_CAST(EffectiveDate AS DATE) AS CertEffectiveDate
    FROM [poc_etl].[cert_split_configs_remainder2]
    WHERE GroupId IS NOT NULL
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND ProductCode IS NOT NULL
      AND LTRIM(RTRIM(ProductCode)) <> ''
      
    UNION  -- UNION (not UNION ALL) deduplicates certificates appearing in both tables
    
    SELECT
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
    FROM [poc_etl].[cert_split_configs_remainder3]
    WHERE GroupId IS NOT NULL
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND ProductCode IS NOT NULL
      AND LTRIM(RTRIM(ProductCode)) <> ''
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
LEFT JOIN [$(ETL_SCHEMA)].[stg_proposal_key_mapping] pkm
    ON pkm.GroupId = ck.GroupId
    AND pkm.EffectiveYear = ck.EffectiveYear
    AND pkm.ProductCode = ck.ProductCode
    AND pkm.PlanCode = ck.PlanCode
GROUP BY 
    ck.GroupId, ck.EffectiveYear, ck.ProductCode, ck.PlanCode,
    ck.CertificateId, ck.CertEffectiveDate;

DECLARE @total_certs INT = @@ROWCOUNT;
PRINT 'Total unique certificates analyzed: ' + CAST(@total_certs AS VARCHAR);

-- =============================================================================
-- Step 2: Classify certificates as conformant or non-conformant
-- =============================================================================
PRINT 'Classifying certificates...';

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
-- Step 3: Aggregate by group (exclude special cases)
-- =============================================================================
PRINT 'Aggregating by group...';

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
    LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = cc.GroupId
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
-- Step 4: Insert into permanent table
-- =============================================================================
PRINT 'Populating GroupConformanceStatistics...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[GroupConformanceStatistics];

INSERT INTO [$(ETL_SCHEMA)].[GroupConformanceStatistics] (
    GroupId,
    GroupName,
    SitusState,
    TotalCertificates,
    ConformantCertificates,
    NonConformantCertificates,
    ConformancePercentage,
    GroupClassification,
    AnalysisDate
)
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
FROM #group_conformance;

DECLARE @total_groups INT = @@ROWCOUNT;
DECLARE @conformant_groups INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[GroupConformanceStatistics] WHERE GroupClassification = 'Conformant');
DECLARE @nearly_conformant_groups INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[GroupConformanceStatistics] WHERE GroupClassification = 'Nearly Conformant (>=95%)');
DECLARE @nonconformant_groups INT = (SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[GroupConformanceStatistics] WHERE GroupClassification = 'Non-Conformant');

PRINT '';
PRINT '════════════════════════════════════════════════════════════';
PRINT 'CONFORMANCE ANALYSIS SUMMARY';
PRINT '════════════════════════════════════════════════════════════';
PRINT 'Total Groups: ' + CAST(@total_groups AS VARCHAR);
PRINT '  • Conformant (100%): ' + CAST(@conformant_groups AS VARCHAR) + 
      ' (' + CAST(CAST(@conformant_groups * 100.0 / NULLIF(@total_groups, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '  • Nearly Conformant (>=95%): ' + CAST(@nearly_conformant_groups AS VARCHAR) + 
      ' (' + CAST(CAST(@nearly_conformant_groups * 100.0 / NULLIF(@total_groups, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '  • Non-Conformant (<95%): ' + CAST(@nonconformant_groups AS VARCHAR) + 
      ' (' + CAST(CAST(@nonconformant_groups * 100.0 / NULLIF(@total_groups, 0) AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT '';
PRINT 'Export Eligibility:';
PRINT '  • WILL EXPORT: ' + CAST(@conformant_groups + @nearly_conformant_groups AS VARCHAR) + ' groups';
PRINT '  • WILL SKIP: ' + CAST(@nonconformant_groups AS VARCHAR) + ' groups (non-conformant)';
PRINT '════════════════════════════════════════════════════════════';
PRINT '';

-- =============================================================================
-- Cleanup
-- =============================================================================
DROP TABLE IF EXISTS #cert_proposal_matches;
DROP TABLE IF EXISTS #cert_classification;
DROP TABLE IF EXISTS #group_conformance;

PRINT '✓ Group conformance analysis complete';
PRINT '';

GO
