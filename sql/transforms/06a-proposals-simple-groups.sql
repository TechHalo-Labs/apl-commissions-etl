-- =============================================================================
-- Transform: Proposals - Step 1: Simple Groups (Single Config)
-- 
-- This handles groups that have exactly ONE split configuration across all
-- their certificates. These get a simple "Group + All Products" proposal.
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PROPOSALS STEP 1: Simple Groups (Single Config)';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build cert_split_configs table (all active certificates)
-- =============================================================================
PRINT 'Step 1: Building cert_split_configs table...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[cert_split_configs];

WITH SplitDetails AS (
    SELECT 
        LTRIM(RTRIM(GroupId)) AS GroupId,
        TRY_CAST(CertEffectiveDate AS DATE) AS EffectiveDate,
        LTRIM(RTRIM(Product)) AS ProductCode,
        COALESCE(LTRIM(RTRIM(PlanCode)), '*') AS PlanCode,
        CertificateId,
        CertSplitSeq,
        SplitBrokerSeq,
        SplitBrokerId,
        CertSplitPercent,
        CommissionsSchedule
    FROM [$(ETL_SCHEMA)].[input_certificate_info]
    WHERE GroupId IS NOT NULL 
      AND LTRIM(RTRIM(GroupId)) <> ''
      AND CertStatus IN ('A', 'Active')
      AND RecStatus = 'A'  -- Only active split configurations (filter out historical/deleted)
      AND Product IS NOT NULL
),
CertConfigs AS (
    SELECT 
        GroupId,
        EffectiveDate,
        ProductCode,
        PlanCode,
        CertificateId,
        (
            SELECT 
                CertSplitSeq as splitSeq,
                SplitBrokerSeq as [level],
                SplitBrokerId as brokerId,
                CertSplitPercent as [percent],
                CommissionsSchedule as schedule
            FROM SplitDetails sd2
            WHERE sd2.CertificateId = sd.CertificateId
            ORDER BY CertSplitSeq, SplitBrokerSeq
            FOR JSON PATH
        ) AS ConfigJson
    FROM SplitDetails sd
    GROUP BY GroupId, EffectiveDate, ProductCode, PlanCode, CertificateId
)
SELECT *
INTO [$(ETL_SCHEMA)].[cert_split_configs]
FROM CertConfigs;

DECLARE @total_certs INT = @@ROWCOUNT;
PRINT 'Total active certificates: ' + CAST(@total_certs AS VARCHAR);

-- Add index for performance
CREATE NONCLUSTERED INDEX IX_cert_split_configs_GroupId ON [$(ETL_SCHEMA)].[cert_split_configs] (GroupId);

-- =============================================================================
-- Step 2: Identify single-config groups
-- =============================================================================
PRINT '';
PRINT 'Step 2: Identifying single-config groups...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[simple_groups];

-- Extract first broker from JSON config for each group
SELECT 
    csc.GroupId,
    COUNT(*) AS CertCount,
    MIN(csc.EffectiveDate) AS MinEffDate,
    MAX(csc.EffectiveDate) AS MaxEffDate,
    MAX(csc.ConfigJson) AS ConfigJson,
    MAX(JSON_VALUE(csc.ConfigJson, '$[0].brokerId')) AS WritingBrokerId
INTO [$(ETL_SCHEMA)].[simple_groups]
FROM [$(ETL_SCHEMA)].[cert_split_configs] csc
GROUP BY csc.GroupId
HAVING COUNT(DISTINCT csc.ConfigJson) = 1;

DECLARE @simple_groups INT = @@ROWCOUNT;
PRINT 'Single-config groups: ' + CAST(@simple_groups AS VARCHAR);

-- Get cert count for simple groups
DECLARE @simple_certs INT = (SELECT SUM(CertCount) FROM [$(ETL_SCHEMA)].[simple_groups]);
PRINT 'Certificates in simple groups: ' + CAST(@simple_certs AS VARCHAR);

-- =============================================================================
-- Step 3: Create proposals for simple groups
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating proposals for simple groups...';

-- Clear existing proposals (will rebuild)
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposals];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState,
    BrokerUniquePartyId, BrokerName, GroupId, GroupName, Notes,
    ProductCodes, PlanCodes, SplitConfigHash, DateRangeFrom, DateRangeTo,
    EnableEffectiveDateFiltering, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, CreationTime, IsDeleted
)
SELECT
    CONCAT('P-G', sg.GroupId, '-1') AS Id,
    CONCAT('G', sg.GroupId, '-1') AS ProposalNumber,
    2 AS [Status],  -- Approved
    sg.MinEffDate AS SubmittedDate,
    sg.MinEffDate AS ProposedEffectiveDate,
    0 AS SpecialCase,
    0 AS SpecialCaseCode,
    g.[State] AS SitusState,
    -- NEW: Use BrokerUniquePartyId (strip 'P' prefix, get numeric string)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b2 
            WHERE b2.ExternalPartyId = REPLACE(REPLACE(sg.WritingBrokerId, 'P', ''), ' ', '')
        )
        THEN REPLACE(REPLACE(sg.WritingBrokerId, 'P', ''), ' ', '')
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    CONCAT('G', sg.GroupId) AS GroupId,
    g.Name AS GroupName,
    'Simple group - single config' AS Notes,
    '*' AS ProductCodes,  -- All products
    '*' AS PlanCodes,     -- All plans  
    CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', sg.ConfigJson), 2) AS SplitConfigHash,
    YEAR(sg.MinEffDate) AS DateRangeFrom,
    NULL AS DateRangeTo,  -- Open-ended
    0 AS EnableEffectiveDateFiltering,
    sg.MinEffDate AS EffectiveDateFrom,
    NULL AS EffectiveDateTo,
    0 AS EnablePlanCodeFiltering,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[simple_groups] sg
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = CONCAT('G', sg.GroupId)
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b ON b.ExternalPartyId = REPLACE(REPLACE(sg.WritingBrokerId, 'P', ''), ' ', '');

DECLARE @proposals_created INT = @@ROWCOUNT;
PRINT 'Proposals created: ' + CAST(@proposals_created AS VARCHAR);

-- =============================================================================
-- Step 4: Create key mapping for simple groups
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating key mapping for simple group certificates...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_proposal_key_mapping];

INSERT INTO [$(ETL_SCHEMA)].[stg_proposal_key_mapping] (
    GroupId, EffectiveYear, ProductCode, PlanCode, ProposalId, SplitConfigHash
)
SELECT DISTINCT
    CONCAT('G', csc.GroupId) AS GroupId,
    YEAR(csc.EffectiveDate) AS EffectiveYear,
    csc.ProductCode,
    csc.PlanCode,
    CONCAT('P-G', csc.GroupId, '-1') AS ProposalId,
    p.SplitConfigHash
FROM [$(ETL_SCHEMA)].[cert_split_configs] csc
INNER JOIN [$(ETL_SCHEMA)].[simple_groups] sg ON sg.GroupId = csc.GroupId
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.Id = CONCAT('P-G', csc.GroupId, '-1');

DECLARE @mappings_created INT = @@ROWCOUNT;
PRINT 'Key mappings created: ' + CAST(@mappings_created AS VARCHAR);

-- =============================================================================
-- Step 5: Create PremiumSplitVersions for simple groups
-- One split version per proposal, with TotalSplitPercent from ConfigJson
-- =============================================================================
PRINT '';
PRINT 'Step 5: Creating PremiumSplitVersions for simple groups...';

-- Clear existing split versions (will be rebuilt for all proposal types)
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_versions];

-- Calculate TotalSplitPercent from ConfigJson (sum of level=1 participants)
INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_versions] (
    Id, GroupId, GroupName, ProposalId, ProposalNumber,
    VersionNumber, EffectiveFrom, EffectiveTo,
    TotalSplitPercent, [Status], [Source], CreationTime, IsDeleted
)
SELECT
    CONCAT('PSV-P-G', sg.GroupId, '-1') AS Id,
    CONCAT('G', sg.GroupId) AS GroupId,
    g.Name AS GroupName,
    CONCAT('P-G', sg.GroupId, '-1') AS ProposalId,
    CONCAT('G', sg.GroupId, '-1') AS ProposalNumber,
    '1.0' AS VersionNumber,
    sg.MinEffDate AS EffectiveFrom,
    NULL AS EffectiveTo,
    (
        SELECT SUM(TRY_CAST(j.[percent] AS DECIMAL(5,2)))
        FROM OPENJSON(sg.ConfigJson)
        WITH ([level] INT '$.level', [percent] DECIMAL(5,2) '$.percent') j
        WHERE j.[level] = 1
    ) AS TotalSplitPercent,
    1 AS [Status],  -- Active
    0 AS [Source],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[simple_groups] sg
LEFT JOIN [$(ETL_SCHEMA)].[stg_groups] g ON g.Id = CONCAT('G', sg.GroupId);

DECLARE @split_versions_created INT = @@ROWCOUNT;
PRINT 'Split versions created: ' + CAST(@split_versions_created AS VARCHAR);

-- =============================================================================
-- Step 6: Create PremiumSplitParticipants for simple groups
-- Extract level=1 participants from ConfigJson, link to hierarchies/schedules
-- =============================================================================
PRINT '';
PRINT 'Step 6: Creating PremiumSplitParticipants for simple groups...';

-- Clear existing split participants (will be rebuilt for all proposal types)
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_premium_split_participants];

-- Note: HierarchyId will be set later in 07-hierarchies.sql via stg_splitseq_hierarchy_map
INSERT INTO [$(ETL_SCHEMA)].[stg_premium_split_participants] (
    Id, VersionId, BrokerId, BrokerUniquePartyId, BrokerName, SplitPercent, IsWritingAgent,
    HierarchyId, HierarchyName, Sequence, WritingBrokerId, GroupId,
    EffectiveFrom, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY sg.GroupId, j.splitSeq) AS Id,
    CONCAT('PSV-P-G', sg.GroupId, '-1') AS VersionId,
    -- BrokerId (required, deprecated but still needed)
    COALESCE(b.Id, TRY_CAST(REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '') AS BIGINT), 0) AS BrokerId,
    -- NEW: Use BrokerUniquePartyId (only if broker exists)
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM [$(ETL_SCHEMA)].[stg_brokers] b2 
            WHERE b2.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
        )
        THEN REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
        ELSE NULL
    END AS BrokerUniquePartyId,
    b.Name AS BrokerName,
    TRY_CAST(j.[percent] AS DECIMAL(5,2)) AS SplitPercent,
    1 AS IsWritingAgent,
    NULL AS HierarchyId,  -- Will be linked in 07-hierarchies.sql
    NULL AS HierarchyName,
    j.splitSeq AS Sequence,
    TRY_CAST(REPLACE(j.brokerId, 'P', '') AS BIGINT) AS WritingBrokerId,
    CONCAT('G', sg.GroupId) AS GroupId,  -- Store for later hierarchy linking
    sg.MinEffDate AS EffectiveFrom,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[simple_groups] sg
CROSS APPLY OPENJSON(sg.ConfigJson)
    WITH (
        splitSeq INT '$.splitSeq',
        [level] INT '$.level',
        brokerId NVARCHAR(50) '$.brokerId',
        [percent] DECIMAL(5,2) '$.percent',
        schedule NVARCHAR(100) '$.schedule'
    ) j
LEFT JOIN [$(ETL_SCHEMA)].[stg_brokers] b 
    ON b.ExternalPartyId = REPLACE(REPLACE(j.brokerId, 'P', ''), ' ', '')
WHERE j.[level] = 1;  -- Only writing broker level (split participants)

DECLARE @split_participants_created INT = @@ROWCOUNT;
PRINT 'Split participants created: ' + CAST(@split_participants_created AS VARCHAR);

-- =============================================================================
-- Step 7: Create remainder table (non-simple groups)
-- =============================================================================
PRINT '';
PRINT 'Step 7: Creating remainder table (complex groups)...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[cert_split_configs_remainder];

SELECT csc.*
INTO [$(ETL_SCHEMA)].[cert_split_configs_remainder]
FROM [$(ETL_SCHEMA)].[cert_split_configs] csc
WHERE NOT EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[simple_groups] sg WHERE sg.GroupId = csc.GroupId
);

DECLARE @remainder_certs INT = @@ROWCOUNT;
PRINT 'Certificates remaining to process: ' + CAST(@remainder_certs AS VARCHAR);

-- Get unique group count in remainder
DECLARE @remainder_groups INT = (SELECT COUNT(DISTINCT GroupId) FROM [$(ETL_SCHEMA)].[cert_split_configs_remainder]);
PRINT 'Groups remaining to process: ' + CAST(@remainder_groups AS VARCHAR);

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'SUMMARY';
PRINT '============================================================';
PRINT 'Total active certificates: ' + CAST(@total_certs AS VARCHAR);
PRINT '';
PRINT 'SIMPLE GROUPS (single config):';
PRINT '  Groups: ' + CAST(@simple_groups AS VARCHAR);
PRINT '  Certificates: ' + CAST(@simple_certs AS VARCHAR);
PRINT '  Proposals created: ' + CAST(@proposals_created AS VARCHAR);
PRINT '  Split versions created: ' + CAST(@split_versions_created AS VARCHAR);
PRINT '  Split participants created: ' + CAST(@split_participants_created AS VARCHAR);
PRINT '';
PRINT 'REMAINDER (complex groups):';
PRINT '  Groups: ' + CAST(@remainder_groups AS VARCHAR);
PRINT '  Certificates: ' + CAST(@remainder_certs AS VARCHAR);
PRINT '';
PRINT '============================================================';
PRINT 'STEP 1 COMPLETED';
PRINT '============================================================';

GO
