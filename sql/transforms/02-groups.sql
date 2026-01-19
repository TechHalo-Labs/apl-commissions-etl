-- =============================================================================
-- Transform: Groups (T-SQL)
-- Creates groups from certificates with names from perf-groups (97% coverage)
-- Usage: sqlcmd -S server -d database -i sql/transforms/02-groups.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Groups';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Build group name lookup from perf-groups (primary source)
-- =============================================================================
PRINT 'Step 1: Building group name lookup from raw_perf_groups...';

DROP TABLE IF EXISTS #tmp_group_names;
CREATE TABLE #tmp_group_names (
    GroupNumber NVARCHAR(100) NOT NULL,
    Name NVARCHAR(500),
    [State] NVARCHAR(10),
    PRIMARY KEY (GroupNumber)
);

INSERT INTO #tmp_group_names (GroupNumber, Name, [State])
SELECT 
    LTRIM(RTRIM(GroupNum)) AS GroupNumber,
    MAX(CASE WHEN LTRIM(RTRIM(GroupName)) <> '' THEN LTRIM(RTRIM(GroupName)) ELSE NULL END) AS Name,
    MAX(CASE WHEN LTRIM(RTRIM(StateAbbreviation)) <> '' THEN LTRIM(RTRIM(StateAbbreviation)) ELSE NULL END) AS [State]
FROM [etl].[raw_perf_groups]
WHERE LTRIM(RTRIM(GroupNum)) <> ''
GROUP BY LTRIM(RTRIM(GroupNum));

PRINT 'Group names from perf-groups: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 2: Add fallback names from premiums for groups not in perf-groups
-- =============================================================================
PRINT 'Step 2: Adding fallback names from raw_premiums...';

INSERT INTO #tmp_group_names (GroupNumber, Name, [State])
SELECT 
    LTRIM(RTRIM(p.GroupNumber)) AS GroupNumber,
    MAX(CASE WHEN LTRIM(RTRIM(p.GroupName)) <> '' AND LTRIM(RTRIM(p.GroupName)) <> 'NULL' 
             THEN LTRIM(RTRIM(p.GroupName)) ELSE NULL END) AS Name,
    MAX(CASE WHEN LTRIM(RTRIM(p.StateIssued)) <> '' THEN LTRIM(RTRIM(p.StateIssued)) ELSE NULL END) AS [State]
FROM [etl].[raw_premiums] p
WHERE LTRIM(RTRIM(p.GroupNumber)) <> ''
  AND NOT EXISTS (SELECT 1 FROM #tmp_group_names gn WHERE gn.GroupNumber = LTRIM(RTRIM(p.GroupNumber)))
GROUP BY LTRIM(RTRIM(p.GroupNumber));

PRINT 'Groups added from premiums: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT 'Total group names' AS info, COUNT(*) AS cnt FROM #tmp_group_names;

-- =============================================================================
-- Step 3: Get all unique groups from certificates
-- =============================================================================
PRINT '';
PRINT 'Step 3: Getting unique groups from input_certificate_info...';

DROP TABLE IF EXISTS #tmp_all_groups;
SELECT DISTINCT LTRIM(RTRIM(GroupId)) AS GroupNumber
INTO #tmp_all_groups
FROM [etl].[input_certificate_info]
WHERE LTRIM(RTRIM(GroupId)) <> '';

PRINT 'Unique groups from certificates: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 4: Truncate and populate stg_groups
-- =============================================================================
PRINT '';
PRINT 'Step 4: Populating stg_groups...';

TRUNCATE TABLE [etl].[stg_groups];

-- Get certificate states as fallback
DROP TABLE IF EXISTS #cert_states;
SELECT 
    LTRIM(RTRIM(ci.GroupId)) AS GroupNumber, 
    MAX(CASE WHEN LTRIM(RTRIM(ci.CertIssuedState)) <> '' THEN LTRIM(RTRIM(ci.CertIssuedState)) ELSE NULL END) AS [State]
INTO #cert_states
FROM [etl].[input_certificate_info] ci
WHERE LTRIM(RTRIM(ci.CertIssuedState)) <> ''
GROUP BY LTRIM(RTRIM(ci.GroupId));

-- Get premium states as secondary fallback
DROP TABLE IF EXISTS #premium_states;
SELECT 
    LTRIM(RTRIM(GroupNumber)) AS GroupNumber, 
    MAX(CASE WHEN LTRIM(RTRIM(StateIssued)) <> '' THEN LTRIM(RTRIM(StateIssued)) ELSE NULL END) AS [State]
INTO #premium_states
FROM [etl].[raw_premiums]
WHERE LTRIM(RTRIM(StateIssued)) <> ''
GROUP BY LTRIM(RTRIM(GroupNumber));

INSERT INTO [etl].[stg_groups] (
    Id, Name, [Description], Code, [State], IsActive, [Status], [Type], CreationTime, IsDeleted
)
SELECT
    -- Canonical Group ID: G{GroupNumber}
    CONCAT('G', ag.GroupNumber) AS Id,
    -- Priority: perf-group name > premium name > generated name
    COALESCE(
        NULLIF(gn.Name, ''),
        CONCAT('Group ', ag.GroupNumber)
    ) AS Name,
    CONCAT('Group: ', ag.GroupNumber) AS [Description],
    ag.GroupNumber AS Code,
    -- Priority: perf-group state > premium state > certificate state
    COALESCE(
        NULLIF(gn.[State], ''),
        NULLIF(ps.[State], ''),
        NULLIF(cs.[State], ''),
        ''
    ) AS [State],
    1 AS IsActive,  -- All groups from certificates are considered active
    0 AS [Status],
    0 AS [Type],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM #tmp_all_groups ag
LEFT JOIN #tmp_group_names gn ON gn.GroupNumber = ag.GroupNumber
LEFT JOIN #premium_states ps ON ps.GroupNumber = ag.GroupNumber
LEFT JOIN #cert_states cs ON cs.GroupNumber = ag.GroupNumber;

PRINT 'Groups staged: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Step 5: Add G00000 sentinel for Direct-to-Consumer policies
-- =============================================================================
PRINT '';
PRINT 'Step 5: Adding G00000 (Direct-to-Consumer) sentinel...';

IF NOT EXISTS (SELECT 1 FROM [etl].[stg_groups] WHERE Id = 'G00000')
BEGIN
    INSERT INTO [etl].[stg_groups] (
        Id, Name, [Description], Code, [State], IsActive, [Status], [Type], CreationTime, IsDeleted
    )
    VALUES (
        'G00000',
        'Direct to Consumer',
        'Direct-to-Consumer policies - not associated with an employer group',
        '00000',
        '',
        1,
        0,
        0,
        GETUTCDATE(),
        0
    );
    PRINT 'G00000 created';
END
ELSE
BEGIN
    PRINT 'G00000 already exists';
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'Total groups staged' AS metric, COUNT(*) AS cnt FROM [etl].[stg_groups];

SELECT 
    'Name coverage' AS metric,
    SUM(CASE WHEN Name IS NULL OR Name = '' OR Name LIKE 'Group %' THEN 1 ELSE 0 END) AS generated_names,
    SUM(CASE WHEN Name IS NOT NULL AND Name <> '' AND Name NOT LIKE 'Group %' THEN 1 ELSE 0 END) AS real_names
FROM [etl].[stg_groups];

SELECT 'State coverage' AS metric,
    SUM(CASE WHEN [State] IS NOT NULL AND [State] <> '' THEN 1 ELSE 0 END) AS has_state,
    SUM(CASE WHEN [State] IS NULL OR [State] = '' THEN 1 ELSE 0 END) AS no_state
FROM [etl].[stg_groups];

-- Cleanup temp tables
DROP TABLE IF EXISTS #tmp_group_names;
DROP TABLE IF EXISTS #tmp_all_groups;
DROP TABLE IF EXISTS #cert_states;
DROP TABLE IF EXISTS #premium_states;

PRINT '';
PRINT '============================================================';
PRINT 'GROUPS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

