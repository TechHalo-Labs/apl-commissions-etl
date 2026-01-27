-- =============================================================================
-- Transform: State Rules & Hierarchy Splits (T-SQL)
-- Creates state rules and hierarchy splits linking products to hierarchies
-- Usage: sqlcmd -S server -d database -i sql/transforms/08-hierarchy-splits.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: State Rules & Hierarchy Splits';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Truncate staging tables
-- =============================================================================
PRINT 'Step 1: Truncating staging tables...';
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_state_rules];
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_state_rule_states];
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_hierarchy_splits];

-- =============================================================================
-- Step 2: Create State Rules (one per HierarchyVersion + State combination)
-- =============================================================================
PRINT '';
PRINT 'Step 2: Creating state rules from certificate data...';

INSERT INTO [$(ETL_SCHEMA)].[stg_state_rules] (
    Id, HierarchyVersionId, ShortName, Name, [Description], [Type], SortOrder,
    CreationTime, IsDeleted
)
SELECT
    -- Id: SR-{HierarchyVersionId}-{State}
    CONCAT('SR-', hv.Id, '-', cert_states.[State]) AS Id,
    hv.Id AS HierarchyVersionId,
    cert_states.[State] AS ShortName,
    cert_states.[State] AS Name,
    CONCAT('State rule for ', cert_states.[State], ' in hierarchy ', h.Name) AS [Description],
    0 AS [Type],  -- 0=Include
    ROW_NUMBER() OVER (PARTITION BY hv.Id ORDER BY cert_states.[State]) AS SortOrder,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    -- Get unique (GroupId, WritingBrokerId, State) combinations
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        LTRIM(RTRIM(ci.CertIssuedState)) AS [State]
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.WritingBrokerID <> ''
      AND ci.CertIssuedState <> ''
      AND ci.RecStatus = 'A'  -- Only active split configurations
      AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
) cert_states
-- Join to hierarchies via GroupId and BrokerId
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchies] h 
    ON h.GroupId = cert_states.GroupId 
    AND h.BrokerId = cert_states.WritingBrokerId
-- Get the hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv 
    ON hv.HierarchyId = h.Id;

DECLARE @state_rules_count INT = @@ROWCOUNT;
PRINT 'State rules created: ' + CAST(@state_rules_count AS VARCHAR);

-- =============================================================================
-- Step 3: Create State Rule States (one per state rule, maps state code to name)
-- =============================================================================
PRINT '';
PRINT 'Step 3: Creating state rule states...';

INSERT INTO [$(ETL_SCHEMA)].[stg_state_rule_states] (
    Id, StateRuleId, StateCode, StateName, CreationTime, IsDeleted
)
SELECT
    CONCAT(sr.Id, '-', sr.ShortName) AS Id,
    sr.Id AS StateRuleId,
    sr.ShortName AS StateCode,
    sr.ShortName AS StateName,  -- Could map to full state name if needed
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_state_rules] sr;

DECLARE @state_rule_states_count INT = @@ROWCOUNT;
PRINT 'State rule states created: ' + CAST(@state_rule_states_count AS VARCHAR);

-- =============================================================================
-- Step 3b: Convert single-rule hierarchies to catch-all rules
-- If a hierarchy has exactly ONE state rule, convert it to a catch-all rule:
--   - ShortName = 'ALL', Name = 'All States'
--   - Remove all entries from stg_state_rule_states for that rule
-- =============================================================================
PRINT '';
PRINT 'Step 3b: Converting single-rule hierarchies to catch-all rules...';

-- Identify hierarchy versions with exactly one state rule
DROP TABLE IF EXISTS #single_rule_hierarchies;

SELECT sr.Id AS StateRuleId, sr.HierarchyVersionId
INTO #single_rule_hierarchies
FROM [$(ETL_SCHEMA)].[stg_state_rules] sr
WHERE sr.HierarchyVersionId IN (
    SELECT HierarchyVersionId
    FROM [$(ETL_SCHEMA)].[stg_state_rules]
    GROUP BY HierarchyVersionId
    HAVING COUNT(*) = 1
);

DECLARE @single_rule_count INT = @@ROWCOUNT;
PRINT 'Hierarchy versions with single state rule: ' + CAST(@single_rule_count AS VARCHAR);

-- Delete state rule states for single-rule hierarchies (catch-all has no states)
DELETE srs
FROM [$(ETL_SCHEMA)].[stg_state_rule_states] srs
WHERE srs.StateRuleId IN (SELECT StateRuleId FROM #single_rule_hierarchies);

PRINT 'State rule states deleted for catch-all conversion: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Update the state rule to be a catch-all
UPDATE sr
SET sr.ShortName = 'ALL',
    sr.Name = 'All States',
    sr.[Description] = 'Catch-all state rule (applies to all states)',
    sr.[Type] = 1  -- 1 = CatchAll (0 = Specific)
FROM [$(ETL_SCHEMA)].[stg_state_rules] sr
WHERE sr.Id IN (SELECT StateRuleId FROM #single_rule_hierarchies);

PRINT 'State rules converted to catch-all: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- Also need to update hierarchy splits to reference the renamed state rule
-- The hierarchy splits are created AFTER this step, so we need to update the 
-- stg_state_rules.Id to reflect the new 'ALL' shortname for proper joins
-- Actually, let's update the IDs to use 'ALL' instead of the old state code
UPDATE sr
SET sr.Id = CONCAT('SR-', sr.HierarchyVersionId, '-ALL')
FROM [$(ETL_SCHEMA)].[stg_state_rules] sr
WHERE sr.Id IN (SELECT StateRuleId FROM #single_rule_hierarchies);

PRINT 'State rule IDs updated to use ALL suffix';

DROP TABLE #single_rule_hierarchies;

-- =============================================================================
-- Step 4: Create Hierarchy Splits (products within each state rule)
-- For catch-all rules (ShortName='ALL'), we link all products regardless of state
-- =============================================================================
PRINT '';
PRINT 'Step 4: Creating hierarchy splits...';

-- First, create splits for state-specific rules (ShortName != 'ALL')
INSERT INTO [$(ETL_SCHEMA)].[stg_hierarchy_splits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName, SortOrder,
    CreationTime, IsDeleted
)
SELECT
    -- Id: {StateRuleId}-{ProductCode}
    CONCAT(sr.Id, '-', cert_products.ProductCode) AS Id,
    sr.Id AS StateRuleId,
    pc.ProductId AS ProductId,
    cert_products.ProductCode AS ProductCode,
    pc.[Description] AS ProductName,
    ROW_NUMBER() OVER (PARTITION BY sr.Id ORDER BY cert_products.ProductCode) AS SortOrder,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    -- Get unique (GroupId, WritingBrokerId, State, ProductCode) combinations
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        LTRIM(RTRIM(ci.CertIssuedState)) AS [State],
        LTRIM(RTRIM(ci.Product)) AS ProductCode
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.WritingBrokerID <> ''
      AND ci.CertIssuedState <> ''
      AND ci.Product <> ''
      AND ci.RecStatus = 'A'  -- Only active split configurations
      AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
) cert_products
-- Join to hierarchies via GroupId and BrokerId
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchies] h 
    ON h.GroupId = cert_products.GroupId 
    AND h.BrokerId = cert_products.WritingBrokerId
-- Get the hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv 
    ON hv.HierarchyId = h.Id
-- Get the state rule for this hierarchy version + state (state-specific only)
INNER JOIN [$(ETL_SCHEMA)].[stg_state_rules] sr
    ON sr.HierarchyVersionId = hv.Id
    AND sr.ShortName = cert_products.[State]  -- Only state-specific rules
-- Get product code metadata (optional)
LEFT JOIN [$(ETL_SCHEMA)].[stg_product_codes] pc 
    ON pc.Code = cert_products.ProductCode;

DECLARE @splits_state_specific INT = @@ROWCOUNT;
PRINT 'Hierarchy splits created (state-specific): ' + CAST(@splits_state_specific AS VARCHAR);

-- Second, create splits for catch-all rules (ShortName = 'ALL')
-- These get ALL products for the hierarchy, regardless of state
INSERT INTO [$(ETL_SCHEMA)].[stg_hierarchy_splits] (
    Id, StateRuleId, ProductId, ProductCode, ProductName, SortOrder,
    CreationTime, IsDeleted
)
SELECT
    -- Id: {StateRuleId}-{ProductCode}
    CONCAT(sr.Id, '-', cert_products.ProductCode) AS Id,
    sr.Id AS StateRuleId,
    pc.ProductId AS ProductId,
    cert_products.ProductCode AS ProductCode,
    pc.[Description] AS ProductName,
    ROW_NUMBER() OVER (PARTITION BY sr.Id ORDER BY cert_products.ProductCode) AS SortOrder,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    -- Get unique (GroupId, WritingBrokerId, ProductCode) combinations - NO state filter
    SELECT DISTINCT
        CONCAT('G', LTRIM(RTRIM(ci.GroupId))) AS GroupId,
        TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) AS WritingBrokerId,
        LTRIM(RTRIM(ci.Product)) AS ProductCode
    FROM [$(ETL_SCHEMA)].[input_certificate_info] ci
    WHERE ci.WritingBrokerID <> ''
      AND ci.Product <> ''
      AND ci.RecStatus = 'A'  -- Only active split configurations
      AND TRY_CAST(REPLACE(ci.WritingBrokerID, 'P', '') AS BIGINT) IS NOT NULL
) cert_products
-- Join to hierarchies via GroupId and BrokerId
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchies] h 
    ON h.GroupId = cert_products.GroupId 
    AND h.BrokerId = cert_products.WritingBrokerId
-- Get the hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_versions] hv 
    ON hv.HierarchyId = h.Id
-- Get the catch-all state rule for this hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_state_rules] sr
    ON sr.HierarchyVersionId = hv.Id
    AND sr.ShortName = 'ALL'  -- Only catch-all rules
-- Get product code metadata (optional)
LEFT JOIN [$(ETL_SCHEMA)].[stg_product_codes] pc 
    ON pc.Code = cert_products.ProductCode;

DECLARE @splits_catch_all INT = @@ROWCOUNT;
PRINT 'Hierarchy splits created (catch-all): ' + CAST(@splits_catch_all AS VARCHAR);

DECLARE @splits_count INT = @splits_state_specific + @splits_catch_all;
PRINT 'Total hierarchy splits created: ' + CAST(@splits_count AS VARCHAR);

-- =============================================================================
-- Step 5: Link HierarchyId to Premium Split Participants
-- The split participants were created in 06-proposals.sql before hierarchies existed
-- Now we can link them to the actual hierarchies
-- =============================================================================
PRINT '';
PRINT 'Step 5: Linking HierarchyId to premium split participants...';

UPDATE psp
SET psp.HierarchyId = h.Id,
    psp.HierarchyName = h.Name
FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] psp
INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_versions] psv ON psv.Id = psp.VersionId
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] p ON p.Id = psv.ProposalId
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchies] h 
    ON h.GroupId = p.GroupId
    AND h.BrokerId = psp.BrokerId
WHERE psp.HierarchyId IS NULL;

DECLARE @split_hier_count INT = @@ROWCOUNT;
PRINT 'Split participants linked to hierarchies: ' + CAST(@split_hier_count AS VARCHAR);

-- Report participants without HierarchyId
DECLARE @missing_hier INT = (
    SELECT COUNT(*) FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] 
    WHERE HierarchyId IS NULL
);
IF @missing_hier > 0
    PRINT 'WARNING: ' + CAST(@missing_hier AS VARCHAR) + ' split participants have no linked hierarchy';

-- =============================================================================
-- Step 6: Create Split Distributions
-- Links each hierarchy split (product) to each participant with their schedule
-- =============================================================================
PRINT '';
PRINT 'Step 6: Creating split distributions...';

TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_split_distributions];

-- For each hierarchy split, create a distribution for each participant in the hierarchy
INSERT INTO [$(ETL_SCHEMA)].[stg_split_distributions] (
    Id, HierarchySplitId, HierarchyParticipantId, ParticipantEntityId,
    Percentage, ScheduleId, ScheduleName, CreationTime, IsDeleted
)
SELECT
    -- Id: {HierarchySplitId}-{ParticipantId}
    CONCAT(hs.Id, '-', hp.Id) AS Id,
    hs.Id AS HierarchySplitId,
    hp.Id AS HierarchyParticipantId,
    hp.EntityId AS ParticipantEntityId,
    -- Percentage: Use the participant's SplitPercent if available, otherwise equal distribution
    COALESCE(hp.SplitPercent, 100.0 / NULLIF(participant_counts.cnt, 0)) AS Percentage,
    -- Resolve ScheduleId from ScheduleCode if ScheduleId is NULL
    COALESCE(hp.ScheduleId, s_resolved.Id) AS ScheduleId,
    COALESCE(s.Name, s_resolved.Name) AS ScheduleName,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_hierarchy_splits] hs
-- Get the state rule to find the hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_state_rules] sr ON sr.Id = hs.StateRuleId
-- Get all participants for this hierarchy version
INNER JOIN [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp ON hp.HierarchyVersionId = sr.HierarchyVersionId
-- Get schedule name (from ScheduleId if available)
LEFT JOIN [$(ETL_SCHEMA)].[stg_schedules] s ON s.Id = hp.ScheduleId
-- Resolve ScheduleId from ScheduleCode if ScheduleId is NULL
LEFT JOIN [$(ETL_SCHEMA)].[stg_schedules] s_resolved ON s_resolved.ExternalId = hp.ScheduleCode AND hp.ScheduleId IS NULL
-- Get participant count for equal distribution fallback
CROSS APPLY (
    SELECT COUNT(*) as cnt
    FROM [$(ETL_SCHEMA)].[stg_hierarchy_participants] hp2
    WHERE hp2.HierarchyVersionId = sr.HierarchyVersionId
) participant_counts;

DECLARE @split_distributions_count INT = @@ROWCOUNT;
PRINT 'Split distributions created: ' + CAST(@split_distributions_count AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'stg_state_rules' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_state_rules]
UNION ALL
SELECT 'stg_state_rule_states' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_state_rule_states]
UNION ALL
SELECT 'stg_hierarchy_splits' AS entity, COUNT(*) AS cnt FROM [$(ETL_SCHEMA)].[stg_hierarchy_splits];

PRINT '';
PRINT 'State rules per hierarchy version (top 10):';
SELECT TOP 10 
       HierarchyVersionId, 
       COUNT(*) AS state_count
FROM [$(ETL_SCHEMA)].[stg_state_rules]
GROUP BY HierarchyVersionId
ORDER BY state_count DESC;

PRINT '';
PRINT 'Splits per state rule (top 10):';
SELECT TOP 10 
       StateRuleId, 
       COUNT(*) AS product_count
FROM [$(ETL_SCHEMA)].[stg_hierarchy_splits]
GROUP BY StateRuleId
ORDER BY product_count DESC;

PRINT '';
PRINT '============================================================';
PRINT 'STATE RULES & HIERARCHY SPLITS TRANSFORM COMPLETED';
PRINT '============================================================';

GO
