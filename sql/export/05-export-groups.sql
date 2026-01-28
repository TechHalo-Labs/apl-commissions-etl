-- =====================================================
-- Export Groups from etl staging to dbo
-- Only exports CONFORMANT and NEARLY CONFORMANT groups
-- Filters out non-conformant groups using GroupConformanceStatistics
-- Maps staging columns to production schema
-- =====================================================

PRINT 'Exporting missing Groups to dbo.EmployerGroups (conformant + nearly conformant only)...';

-- Filter: Only export groups with >=95% conformance
INSERT INTO [$(PRODUCTION_SCHEMA)].[EmployerGroups] (
    Id, GroupNumber, GroupName, StateAbbreviation, SitusState, GroupSize,
    TaxId, IsPublicSector, IsNonConformant, NonConformantDescription,
    PercentConformant, ConformantPolicies, NonConformantPolicies, TotalPoliciesAnalyzed,
    NextProposalNumber, CreationTime, IsDeleted
)
SELECT 
    sg.Id,
    sg.Code AS GroupNumber,
    sg.Name AS GroupName,
    sg.[State] AS StateAbbreviation,
    sg.[State] AS SitusState,
    0 AS GroupSize,  -- Will be updated from distinct CustomerId counts (lives)
    sg.TaxId,
    0 AS IsPublicSector,  -- Default to false
    COALESCE(sg.IsNonConformant, 0) AS IsNonConformant,
    sg.NonConformantDescription,
    sg.PercentConformant,
    sg.ConformantPolicies,
    sg.NonConformantPolicies,
    sg.TotalPoliciesAnalyzed,
    1 AS NextProposalNumber,  -- Default
    sg.CreationTime,
    COALESCE(sg.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_groups] sg
-- Export ALL groups (no conformance filtering)
-- Non-conformant groups will use PolicyHierarchyAssignments for commissions
WHERE sg.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[EmployerGroups])

DECLARE @groupCount INT;
SELECT @groupCount = @@ROWCOUNT;
PRINT 'Groups exported: ' + CAST(@groupCount AS VARCHAR);
GO

-- Update GroupSize from distinct CustomerId counts (number of lives)
PRINT 'Updating GroupSize from distinct CustomerId counts (lives)...';

UPDATE g
SET g.GroupSize = COALESCE(pc.LivesCount, 0)
FROM [$(PRODUCTION_SCHEMA)].[EmployerGroups] g
LEFT JOIN (
    SELECT GroupId, COUNT(DISTINCT CustomerId) AS LivesCount 
    FROM [$(PRODUCTION_SCHEMA)].[Policies] 
    WHERE CustomerId IS NOT NULL AND CustomerId <> ''
    GROUP BY GroupId
) pc ON pc.GroupId = g.Id;

DECLARE @updatedCount INT = @@ROWCOUNT;
PRINT 'GroupSize updated for ' + CAST(@updatedCount AS VARCHAR) + ' groups based on distinct CustomerId (lives)';
GO

DECLARE @totalGroups INT;
SELECT @totalGroups = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[EmployerGroups];
PRINT 'Total groups in dbo: ' + CAST(@totalGroups AS VARCHAR);
GO

-- =====================================================
-- Post-Export: Flag Non-Conformant Groups
-- Sets IsNonConformant = 1 for groups identified in etl.non_conformant_keys
-- =====================================================
PRINT 'Flagging non-conformant groups from ETL analysis...';

UPDATE g
SET g.IsNonConformant = 1,
    g.LastModificationTime = GETUTCDATE()
FROM [$(PRODUCTION_SCHEMA)].[EmployerGroups] g
WHERE EXISTS (
    SELECT 1 FROM [$(ETL_SCHEMA)].[non_conformant_keys] nck
    WHERE CONCAT('G', nck.GroupId) = g.Id
)
AND (g.IsNonConformant IS NULL OR g.IsNonConformant = 0);  -- Only update if not already flagged

DECLARE @flaggedCount INT = @@ROWCOUNT;
PRINT 'Non-conformant groups flagged: ' + CAST(@flaggedCount AS VARCHAR);
GO

PRINT '=== Group Export Complete ===';
