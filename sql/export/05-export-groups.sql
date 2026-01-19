-- =====================================================
-- Export Groups from etl staging to dbo
-- Only exports groups that don't already exist
-- Maps staging columns to production schema
-- =====================================================

PRINT 'Exporting missing Groups to dbo.Group...';

INSERT INTO [dbo].[Group] (
    Id, GroupNumber, GroupName, StateAbbreviation, SitusState, GroupSize,
    TaxId, IsPublicSector, IsNonConformant, NonConformantDescription,
    CreationTime, IsDeleted
)
SELECT 
    sg.Id,
    sg.Code AS GroupNumber,
    sg.Name AS GroupName,
    sg.[State] AS StateAbbreviation,
    sg.[State] AS SitusState,
    0 AS GroupSize,  -- Will be updated from policy counts
    sg.TaxId,
    0 AS IsPublicSector,  -- Default to false
    COALESCE(sg.IsNonConformant, 0) AS IsNonConformant,
    sg.NonConformantDescription,
    sg.CreationTime,
    COALESCE(sg.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_groups] sg
WHERE sg.Id NOT IN (SELECT Id FROM [dbo].[Group]);

DECLARE @groupCount INT;
SELECT @groupCount = @@ROWCOUNT;
PRINT 'Groups exported: ' + CAST(@groupCount AS VARCHAR);
GO

-- Update GroupSize from policy counts
PRINT 'Updating GroupSize from policy counts...';

UPDATE g
SET g.GroupSize = COALESCE(pc.PolicyCount, 0)
FROM [dbo].[Group] g
LEFT JOIN (
    SELECT GroupId, COUNT(*) AS PolicyCount 
    FROM [dbo].[Policies] 
    GROUP BY GroupId
) pc ON pc.GroupId = g.Id;

PRINT 'GroupSize updated';
GO

DECLARE @totalGroups INT;
SELECT @totalGroups = COUNT(*) FROM [dbo].[Group];
PRINT 'Total groups in dbo: ' + CAST(@totalGroups AS VARCHAR);
GO

PRINT '=== Group Export Complete ===';
