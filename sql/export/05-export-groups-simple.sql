-- =====================================================
-- Export Groups from etl staging to dbo.EmployerGroups
-- Simple mapping - only uses available columns from stg_groups
-- =====================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT 'Exporting Groups to dbo.EmployerGroups...';

INSERT INTO [dbo].[EmployerGroups] (
    Id, 
    GroupNumber, 
    GroupName, 
    StateAbbreviation, 
    SitusState, 
    GroupSize,
    TaxId, 
    PrimaryBrokerId,
    IsPublicSector, 
    IsNonConformant, 
    NonConformantDescription,
    NextProposalNumber, 
    CreationTime, 
    IsDeleted
)
SELECT 
    sg.Id,
    sg.Code AS GroupNumber,
    sg.Name AS GroupName,
    sg.[State] AS StateAbbreviation,
    sg.[State] AS SitusState,
    0 AS GroupSize,  -- Default, will be updated later if needed
    sg.TaxId,
    sg.PrimaryBrokerId,
    0 AS IsPublicSector,  -- Default to false
    COALESCE(sg.IsNonConformant, 0) AS IsNonConformant,
    sg.NonConformantDescription,
    1 AS NextProposalNumber,  -- Default
    COALESCE(sg.CreationTime, GETDATE()) AS CreationTime,
    COALESCE(sg.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_groups] sg
WHERE sg.Id NOT IN (SELECT Id FROM [dbo].[EmployerGroups])
  -- Exclude groups flagged in stg_excluded_groups
  AND sg.Id NOT IN (SELECT GroupId FROM [etl].[stg_excluded_groups]);

DECLARE @groupCount INT = @@ROWCOUNT;
DECLARE @excluded_count INT;
SELECT @excluded_count = COUNT(*) FROM [etl].[stg_excluded_groups];
PRINT '  ✓ Groups exported: ' + CAST(@groupCount AS VARCHAR);
PRINT '  ⚠️  Groups excluded: ' + CAST(@excluded_count AS VARCHAR) + ' (Universal Trucking + DTC groups)';
PRINT '';

PRINT '=== Group Export Complete ===';

GO
