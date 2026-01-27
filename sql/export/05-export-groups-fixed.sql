-- =====================================================
-- Export Groups from etl staging to dbo
-- Fixed to match production schema exactly
-- =====================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

PRINT 'Exporting Groups to dbo.EmployerGroups...';

INSERT INTO [dbo].[EmployerGroups] (
    Id, GroupNumber, GroupName, StateAbbreviation, SitusState, GroupSize,
    TaxId, PrimaryBrokerId, IsPublicSector, IsNonConformant, NonConformantDescription,
    NextProposalNumber, CreationTime, IsDeleted
)
SELECT 
    sg.Id,
    sg.Code AS GroupNumber,
    sg.Name AS GroupName,
    sg.[State] AS StateAbbreviation,
    sg.[State] AS SitusState,
    0 AS GroupSize,  -- Will be updated later
    sg.TaxId,
    NULL AS PrimaryBrokerId,  -- Will be set later
    0 AS IsPublicSector,
    COALESCE(sg.IsNonConformant, 0) AS IsNonConformant,
    sg.NonConformantDescription,
    1 AS NextProposalNumber,
    COALESCE(sg.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sg.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_groups] sg
WHERE sg.Id NOT IN (SELECT Id FROM [dbo].[EmployerGroups])
  AND sg.Id IN (SELECT Id FROM [etl].[stg_included_groups]);

DECLARE @excluded_cnt INT;
SELECT @excluded_cnt = COUNT(*) FROM [etl].[stg_excluded_groups];
PRINT 'Note: Excluded ' + CAST(@excluded_cnt AS VARCHAR) + ' groups (7-series)';

DECLARE @groupCount INT;
SELECT @groupCount = @@ROWCOUNT;
PRINT 'Groups exported: ' + CAST(@groupCount AS VARCHAR);
GO

DECLARE @totalGroups INT;
SELECT @totalGroups = COUNT(*) FROM [dbo].[EmployerGroups];
PRINT 'Total groups in dbo: ' + CAST(@totalGroups AS VARCHAR);
GO

PRINT '=== Group Export Complete ===';
