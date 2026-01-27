-- =====================================================
-- DESTRUCTIVE EXPORT: Delete and Re-export Proposal Data
-- This script deletes all proposal-related data and re-exports from staging
-- with improved filters (excludes broken proposals)
-- =====================================================
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

PRINT '============================================================';
PRINT 'DESTRUCTIVE EXPORT: Proposal-Related Data';
PRINT '============================================================';
PRINT '';

-- Show current counts
DECLARE @before_proposals INT;
DECLARE @before_products INT;
DECLARE @before_versions INT;
DECLARE @before_participants INT;

SELECT @before_proposals = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Proposals];
SELECT @before_products = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts];
SELECT @before_versions = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions];
SELECT @before_participants = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitParticipants];

PRINT 'BEFORE DELETION:';
PRINT '  Proposals: ' + CAST(@before_proposals AS VARCHAR);
PRINT '  ProposalProducts: ' + CAST(@before_products AS VARCHAR);
PRINT '  PremiumSplitVersions: ' + CAST(@before_versions AS VARCHAR);
PRINT '  PremiumSplitParticipants: ' + CAST(@before_participants AS VARCHAR);
PRINT '';

-- =====================================================
-- STEP 1: Delete in correct order (respecting foreign keys)
-- =====================================================
PRINT 'STEP 1: Deleting existing proposal-related data...';
PRINT '';

PRINT '  Deleting PremiumSplitParticipants...';
DELETE FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitParticipants];
DECLARE @deleted_participants INT = @@ROWCOUNT;
PRINT '    Deleted: ' + CAST(@deleted_participants AS VARCHAR);
PRINT '';

PRINT '  Deleting PremiumSplitVersions...';
DELETE FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions];
DECLARE @deleted_versions INT = @@ROWCOUNT;
PRINT '    Deleted: ' + CAST(@deleted_versions AS VARCHAR);
PRINT '';

PRINT '  Deleting ProposalProducts...';
DELETE FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts];
DECLARE @deleted_products INT = @@ROWCOUNT;
PRINT '    Deleted: ' + CAST(@deleted_products AS VARCHAR);
PRINT '';

PRINT '  Deleting Proposals...';
DELETE FROM [$(PRODUCTION_SCHEMA)].[Proposals];
DECLARE @deleted_proposals INT = @@ROWCOUNT;
PRINT '    Deleted: ' + CAST(@deleted_proposals AS VARCHAR);
PRINT '';

PRINT 'DELETION COMPLETE';
PRINT '';

-- =====================================================
-- STEP 2: Export Proposals (with improved filters)
-- =====================================================
PRINT '============================================================';
PRINT 'STEP 2: Exporting Proposals (with broken proposal exclusion)';
PRINT '============================================================';
PRINT '';

-- Export Proposals
PRINT 'Exporting Proposals...';
INSERT INTO [$(PRODUCTION_SCHEMA)].[Proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState, BrokerUniquePartyId, BrokerName,
    GroupId, GroupName, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, EnableEffectiveDateFiltering,
    ConstrainingEffectiveDateFrom, ConstrainingEffectiveDateTo,
    CreationTime, IsDeleted
)
SELECT 
    sp.Id,
    sp.ProposalNumber,
    CASE 
        WHEN TRY_CAST(sp.[Status] AS INT) IS NOT NULL THEN TRY_CAST(sp.[Status] AS INT)
        WHEN sp.[Status] = 'Active' THEN 1 
        WHEN sp.[Status] = 'Inactive' THEN 0 
        WHEN sp.[Status] = 'Pending' THEN 2 
        WHEN sp.[Status] = 'Expired' THEN 3 
        ELSE 1  -- Default to Active
    END AS [Status],
    COALESCE(sp.SubmittedDate, GETUTCDATE()) AS SubmittedDate,
    COALESCE(sp.ProposedEffectiveDate, sp.EffectiveDateFrom, GETUTCDATE()) AS ProposedEffectiveDate,
    COALESCE(sp.SpecialCase, 0) AS SpecialCase,
    COALESCE(sp.SpecialCaseCode, 0) AS SpecialCaseCode,
    sp.SitusState,
    sp.BrokerUniquePartyId,
    COALESCE(
        NULLIF(LTRIM(RTRIM(sp.BrokerName)), ''),
        b.Name,
        CONCAT('Broker ', sp.BrokerUniquePartyId)
    ) AS BrokerName,
    sp.GroupId,
    sp.GroupName,
    sp.EffectiveDateFrom,
    sp.EffectiveDateTo,
    COALESCE(sp.EnablePlanCodeFiltering, 0) AS EnablePlanCodeFiltering,
    COALESCE(sp.EnableEffectiveDateFiltering, 0) AS EnableEffectiveDateFiltering,
    sp.ConstrainingEffectiveDateFrom,
    sp.ConstrainingEffectiveDateTo,
    sp.CreationTime,
    sp.IsDeleted
FROM [$(ETL_SCHEMA)].[stg_proposals] sp
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Brokers] b ON b.ExternalPartyId = sp.BrokerUniquePartyId
-- EXCLUDE broken proposals: proposals that have PremiumSplitParticipants without HierarchyId
WHERE NOT EXISTS (
    SELECT 1 
    FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] spsv
    INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp ON spsp.VersionId = spsv.Id
    WHERE spsv.ProposalId = sp.Id
      AND spsp.HierarchyId IS NULL
  );

DECLARE @exported_proposals INT = @@ROWCOUNT;
PRINT '  Exported Proposals: ' + CAST(@exported_proposals AS VARCHAR);
PRINT '';

-- Export ProposalProducts
PRINT 'Exporting ProposalProducts...';

-- Method 1: From stg_proposal_products
INSERT INTO [$(PRODUCTION_SCHEMA)].[ProposalProducts] (
    ProposalId, ProductCode, ProductName, CommissionStructure, ResolvedScheduleId,
    ResolvedScheduleName, CreatedAt
)
SELECT 
    spp.ProposalId,
    spp.ProductCode,
    spp.ProductName,
    spp.CommissionStructure,
    spp.ResolvedScheduleId,
    NULL AS ResolvedScheduleName,
    COALESCE(spp.CreationTime, GETUTCDATE()) AS CreatedAt
FROM [$(ETL_SCHEMA)].[stg_proposal_products] spp
WHERE spp.ProposalId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals]);

DECLARE @exported_products1 INT = @@ROWCOUNT;
PRINT '  Exported ProposalProducts (from stg_proposal_products): ' + CAST(@exported_products1 AS VARCHAR);

-- Method 2: From stg_proposals.ProductCodes JSON
INSERT INTO [$(PRODUCTION_SCHEMA)].[ProposalProducts] (
    ProposalId, ProductCode, ProductName, CommissionStructure, ResolvedScheduleId,
    ResolvedScheduleName, CreatedAt
)
SELECT DISTINCT
    sp.Id AS ProposalId,
    TRIM(pc.[value]) AS ProductCode,
    pr.ProductName AS ProductName,
    NULL AS CommissionStructure,
    NULL AS ResolvedScheduleId,
    NULL AS ResolvedScheduleName,
    GETUTCDATE() AS CreatedAt
FROM [$(ETL_SCHEMA)].[stg_proposals] sp
CROSS APPLY OPENJSON(sp.ProductCodes) AS pc
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Products] pr ON pr.ProductCode = TRIM(pc.[value])
WHERE sp.Id IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals])
  AND sp.ProductCodes IS NOT NULL
  AND sp.ProductCodes != '[]'
  AND sp.ProductCodes != ''
  AND sp.ProductCodes != '*'
  AND ISJSON(sp.ProductCodes) = 1
  AND NOT EXISTS (
    SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts] pp
    WHERE pp.ProposalId = sp.Id
      AND pp.ProductCode = TRIM(pc.[value])
  );

DECLARE @exported_products2 INT = @@ROWCOUNT;
PRINT '  Exported ProposalProducts (from JSON ProductCodes): ' + CAST(@exported_products2 AS VARCHAR);
PRINT '';

-- =====================================================
-- STEP 3: Export PremiumSplitVersions and Participants
-- =====================================================
PRINT '============================================================';
PRINT 'STEP 3: Exporting PremiumSplitVersions and Participants';
PRINT '============================================================';
PRINT '';

-- Export PremiumSplitVersions (with broken version exclusion)
PRINT 'Exporting PremiumSplitVersions...';
INSERT INTO [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions] (
    Id, GroupId, GroupName, ProposalId, VersionNumber,
    EffectiveFrom, EffectiveTo, ChangeDescription, TotalSplitPercent,
    [Status], [Source], HubspotDealId, CreationTime, IsDeleted
)
SELECT 
    spsv.Id,
    TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
    ) AS GroupId,
    spsv.GroupName,
    spsv.ProposalId,
    spsv.VersionNumber,
    spsv.EffectiveFrom,
    spsv.EffectiveTo,
    spsv.ChangeDescription,
    CASE 
        WHEN spsv.TotalSplitPercent > 999.99 THEN 999.99 
        ELSE CAST(spsv.TotalSplitPercent AS DECIMAL(5,2)) 
    END AS TotalSplitPercent,
    COALESCE(spsv.[Status], 1) AS [Status],
    COALESCE(spsv.[Source], 0) AS [Source],
    spsv.HubspotDealId,
    spsv.CreationTime,
    COALESCE(spsv.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] spsv
WHERE TRY_CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            spsv.GroupId,
            'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),
            'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),
            'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),'U',''),
            'V',''),'W',''),'X',''),'Y',''),'Z','')
        AS BIGINT
      ) IS NOT NULL
  -- EXCLUDE broken split versions: versions that have participants without HierarchyId
  AND NOT EXISTS (
    SELECT 1 
    FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp
    WHERE spsp.VersionId = spsv.Id
      AND spsp.HierarchyId IS NULL
  )
  AND spsv.ProposalId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals]);  -- Only export versions for exported proposals

DECLARE @exported_versions INT = @@ROWCOUNT;
PRINT '  Exported PremiumSplitVersions: ' + CAST(@exported_versions AS VARCHAR);
PRINT '';

-- Export PremiumSplitParticipants (only those with HierarchyId)
PRINT 'Exporting PremiumSplitParticipants...';
INSERT INTO [$(PRODUCTION_SCHEMA)].[PremiumSplitParticipants] (
    Id, VersionId, BrokerUniquePartyId, BrokerName, BrokerNPN, SplitPercent,
    IsWritingAgent, HierarchyId, HierarchyName, TemplateId, TemplateName,
    EffectiveFrom, EffectiveTo, Notes
)
SELECT 
    spsp.Id,
    spsp.VersionId,
    spsp.BrokerUniquePartyId,
    spsp.BrokerName,
    spsp.BrokerNPN,
    spsp.SplitPercent,
    COALESCE(spsp.IsWritingAgent, 0) AS IsWritingAgent,
    spsp.HierarchyId,
    spsp.HierarchyName,
    spsp.TemplateId,
    spsp.TemplateName,
    spsp.EffectiveFrom,
    spsp.EffectiveTo,
    spsp.Notes
FROM [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp
WHERE spsp.VersionId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[PremiumSplitVersions])
  AND spsp.HierarchyId IS NOT NULL;  -- EXCLUDE broken participants: only export participants with HierarchyId

DECLARE @exported_participants INT = @@ROWCOUNT;
PRINT '  Exported PremiumSplitParticipants: ' + CAST(@exported_participants AS VARCHAR);
PRINT '';

-- =====================================================
-- STEP 4: Summary
-- =====================================================
PRINT '============================================================';
PRINT 'EXPORT COMPLETE';
PRINT '============================================================';
PRINT '';
PRINT 'DELETED:';
PRINT '  Proposals: ' + CAST(@deleted_proposals AS VARCHAR);
PRINT '  ProposalProducts: ' + CAST(@deleted_products AS VARCHAR);
PRINT '  PremiumSplitVersions: ' + CAST(@deleted_versions AS VARCHAR);
PRINT '  PremiumSplitParticipants: ' + CAST(@deleted_participants AS VARCHAR);
PRINT '';
PRINT 'EXPORTED:';
PRINT '  Proposals: ' + CAST(@exported_proposals AS VARCHAR);
PRINT '  ProposalProducts: ' + CAST(@exported_products1 + @exported_products2 AS VARCHAR);
PRINT '  PremiumSplitVersions: ' + CAST(@exported_versions AS VARCHAR);
PRINT '  PremiumSplitParticipants: ' + CAST(@exported_participants AS VARCHAR);
PRINT '';
PRINT 'BROKEN PROPOSALS EXCLUDED: ' + CAST(@before_proposals - @exported_proposals AS VARCHAR);
PRINT '';

GO
