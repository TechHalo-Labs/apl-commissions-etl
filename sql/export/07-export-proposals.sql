-- =====================================================
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
-- Export Proposals from etl staging to dbo
-- Only exports proposals that don't already exist
-- =====================================================

PRINT 'Exporting missing Proposals to dbo.Proposals...';

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
    sp.BrokerUniquePartyId,  -- NEW: Use BrokerUniquePartyId instead of BrokerId
    -- Populate BrokerName from Brokers table using ExternalPartyId
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
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Brokers] b ON b.ExternalPartyId = sp.BrokerUniquePartyId  -- NEW: Join on ExternalPartyId
WHERE sp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals])
  -- Removed stg_included_groups filter - table may not exist and prevents export of fixed proposals
  AND sp.BrokerUniquePartyId IS NOT NULL  -- Only export proposals with valid broker reference
  -- EXCLUDE broken proposals: proposals that have PremiumSplitParticipants without HierarchyId
  AND NOT EXISTS (
    SELECT 1 
    FROM [$(ETL_SCHEMA)].[stg_premium_split_versions] spsv
    INNER JOIN [$(ETL_SCHEMA)].[stg_premium_split_participants] spsp ON spsp.VersionId = spsv.Id
    WHERE spsv.ProposalId = sp.Id
      AND spsp.HierarchyId IS NULL
  );

DECLARE @proposalCount INT;
SELECT @proposalCount = @@ROWCOUNT;
PRINT 'Proposals exported: ' + CAST(@proposalCount AS VARCHAR);

DECLARE @totalProposals INT;
SELECT @totalProposals = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Proposals];
PRINT 'Total proposals in dbo: ' + CAST(@totalProposals AS VARCHAR);
GO

-- =====================================================
-- Update EffectiveDateTo for existing proposals
-- This ensures proposals with multiple EffectiveDateFrom values
-- have their EffectiveDateTo properly set
-- =====================================================
PRINT 'Updating EffectiveDateTo for existing proposals...';

UPDATE p
SET 
    p.EffectiveDateTo = sp.EffectiveDateTo,
    p.LastModificationTime = GETUTCDATE()
FROM [$(PRODUCTION_SCHEMA)].[Proposals] p
INNER JOIN [$(ETL_SCHEMA)].[stg_proposals] sp ON sp.Id = p.Id
WHERE sp.EffectiveDateTo IS NOT NULL
    AND (
        p.EffectiveDateTo IS NULL 
        OR p.EffectiveDateTo != sp.EffectiveDateTo
    );

DECLARE @updated_count INT = @@ROWCOUNT;
PRINT 'Updated EffectiveDateTo for ' + CAST(@updated_count AS VARCHAR) + ' existing proposals.';
GO

-- =====================================================
-- Update BrokerName for existing proposals (populate from Brokers table if NULL)
-- =====================================================
PRINT 'Updating BrokerName for existing proposals with NULL/empty names...';

UPDATE p
SET 
    p.BrokerName = COALESCE(
        NULLIF(LTRIM(RTRIM(b.Name)), ''),
        CONCAT('Broker ', p.BrokerUniquePartyId)
    ),
    p.LastModificationTime = GETUTCDATE()
FROM [$(PRODUCTION_SCHEMA)].[Proposals] p
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Brokers] b ON b.ExternalPartyId = p.BrokerUniquePartyId  -- NEW: Join on ExternalPartyId
WHERE p.BrokerUniquePartyId IS NOT NULL
    AND (
        p.BrokerName IS NULL 
        OR LTRIM(RTRIM(p.BrokerName)) = ''
        OR p.BrokerName = CONCAT('Broker ', p.BrokerUniquePartyId)  -- Update placeholder names too
    )
    AND b.Name IS NOT NULL
    AND LTRIM(RTRIM(b.Name)) <> '';

DECLARE @broker_name_updated INT = @@ROWCOUNT;
PRINT 'Updated BrokerName for ' + CAST(@broker_name_updated AS VARCHAR) + ' existing proposals.';
GO

PRINT 'Exporting missing ProposalProducts to dbo.ProposalProducts...';

-- Method 1: From stg_proposal_products (unconsolidated proposals that match production)
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
WHERE spp.ProposalId IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals])
  AND NOT EXISTS (
    SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts] pp
    WHERE pp.ProposalId = spp.ProposalId
      AND pp.ProductCode = spp.ProductCode
);

DECLARE @productCount1 INT;
SELECT @productCount1 = @@ROWCOUNT;
PRINT 'ProposalProducts exported (from stg_proposal_products): ' + CAST(@productCount1 AS VARCHAR);

-- Method 2: From stg_proposals.ProductCodes JSON (consolidated proposals)
-- This handles consolidated proposals (-CS1, -CS2, etc.) that have ProductCodes stored as JSON
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
WHERE sp.Id IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Proposals])  -- Proposal exists in production
  AND sp.ProductCodes IS NOT NULL
  AND sp.ProductCodes != '[]'
  AND sp.ProductCodes != ''
  AND sp.ProductCodes != '*'
  AND ISJSON(sp.ProductCodes) = 1  -- Skip invalid JSON
  AND NOT EXISTS (
    SELECT 1 FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts] pp
    WHERE pp.ProposalId = sp.Id
      AND pp.ProductCode = TRIM(pc.[value])
);

DECLARE @productCount2 INT;
SELECT @productCount2 = @@ROWCOUNT;
PRINT 'ProposalProducts exported (from JSON ProductCodes): ' + CAST(@productCount2 AS VARCHAR);

DECLARE @totalProducts INT;
SELECT @totalProducts = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[ProposalProducts];
PRINT 'Total proposal products in dbo: ' + CAST(@totalProducts AS VARCHAR);
GO

PRINT '=== Proposal Export Complete ===';

