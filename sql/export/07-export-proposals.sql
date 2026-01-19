-- =====================================================
-- Export Proposals from etl staging to dbo
-- Only exports proposals that don't already exist
-- =====================================================

PRINT 'Exporting missing Proposals to dbo.Proposals...';

INSERT INTO [dbo].[Proposals] (
    Id, ProposalNumber, [Status], SubmittedDate, ProposedEffectiveDate,
    SpecialCase, SpecialCaseCode, SitusState, BrokerId, BrokerName,
    GroupId, GroupName, EffectiveDateFrom, EffectiveDateTo,
    EnablePlanCodeFiltering, EnableEffectiveDateFiltering,
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
    sp.BrokerId,
    sp.BrokerName,
    sp.GroupId,
    sp.GroupName,
    sp.EffectiveDateFrom,
    sp.EffectiveDateTo,
    0 AS EnablePlanCodeFiltering,
    0 AS EnableEffectiveDateFiltering,
    sp.CreationTime,
    sp.IsDeleted
FROM [etl].[stg_proposals] sp
WHERE sp.Id NOT IN (SELECT Id FROM [dbo].[Proposals]);

DECLARE @proposalCount INT;
SELECT @proposalCount = @@ROWCOUNT;
PRINT 'Proposals exported: ' + CAST(@proposalCount AS VARCHAR);

DECLARE @totalProposals INT;
SELECT @totalProposals = COUNT(*) FROM [dbo].[Proposals];
PRINT 'Total proposals in dbo: ' + CAST(@totalProposals AS VARCHAR);
GO

PRINT 'Exporting missing ProposalProducts to dbo.ProposalProducts...';

INSERT INTO [dbo].[ProposalProducts] (
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
FROM [etl].[stg_proposal_products] spp
WHERE spp.ProposalId IN (SELECT Id FROM [dbo].[Proposals])
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[ProposalProducts] pp
    WHERE pp.ProposalId = spp.ProposalId
      AND pp.ProductCode = spp.ProductCode
);

DECLARE @productCount INT;
SELECT @productCount = @@ROWCOUNT;
PRINT 'ProposalProducts exported: ' + CAST(@productCount AS VARCHAR);

DECLARE @totalProducts INT;
SELECT @totalProducts = COUNT(*) FROM [dbo].[ProposalProducts];
PRINT 'Total proposal products in dbo: ' + CAST(@totalProducts AS VARCHAR);
GO

PRINT '=== Proposal Export Complete ===';

