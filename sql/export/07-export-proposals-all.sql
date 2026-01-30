-- =====================================================
-- Export ALL Proposals from etl staging to dbo (NO FILTERING)
-- =====================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT 'Exporting ALL Proposals to dbo.Proposals (no conformance filtering)...';

INSERT INTO [dbo].[Proposals] (
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
FROM [etl].[stg_proposals] sp
LEFT JOIN [dbo].[Brokers] b ON b.ExternalPartyId = sp.BrokerUniquePartyId;

DECLARE @proposalCount INT = @@ROWCOUNT;
PRINT '  ✓ Proposals exported: ' + CAST(@proposalCount AS VARCHAR);
PRINT '';

-- Export ProposalProducts
PRINT 'Exporting ProposalProducts...';

SET IDENTITY_INSERT [dbo].[ProposalProducts] ON;

INSERT INTO [dbo].[ProposalProducts] (
    Id, ProposalId, ProductCode, ProductName, CreationTime, IsDeleted
)
SELECT 
    pp.Id, pp.ProposalId, pp.ProductCode, pp.ProductName, 
    COALESCE(pp.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(pp.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_proposal_products] pp
WHERE EXISTS (SELECT 1 FROM [dbo].[Proposals] WHERE Id = pp.ProposalId);

SET IDENTITY_INSERT [dbo].[ProposalProducts] OFF;

DECLARE @ppCount INT = @@ROWCOUNT;
PRINT '  ✓ ProposalProducts exported: ' + CAST(@ppCount AS VARCHAR);
PRINT '';

PRINT '=== Proposal Export Complete ===';

GO
