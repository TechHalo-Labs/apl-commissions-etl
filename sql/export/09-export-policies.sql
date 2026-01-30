-- =====================================================
-- Export Policies from etl staging to dbo
-- Exports ALL policies (conformance filtering disabled)
-- =====================================================

PRINT 'Exporting missing Policies to dbo.Policies (all policies)...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[Policies] (
    Id, PolicyNumber, CertificateNumber, OldPolicyNumber, PolicyType, [Status],
    StatusDate, BrokerId, ContractId, GroupId, CarrierName, CarrierId,
    ProductCode, ProductName, PlanCode, PlanName, MasterCategory, Category,
    InsuredName, InsuredFirstName, InsuredLastName, Premium, FaceAmount,
    PayMode, Frequency, EffectiveDate, IssueDate, ExpirationDate,
    [State], Division, CompanyCode, LionRecordNumber, CustomerId,
    PaidThroughDate,
    ProposalId, ProposalAssignedAt, ProposalAssignmentSource,
    CreationTime, IsDeleted
)
SELECT 
    sp.Id,
    sp.PolicyNumber,
    sp.CertificateNumber,
    sp.OldPolicyNumber,
    COALESCE(sp.PolicyType, 0) AS PolicyType,
    COALESCE(sp.[Status], 1) AS [Status],  -- 1 = Active
    sp.StatusDate,
    COALESCE(sp.BrokerId, 0) AS BrokerId,
    sp.ContractId,
    -- Add G-prefix to ALL GroupIds (including DTC 00000 → G00000)
    CASE 
        WHEN sp.GroupId IS NULL OR sp.GroupId = '' THEN NULL
        ELSE CONCAT('G', sp.GroupId)
    END AS GroupId,
    COALESCE(sp.CarrierName, 'APL') AS CarrierName,
    sp.CarrierId,
    COALESCE(sp.ProductCode, 'UNKNOWN') AS ProductCode,
    COALESCE(sp.ProductName, 'Unknown Product') AS ProductName,
    sp.PlanCode,
    sp.PlanName,
    sp.MasterCategory,
    sp.Category,
    COALESCE(sp.InsuredName, 'Unknown') AS InsuredName,
    sp.InsuredFirstName,
    sp.InsuredLastName,
    COALESCE(sp.Premium, 0) AS Premium,
    COALESCE(sp.FaceAmount, 0) AS FaceAmount,
    sp.PayMode,
    sp.Frequency,
    COALESCE(sp.EffectiveDate, '2020-01-01') AS EffectiveDate,
    sp.IssueDate,
    sp.ExpirationDate,
    sp.[State],
    sp.Division,
    sp.CompanyCode,
    sp.LionRecordNumber,
    sp.CustomerId,
    sp.PaidThroughDate,
    sp.ProposalId,
    sp.ProposalAssignedAt,
    sp.ProposalAssignmentSource,
    COALESCE(sp.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sp.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_policies] sp
WHERE sp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Policies])
  -- Exclude groups flagged in stg_excluded_groups
  AND (
    sp.GroupId IS NULL 
    OR sp.GroupId = ''
    OR CONCAT('G', sp.GroupId) NOT IN (SELECT GroupId FROM [$(ETL_SCHEMA)].[stg_excluded_groups])
  )
  -- Export ALL policies (conformance analysis disabled - all staging data validated)


DECLARE @policyCount INT;
SELECT @policyCount = @@ROWCOUNT;
PRINT 'Policies exported: ' + CAST(@policyCount AS VARCHAR);

DECLARE @totalPolicies INT;
SELECT @totalPolicies = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Policies];
PRINT 'Total policies in dbo: ' + CAST(@totalPolicies AS VARCHAR);
GO

PRINT '=== Policy Export Complete ===';

