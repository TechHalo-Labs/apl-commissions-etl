-- =============================================================================
-- Commission Calculation Pipeline - Master Script (SQL Server)
-- =============================================================================
-- 8-Stage cascading INSERT INTO...SELECT FROM commission calculation
-- 
-- Stages:
--   1. Premium Context    - Enrich premiums with policy/group data
--   2. Proposals Resolved - Match proposal by GroupId + date range
--   3. Splits Applied     - Explode by split participants (1:N)
--   4. Hierarchies Resolved - Find active hierarchy version
--   5. Participants Expanded - Explode by tier level (1:N)
--   6. Rates Applied      - Lookup commission rates (CertRate > ParticipantRate > Schedule)
--   7. Commissions Calculated - Calculate amounts: SplitPremium × (Rate / 100)
--   8. Assignments Applied - Handle transfers/assignments
--
-- Output:
--   - GL Journal Entries (Original + Assigned)
--   - Traceability Reports (one per premium)
--   - Broker Traceabilities (one per GL entry)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'COMMISSION CALCULATION PIPELINE';
PRINT '============================================================';
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '';

-- =============================================================================
-- Stage 0: Truncate all calculation tables
-- =============================================================================
PRINT 'Stage 0: Truncating calculation tables...';

TRUNCATE TABLE [etl].[calc_1_premium_context];
TRUNCATE TABLE [etl].[calc_2_proposals_resolved];
TRUNCATE TABLE [etl].[calc_3_splits_applied];
TRUNCATE TABLE [etl].[calc_4_hierarchies_resolved];
TRUNCATE TABLE [etl].[calc_5_participants_expanded];
TRUNCATE TABLE [etl].[calc_6_rates_applied];
TRUNCATE TABLE [etl].[calc_7_commissions_calculated];
TRUNCATE TABLE [etl].[calc_8_assignments_applied];
TRUNCATE TABLE [etl].[calc_gl_journal_entries];
TRUNCATE TABLE [etl].[calc_traceability];
TRUNCATE TABLE [etl].[calc_broker_traceabilities];

PRINT 'All calculation tables truncated.';
PRINT '';

GO

-- =============================================================================
-- Stage 1: Premium Context
-- Enriches premium transactions with policy and group context
-- =============================================================================
PRINT 'Stage 1: Premium Context...';

INSERT INTO [etl].[calc_1_premium_context] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate,
    GroupSize, IsFirstYear, BasisYear
)
SELECT 
    pt.Id AS PremiumTransactionId,
    pt.CertificateId,
    pt.TransactionDate,
    pt.PremiumAmount,
    p.GroupId,
    p.ProductCode,
    p.[State],
    p.EffectiveDate AS CertificateEffectiveDate,
    TRY_CAST(pg.GroupSize AS INT) AS GroupSize,
    CASE WHEN pt.TransactionDate < DATEADD(YEAR, 1, p.EffectiveDate) THEN 1 ELSE 0 END AS IsFirstYear,
    IIF(DATEDIFF(YEAR, p.EffectiveDate, pt.TransactionDate) + 1 < 1, 1, DATEDIFF(YEAR, p.EffectiveDate, pt.TransactionDate) + 1) AS BasisYear
FROM [etl].[stg_premium_transactions] pt
INNER JOIN [etl].[stg_policies] p ON p.Id = CAST(pt.CertificateId AS NVARCHAR(100))
LEFT JOIN [etl].[raw_perf_groups] pg ON pg.GroupNum = REPLACE(p.GroupId, 'G', '')
WHERE pt.PremiumAmount > 0;

PRINT 'Stage 1 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_1_rows FROM [etl].[calc_1_premium_context];

GO

-- =============================================================================
-- Stage 2: Proposals Resolved
-- Match proposal by GroupId + date range
-- =============================================================================
PRINT '';
PRINT 'Stage 2: Proposals Resolved...';

INSERT INTO [etl].[calc_2_proposals_resolved] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode, ErrorMessage
)
SELECT 
    c1.PremiumTransactionId, c1.CertificateId, c1.TransactionDate, c1.PremiumAmount,
    c1.GroupId, c1.ProductCode, c1.[State], c1.CertificateEffectiveDate, c1.GroupSize,
    c1.IsFirstYear, c1.BasisYear,
    pr.Id AS ProposalId, 
    pr.BrokerId AS ProposalBrokerId, 
    pr.SpecialCaseCode,
    CASE WHEN pr.Id IS NULL THEN CONCAT('No proposal for GroupId=', c1.GroupId) ELSE NULL END AS ErrorMessage
FROM [etl].[calc_1_premium_context] c1
LEFT JOIN [etl].[stg_proposals] pr 
    ON pr.GroupId = c1.GroupId
    AND c1.TransactionDate >= pr.EffectiveDateFrom
    AND (pr.EffectiveDateTo IS NULL OR c1.TransactionDate <= pr.EffectiveDateTo);

PRINT 'Stage 2 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_2_rows FROM [etl].[calc_2_proposals_resolved];

GO

-- =============================================================================
-- Stage 3: Splits Applied
-- Explode premiums into split participants (1:N explosion)
-- =============================================================================
PRINT '';
PRINT 'Stage 3: Splits Applied...';

INSERT INTO [etl].[calc_3_splits_applied] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount
)
SELECT 
    c2.PremiumTransactionId, c2.CertificateId, c2.TransactionDate, c2.PremiumAmount,
    c2.GroupId, c2.ProductCode, c2.[State], c2.CertificateEffectiveDate, c2.GroupSize,
    c2.IsFirstYear, c2.BasisYear, c2.ProposalId, c2.ProposalBrokerId, c2.SpecialCaseCode,
    psp.Sequence AS SplitSequence, 
    psp.SplitPercent, 
    psp.HierarchyId, 
    psp.WritingBrokerId,
    ROUND(c2.PremiumAmount * (psp.SplitPercent / 100.0), 2) AS SplitPremiumAmount
FROM [etl].[calc_2_proposals_resolved] c2
INNER JOIN [etl].[stg_premium_split_versions] psv 
    ON psv.ProposalId = c2.ProposalId 
    AND psv.[Status] = 1  -- Active
    AND c2.TransactionDate >= psv.EffectiveFrom
    AND (psv.EffectiveTo IS NULL OR c2.TransactionDate <= psv.EffectiveTo)
INNER JOIN [etl].[stg_premium_split_participants] psp ON psp.VersionId = psv.Id
WHERE c2.ProposalId IS NOT NULL;

PRINT 'Stage 3 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_3_rows FROM [etl].[calc_3_splits_applied];

GO

-- =============================================================================
-- Stage 4: Hierarchies Resolved
-- Find active hierarchy version for each split
-- =============================================================================
PRINT '';
PRINT 'Stage 4: Hierarchies Resolved...';

INSERT INTO [etl].[calc_4_hierarchies_resolved] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount,
    HierarchyVersionId, HierarchyEffectiveFrom
)
SELECT 
    c3.PremiumTransactionId, c3.CertificateId, c3.TransactionDate, c3.PremiumAmount,
    c3.GroupId, c3.ProductCode, c3.[State], c3.CertificateEffectiveDate, c3.GroupSize,
    c3.IsFirstYear, c3.BasisYear, c3.ProposalId, c3.ProposalBrokerId, c3.SpecialCaseCode,
    c3.SplitSequence, c3.SplitPercent, c3.HierarchyId, c3.WritingBrokerId, c3.SplitPremiumAmount,
    hv.Id AS HierarchyVersionId,
    hv.EffectiveFrom AS HierarchyEffectiveFrom
FROM [etl].[calc_3_splits_applied] c3
LEFT JOIN [etl].[stg_hierarchy_versions] hv 
    ON hv.HierarchyId = c3.HierarchyId
    AND hv.[Status] = 1;  -- Active

PRINT 'Stage 4 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_4_rows FROM [etl].[calc_4_hierarchies_resolved];

GO

-- =============================================================================
-- Stage 5: Participants Expanded
-- Get all hierarchy participants (another 1:N explosion)
-- =============================================================================
PRINT '';
PRINT 'Stage 5: Participants Expanded...';

INSERT INTO [etl].[calc_5_participants_expanded] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount,
    HierarchyVersionId, HierarchyEffectiveFrom,
    HierarchyParticipantId, BrokerId, BrokerName, TierLevel, ScheduleCode, PaidBrokerId, ParticipantCommissionRate
)
SELECT 
    c4.PremiumTransactionId, c4.CertificateId, c4.TransactionDate, c4.PremiumAmount,
    c4.GroupId, c4.ProductCode, c4.[State], c4.CertificateEffectiveDate, c4.GroupSize,
    c4.IsFirstYear, c4.BasisYear, c4.ProposalId, c4.ProposalBrokerId, c4.SpecialCaseCode,
    c4.SplitSequence, c4.SplitPercent, c4.HierarchyId, c4.WritingBrokerId, c4.SplitPremiumAmount,
    c4.HierarchyVersionId, c4.HierarchyEffectiveFrom,
    hp.Id AS HierarchyParticipantId,
    hp.EntityId AS BrokerId,
    hp.EntityName AS BrokerName,
    hp.[Level] AS TierLevel,
    hp.ScheduleCode,
    hp.PaidBrokerId,
    hp.CommissionRate AS ParticipantCommissionRate
FROM [etl].[calc_4_hierarchies_resolved] c4
INNER JOIN [etl].[stg_hierarchy_participants] hp 
    ON hp.HierarchyVersionId = c4.HierarchyVersionId;

PRINT 'Stage 5 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_5_rows FROM [etl].[calc_5_participants_expanded];

GO

-- =============================================================================
-- Stage 6: Rates Applied
-- Priority: CertificateRate > ParticipantRate > ScheduleRate
-- =============================================================================
PRINT '';
PRINT 'Stage 6: Rates Applied...';

INSERT INTO [etl].[calc_6_rates_applied] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount,
    HierarchyVersionId, HierarchyEffectiveFrom,
    HierarchyParticipantId, BrokerId, BrokerName, TierLevel, ScheduleCode, PaidBrokerId, ParticipantCommissionRate,
    RatePercent, RateSource, ScheduleId, ScheduleVersionId
)
SELECT 
    c5.PremiumTransactionId, c5.CertificateId, c5.TransactionDate, c5.PremiumAmount,
    c5.GroupId, c5.ProductCode, c5.[State], c5.CertificateEffectiveDate, c5.GroupSize,
    c5.IsFirstYear, c5.BasisYear, c5.ProposalId, c5.ProposalBrokerId, c5.SpecialCaseCode,
    c5.SplitSequence, c5.SplitPercent, c5.HierarchyId, c5.WritingBrokerId, c5.SplitPremiumAmount,
    c5.HierarchyVersionId, c5.HierarchyEffectiveFrom,
    c5.HierarchyParticipantId, c5.BrokerId, c5.BrokerName, c5.TierLevel, c5.ScheduleCode, c5.PaidBrokerId, c5.ParticipantCommissionRate,
    -- Priority: Certificate rate -> Participant rate -> Schedule rate
    COALESCE(
        certRate.CommissionRate,
        c5.ParticipantCommissionRate,
        schedRate.Rate,
        0
    ) AS RatePercent,
    CASE 
        WHEN certRate.CommissionRate IS NOT NULL THEN 'CertificateRate'
        WHEN c5.ParticipantCommissionRate IS NOT NULL AND c5.ParticipantCommissionRate > 0 THEN 'ParticipantRate'
        WHEN schedRate.Rate IS NOT NULL THEN 'ScheduleLookup'
        ELSE 'NoRate'
    END AS RateSource,
    schedRate.ScheduleId,
    schedRate.ScheduleVersionId
FROM [etl].[calc_5_participants_expanded] c5
-- Certificate-level rates (authoritative for bootstrap)
LEFT JOIN (
    SELECT 
        CertificateId, 
        SplitBrokerId, 
        MIN(TRY_CAST(RealCommissionRate AS DECIMAL(18,4))) AS CommissionRate
    FROM [etl].[input_commission_details]
    WHERE TRY_CAST(RealCommissionRate AS DECIMAL(18,4)) > 0
    GROUP BY CertificateId, SplitBrokerId
) certRate ON certRate.CertificateId = c5.CertificateId 
          AND certRate.SplitBrokerId = CONCAT('P', CAST(c5.BrokerId AS VARCHAR))
-- Schedule rates
LEFT JOIN (
    SELECT 
        s.Id AS ScheduleId, 
        s.ExternalId AS ScheduleCode, 
        sv.Id AS ScheduleVersionId,
        sr.ProductCode, 
        sr.[State], 
        sr.GroupSizeFrom, 
        sr.GroupSizeTo,
        sr.FirstYearRate,
        sr.RenewalRate,
        COALESCE(sr.FirstYearRate, sr.RenewalRate) AS Rate
    FROM [etl].[stg_schedules] s
    INNER JOIN [etl].[stg_schedule_versions] sv ON sv.ScheduleId = s.Id AND sv.[Status] = 1
    INNER JOIN [etl].[stg_schedule_rates] sr ON sr.ScheduleVersionId = sv.Id 
    WHERE (sr.FirstYearRate IS NOT NULL AND sr.FirstYearRate > 0) 
       OR (sr.RenewalRate IS NOT NULL AND sr.RenewalRate > 0)
) schedRate ON schedRate.ScheduleCode = c5.ScheduleCode 
           AND schedRate.ProductCode = c5.ProductCode
           AND schedRate.[State] = c5.[State]
           AND (c5.GroupSize IS NULL OR c5.GroupSize = 0 
                OR c5.GroupSize BETWEEN COALESCE(schedRate.GroupSizeFrom, 0) AND COALESCE(schedRate.GroupSizeTo, 999999));

PRINT 'Stage 6 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_6_rows FROM [etl].[calc_6_rates_applied];

GO

-- =============================================================================
-- Stage 7: Commissions Calculated
-- Commission = SplitPremium × (Rate / 100)
-- =============================================================================
PRINT '';
PRINT 'Stage 7: Commissions Calculated...';

INSERT INTO [etl].[calc_7_commissions_calculated] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount,
    HierarchyVersionId, HierarchyEffectiveFrom,
    HierarchyParticipantId, BrokerId, BrokerName, TierLevel, ScheduleCode, PaidBrokerId, ParticipantCommissionRate,
    RatePercent, RateSource, ScheduleId, ScheduleVersionId,
    CommissionAmount
)
SELECT 
    c6.PremiumTransactionId, c6.CertificateId, c6.TransactionDate, c6.PremiumAmount,
    c6.GroupId, c6.ProductCode, c6.[State], c6.CertificateEffectiveDate, c6.GroupSize,
    c6.IsFirstYear, c6.BasisYear, c6.ProposalId, c6.ProposalBrokerId, c6.SpecialCaseCode,
    c6.SplitSequence, c6.SplitPercent, c6.HierarchyId, c6.WritingBrokerId, c6.SplitPremiumAmount,
    c6.HierarchyVersionId, c6.HierarchyEffectiveFrom,
    c6.HierarchyParticipantId, c6.BrokerId, c6.BrokerName, c6.TierLevel, c6.ScheduleCode, c6.PaidBrokerId, c6.ParticipantCommissionRate,
    c6.RatePercent, c6.RateSource, c6.ScheduleId, c6.ScheduleVersionId,
    ROUND(c6.SplitPremiumAmount * (COALESCE(c6.RatePercent, 0) / 100.0), 2) AS CommissionAmount
FROM [etl].[calc_6_rates_applied] c6;

PRINT 'Stage 7 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_7_rows FROM [etl].[calc_7_commissions_calculated];

GO

-- =============================================================================
-- Stage 8: Assignments Applied
-- Handle commission transfers/assignments
-- =============================================================================
PRINT '';
PRINT 'Stage 8: Assignments Applied...';

INSERT INTO [etl].[calc_8_assignments_applied] (
    PremiumTransactionId, CertificateId, TransactionDate, PremiumAmount,
    GroupId, ProductCode, [State], CertificateEffectiveDate, GroupSize,
    IsFirstYear, BasisYear, ProposalId, ProposalBrokerId, SpecialCaseCode,
    SplitSequence, SplitPercent, HierarchyId, WritingBrokerId, SplitPremiumAmount,
    HierarchyVersionId, HierarchyEffectiveFrom,
    HierarchyParticipantId, BrokerId, BrokerName, TierLevel, ScheduleCode, PaidBrokerId, ParticipantCommissionRate,
    RatePercent, RateSource, ScheduleId, ScheduleVersionId, CommissionAmount,
    AssignmentVersionId, AssigneeBrokerId, AssigneeBrokerName, TotalAssignedPercent, AssignedAmount, RetainedAmount
)
SELECT 
    c7.PremiumTransactionId, c7.CertificateId, c7.TransactionDate, c7.PremiumAmount,
    c7.GroupId, c7.ProductCode, c7.[State], c7.CertificateEffectiveDate, c7.GroupSize,
    c7.IsFirstYear, c7.BasisYear, c7.ProposalId, c7.ProposalBrokerId, c7.SpecialCaseCode,
    c7.SplitSequence, c7.SplitPercent, c7.HierarchyId, c7.WritingBrokerId, c7.SplitPremiumAmount,
    c7.HierarchyVersionId, c7.HierarchyEffectiveFrom,
    c7.HierarchyParticipantId, c7.BrokerId, c7.BrokerName, c7.TierLevel, c7.ScheduleCode, c7.PaidBrokerId, c7.ParticipantCommissionRate,
    c7.RatePercent, c7.RateSource, c7.ScheduleId, c7.ScheduleVersionId, c7.CommissionAmount,
    cav.Id AS AssignmentVersionId,
    car.RecipientBrokerId AS AssigneeBrokerId,
    car.RecipientBrokerName AS AssigneeBrokerName,
    COALESCE(cav.TotalAssignedPercent, 0) AS TotalAssignedPercent,
    IIF(cav.Id IS NOT NULL, ROUND(c7.CommissionAmount * (cav.TotalAssignedPercent / 100.0), 2), 0) AS AssignedAmount,
    IIF(cav.Id IS NOT NULL, ROUND(c7.CommissionAmount * ((100 - cav.TotalAssignedPercent) / 100.0), 2), c7.CommissionAmount) AS RetainedAmount
FROM [etl].[calc_7_commissions_calculated] c7
LEFT JOIN [etl].[stg_commission_assignment_versions] cav 
    ON cav.BrokerId = c7.BrokerId
    AND (cav.ProposalId = c7.ProposalId OR cav.ProposalId = '__DEFAULT__')
    AND cav.[Status] = 0  -- Active
    AND c7.TransactionDate >= cav.EffectiveFrom
    AND (cav.EffectiveTo IS NULL OR c7.TransactionDate <= cav.EffectiveTo)
LEFT JOIN [etl].[stg_commission_assignment_recipients] car ON car.AssignmentVersionId = cav.Id;

PRINT 'Stage 8 completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';

SELECT COUNT(*) AS calc_8_rows FROM [etl].[calc_8_assignments_applied];

GO

-- =============================================================================
-- Output: GL Journal Entries - Original (Retained)
-- =============================================================================
PRINT '';
PRINT 'Generating GL Journal Entries (Original)...';

INSERT INTO [etl].[calc_gl_journal_entries] (
    PremiumTransactionId, PolicyId, BrokerId, BrokerName, PremiumAmount, CommissionAmount,
    RatePercent, TransactionDate, ProductCode, [State], GroupId, HierarchyId, HierarchyVersionId,
    SplitSequence, SplitPercent, TierLevel, IsFirstYear, BasisYear, RateSource, EntryType, SourceBrokerId
)
SELECT 
    PremiumTransactionId,
    CertificateId AS PolicyId,
    BrokerId,
    BrokerName,
    PremiumAmount,
    RetainedAmount AS CommissionAmount,
    RatePercent,
    TransactionDate,
    ProductCode,
    [State],
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    SplitSequence,
    SplitPercent,
    TierLevel,
    IsFirstYear,
    BasisYear,
    RateSource,
    'Original' AS EntryType,
    NULL AS SourceBrokerId
FROM [etl].[calc_8_assignments_applied]
WHERE RetainedAmount > 0;

PRINT 'GL entries (Original) created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Output: GL Journal Entries - Assigned
-- =============================================================================
PRINT 'Generating GL Journal Entries (Assigned)...';

INSERT INTO [etl].[calc_gl_journal_entries] (
    PremiumTransactionId, PolicyId, BrokerId, BrokerName, PremiumAmount, CommissionAmount,
    RatePercent, TransactionDate, ProductCode, [State], GroupId, HierarchyId, HierarchyVersionId,
    SplitSequence, SplitPercent, TierLevel, IsFirstYear, BasisYear, RateSource, EntryType, SourceBrokerId
)
SELECT 
    PremiumTransactionId,
    CertificateId AS PolicyId,
    AssigneeBrokerId AS BrokerId,
    AssigneeBrokerName AS BrokerName,
    PremiumAmount,
    AssignedAmount AS CommissionAmount,
    RatePercent,
    TransactionDate,
    ProductCode,
    [State],
    GroupId,
    HierarchyId,
    HierarchyVersionId,
    SplitSequence,
    SplitPercent,
    TierLevel,
    IsFirstYear,
    BasisYear,
    RateSource,
    'Assigned' AS EntryType,
    BrokerId AS SourceBrokerId
FROM [etl].[calc_8_assignments_applied]
WHERE AssignedAmount > 0;

PRINT 'GL entries (Assigned) created: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS total_gl_entries FROM [etl].[calc_gl_journal_entries];

GO

-- =============================================================================
-- Output: Traceability - Successful Calculations
-- =============================================================================
PRINT '';
PRINT 'Generating Traceability Reports (Successful)...';

INSERT INTO [etl].[calc_traceability] (
    Id, PremiumTransactionId, PolicyId, TransactionDate, PremiumAmount, TotalCommission,
    TraceabilityJson, TraceabilityMarkdown, ProposalId, GroupId, ProductCode, [State],
    IsFirstYear, BasisYear, HierarchyCount, ParticipantCount, HasAssignments, HasErrors, ErrorMessages,
    IsBootstrap, IsClean, SourceType
)
SELECT 
    CONCAT('TRACE-', PremiumTransactionId) AS Id,
    PremiumTransactionId, 
    MIN(CertificateId) AS PolicyId, 
    MIN(TransactionDate) AS TransactionDate,
    MIN(PremiumAmount) AS PremiumAmount, 
    CAST(ROUND(SUM(CAST(CommissionAmount AS FLOAT)), 2) AS DECIMAL(18,2)) AS TotalCommission,
    '{}' AS TraceabilityJson,
    NULL AS TraceabilityMarkdown,
    MIN(ProposalId) AS ProposalId,
    MIN(GroupId) AS GroupId,
    MIN(ProductCode) AS ProductCode,
    MIN([State]) AS [State],
    MIN(CAST(IsFirstYear AS INT)) AS IsFirstYear,
    MIN(BasisYear) AS BasisYear,
    COUNT(DISTINCT HierarchyId) AS HierarchyCount,
    COUNT(*) AS ParticipantCount, 
    CASE WHEN SUM(CAST(AssignedAmount AS FLOAT)) > 0 THEN 1 ELSE 0 END AS HasAssignments, 
    0 AS HasErrors, 
    NULL AS ErrorMessages,
    0 AS IsBootstrap,
    1 AS IsClean,
    'SqlCalculation' AS SourceType
FROM [etl].[calc_8_assignments_applied]
GROUP BY PremiumTransactionId;

PRINT 'Traceability reports (Successful) created: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Output: Traceability - Failed Calculations
-- =============================================================================
PRINT 'Generating Traceability Reports (Failed)...';

-- Use a CTE to deduplicate by PremiumTransactionId (can have multiple proposals per premium)
;WITH failed_premiums AS (
    SELECT 
        PremiumTransactionId,
        MIN(CertificateId) AS CertificateId,
        MIN(TransactionDate) AS TransactionDate,
        MIN(PremiumAmount) AS PremiumAmount,
        MIN(ProposalId) AS ProposalId,
        MIN(GroupId) AS GroupId,
        MIN(ProductCode) AS ProductCode,
        MIN([State]) AS [State],
        MIN(CAST(IsFirstYear AS INT)) AS IsFirstYear,
        MIN(BasisYear) AS BasisYear
    FROM [etl].[calc_2_proposals_resolved]
    WHERE PremiumTransactionId NOT IN (SELECT DISTINCT PremiumTransactionId FROM [etl].[calc_8_assignments_applied])
      AND PremiumTransactionId NOT IN (SELECT PremiumTransactionId FROM [etl].[calc_traceability])
    GROUP BY PremiumTransactionId
)
INSERT INTO [etl].[calc_traceability] (
    Id, PremiumTransactionId, PolicyId, TransactionDate, PremiumAmount, TotalCommission,
    TraceabilityJson, TraceabilityMarkdown, ProposalId, GroupId, ProductCode, [State],
    IsFirstYear, BasisYear, HierarchyCount, ParticipantCount, HasAssignments, HasErrors, ErrorMessages,
    IsBootstrap, IsClean, SourceType
)
SELECT 
    CONCAT('TRACE-', c2.PremiumTransactionId) AS Id,
    c2.PremiumTransactionId, 
    c2.CertificateId AS PolicyId, 
    c2.TransactionDate,
    c2.PremiumAmount, 
    CAST(0 AS DECIMAL(18,2)) AS TotalCommission,
    '{}' AS TraceabilityJson,
    NULL AS TraceabilityMarkdown,
    c2.ProposalId,
    c2.GroupId,
    c2.ProductCode,
    c2.[State],
    c2.IsFirstYear,
    c2.BasisYear,
    0 AS HierarchyCount,
    0 AS ParticipantCount, 
    0 AS HasAssignments, 
    1 AS HasErrors,
    CASE 
        WHEN c2.ProposalId IS NULL THEN 'No matching proposal'
        WHEN NOT EXISTS (SELECT 1 FROM [etl].[calc_3_splits_applied] c3 WHERE c3.PremiumTransactionId = c2.PremiumTransactionId) THEN 'No matching split version'
        WHEN NOT EXISTS (SELECT 1 FROM [etl].[calc_4_hierarchies_resolved] c4 WHERE c4.PremiumTransactionId = c2.PremiumTransactionId AND c4.HierarchyVersionId IS NOT NULL) THEN 'No active hierarchy version'
        ELSE 'Unknown error'
    END AS ErrorMessages,
    0 AS IsBootstrap,
    0 AS IsClean,
    'SqlCalculation' AS SourceType
FROM failed_premiums c2;

PRINT 'Traceability reports (Failed) created: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS total_traceability FROM [etl].[calc_traceability];

GO

-- =============================================================================
-- Output: Broker Traceabilities
-- =============================================================================
PRINT '';
PRINT 'Generating Broker Traceabilities...';

INSERT INTO [etl].[calc_broker_traceabilities] (
    Id, CommissionTraceabilityReportId, BrokerId, BrokerName,
    [Level], LevelName, SplitSequence, SplitPercent,
    RatePercent, RateSource, CommissionAmount,
    HierarchyId, HierarchyVersionId, HierarchyParticipantId,
    ScheduleId, ScheduleVersionId, ScheduleCode,
    IsAssigned, AssignedFromBrokerId, AssignmentVersionId, EntryType
)
SELECT 
    CONCAT('BT-', CAST(gl.Id AS VARCHAR)) AS Id,
    CONCAT('TRACE-', gl.PremiumTransactionId) AS CommissionTraceabilityReportId,
    gl.BrokerId,
    gl.BrokerName,
    gl.TierLevel AS [Level],
    CONCAT('Level ', CAST(gl.TierLevel AS VARCHAR)) AS LevelName,
    gl.SplitSequence,
    gl.SplitPercent,
    gl.RatePercent,
    gl.RateSource,
    gl.CommissionAmount,
    gl.HierarchyId,
    gl.HierarchyVersionId,
    NULL AS HierarchyParticipantId,
    NULL AS ScheduleId,
    NULL AS ScheduleVersionId,
    NULL AS ScheduleCode,
    CASE WHEN gl.EntryType = 'Assigned' THEN 1 ELSE 0 END AS IsAssigned,
    gl.SourceBrokerId AS AssignedFromBrokerId,
    NULL AS AssignmentVersionId,
    gl.EntryType
FROM [etl].[calc_gl_journal_entries] gl;

PRINT 'Broker traceabilities created: ' + CAST(@@ROWCOUNT AS VARCHAR);

SELECT COUNT(*) AS total_broker_traceabilities FROM [etl].[calc_broker_traceabilities];

GO

-- =============================================================================
-- Summary
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'CALCULATION PIPELINE SUMMARY';
PRINT '============================================================';

SELECT 'calc_1_premium_context' AS Stage, COUNT(*) AS [Rows] FROM [etl].[calc_1_premium_context]
UNION ALL SELECT 'calc_2_proposals_resolved', COUNT(*) FROM [etl].[calc_2_proposals_resolved]
UNION ALL SELECT 'calc_3_splits_applied', COUNT(*) FROM [etl].[calc_3_splits_applied]
UNION ALL SELECT 'calc_4_hierarchies_resolved', COUNT(*) FROM [etl].[calc_4_hierarchies_resolved]
UNION ALL SELECT 'calc_5_participants_expanded', COUNT(*) FROM [etl].[calc_5_participants_expanded]
UNION ALL SELECT 'calc_6_rates_applied', COUNT(*) FROM [etl].[calc_6_rates_applied]
UNION ALL SELECT 'calc_7_commissions_calculated', COUNT(*) FROM [etl].[calc_7_commissions_calculated]
UNION ALL SELECT 'calc_8_assignments_applied', COUNT(*) FROM [etl].[calc_8_assignments_applied]
UNION ALL SELECT 'calc_gl_journal_entries', COUNT(*) FROM [etl].[calc_gl_journal_entries]
UNION ALL SELECT 'calc_traceability', COUNT(*) FROM [etl].[calc_traceability]
UNION ALL SELECT 'calc_broker_traceabilities', COUNT(*) FROM [etl].[calc_broker_traceabilities]
ORDER BY Stage;

PRINT '';
PRINT 'End Time: ' + CONVERT(VARCHAR, GETUTCDATE(), 121);
PRINT '============================================================';
PRINT 'COMMISSION CALCULATION PIPELINE COMPLETED';
PRINT '============================================================';

GO

