-- =============================================================================
-- Export: Hierarchy Participant Product Rates (Product-Specific Rate Overrides)
-- =============================================================================
-- Exports stg_hierarchy_participant_product_rates to dbo.HierarchyParticipantProductRates
-- Used for product-specific commission rate overrides for individual participants
-- (e.g., Broker A gets 12% for DENTAL but 10% for VISION)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'EXPORT: Hierarchy Participant Product Rates';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Check if staging table exists
-- =============================================================================
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_hierarchy_participant_product_rates'
)
BEGIN
    PRINT 'SKIPPED: stg_hierarchy_participant_product_rates table does not exist';
    PRINT 'Run 01-add-missing-staging-tables.sql first, then populate staging';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 2: Check current state
-- =============================================================================
DECLARE @before_count INT;
SELECT @before_count = COUNT(*) FROM [dbo].[HierarchyParticipantProductRates];
PRINT 'HierarchyParticipantProductRates before export: ' + CAST(@before_count AS VARCHAR);

DECLARE @staging_count INT;
SELECT @staging_count = COUNT(*) FROM [etl].[stg_hierarchy_participant_product_rates];
PRINT 'HierarchyParticipantProductRates in staging: ' + CAST(@staging_count AS VARCHAR);

IF @staging_count = 0
BEGIN
    PRINT '';
    PRINT 'INFO: No records in staging table. Nothing to export.';
    PRINT 'If product-specific rate overrides exist, populate stg_hierarchy_participant_product_rates first.';
    PRINT '============================================================';
    RETURN;
END

-- =============================================================================
-- Step 3: Export hierarchy participant product rates
-- Only export for hierarchy participants that exist in production
-- =============================================================================
PRINT '';
PRINT 'Step 3: Exporting hierarchy participant product rates...';

SET IDENTITY_INSERT [dbo].[HierarchyParticipantProductRates] ON;

INSERT INTO [dbo].[HierarchyParticipantProductRates] (
    Id,
    HierarchyParticipantId,
    ProductCode,
    FirstYearRate,
    RenewalRate,
    EffectiveFrom,
    EffectiveTo,
    Notes,
    CreationTime,
    IsDeleted
)
SELECT
    hppr.Id,
    hppr.HierarchyParticipantId,
    hppr.ProductCode,
    hppr.FirstYearRate,
    hppr.RenewalRate,
    COALESCE(hppr.EffectiveFrom, '0001-01-01') AS EffectiveFrom,
    hppr.EffectiveTo,
    hppr.Notes,
    COALESCE(hppr.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(hppr.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_hierarchy_participant_product_rates] hppr
WHERE 
    -- HierarchyParticipant must exist in production
    EXISTS (SELECT 1 FROM [dbo].[HierarchyParticipants] hp WHERE hp.Id = hppr.HierarchyParticipantId)
    -- Don't create duplicates
    AND hppr.Id NOT IN (SELECT Id FROM [dbo].[HierarchyParticipantProductRates]);

SET IDENTITY_INSERT [dbo].[HierarchyParticipantProductRates] OFF;

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'New HierarchyParticipantProductRates exported: ' + CAST(@exported AS VARCHAR);

-- =============================================================================
-- Step 4: Report skipped records
-- =============================================================================
PRINT '';
PRINT 'Step 4: Reporting skipped records...';

-- Report staging records that couldn't be exported due to missing hierarchy participant
DECLARE @no_participant_count INT;
SELECT @no_participant_count = COUNT(*)
FROM [etl].[stg_hierarchy_participant_product_rates] hppr
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[HierarchyParticipants] hp WHERE hp.Id = hppr.HierarchyParticipantId);

IF @no_participant_count > 0
BEGIN
    PRINT 'WARNING: ' + CAST(@no_participant_count AS VARCHAR) + ' staging records skipped (HierarchyParticipantId not in production)';
    
    -- Show sample of skipped records
    SELECT TOP 10 
        hppr.Id,
        hppr.HierarchyParticipantId,
        hppr.ProductCode,
        hppr.FirstYearRate,
        'HierarchyParticipantId not in dbo.HierarchyParticipants' AS Reason
    FROM [etl].[stg_hierarchy_participant_product_rates] hppr
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[HierarchyParticipants] hp WHERE hp.Id = hppr.HierarchyParticipantId)
    ORDER BY hppr.HierarchyParticipantId, hppr.ProductCode;
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

DECLARE @after_count INT;
SELECT @after_count = COUNT(*) FROM [dbo].[HierarchyParticipantProductRates];
PRINT 'HierarchyParticipantProductRates after export: ' + CAST(@after_count AS VARCHAR);
PRINT 'Net new records: ' + CAST(@after_count - @before_count AS VARCHAR);

-- Breakdown by product code
SELECT 'Production by Product Code' AS Metric, ProductCode, COUNT(*) AS Cnt, AVG(FirstYearRate) AS AvgFYRate
FROM [dbo].[HierarchyParticipantProductRates]
GROUP BY ProductCode
ORDER BY Cnt DESC;

PRINT '';
PRINT '============================================================';
PRINT 'HIERARCHY PARTICIPANT PRODUCT RATES EXPORT COMPLETED';
PRINT '============================================================';

GO
