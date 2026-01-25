-- =============================================================================
-- PATCH: Add Missing Staging Tables
-- =============================================================================
-- Creates staging tables for entities missing from ETL:
-- 1. stg_special_schedule_rates (heaped schedule year rates)
-- 2. stg_schedule_rate_tiers (group-size tiered rates)
-- 3. stg_hierarchy_participant_product_rates (product-specific rate overrides)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'PATCH: Adding Missing Staging Tables';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- 1. Special Schedule Rates (Heaped Schedule Year Rates)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_special_schedule_rates')
BEGIN
    CREATE TABLE [etl].[stg_special_schedule_rates] (
        Id BIGINT NOT NULL,
        ScheduleRateId BIGINT NOT NULL,
        [Year] INT NOT NULL,
        Rate DECIMAL(18,6) NOT NULL,
        CreationTime DATETIME2 DEFAULT GETUTCDATE(),
        IsDeleted BIT DEFAULT 0,
        CONSTRAINT PK_stg_special_schedule_rates PRIMARY KEY (Id)
    );
    
    CREATE NONCLUSTERED INDEX IX_stg_special_schedule_rates_ScheduleRateId
    ON [etl].[stg_special_schedule_rates] (ScheduleRateId);
    
    CREATE UNIQUE NONCLUSTERED INDEX IX_stg_special_schedule_rates_RateYear
    ON [etl].[stg_special_schedule_rates] (ScheduleRateId, [Year]);
    
    PRINT 'Created [etl].[stg_special_schedule_rates]';
END
ELSE
BEGIN
    PRINT 'Table [etl].[stg_special_schedule_rates] already exists';
END
GO

-- =============================================================================
-- 2. Schedule Rate Tiers (Group-Size Tiered Rates)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_schedule_rate_tiers')
BEGIN
    CREATE TABLE [etl].[stg_schedule_rate_tiers] (
        Id BIGINT NOT NULL,
        ScheduleRateId BIGINT NOT NULL,
        MinVolume DECIMAL(18,2) NOT NULL DEFAULT 0,
        MaxVolume DECIMAL(18,2) NULL,
        Rate DECIMAL(18,6) NOT NULL,
        FirstYearRate DECIMAL(18,6) NULL,
        RenewalRate DECIMAL(18,6) NULL,
        CreationTime DATETIME2 DEFAULT GETUTCDATE(),
        IsDeleted BIT DEFAULT 0,
        CONSTRAINT PK_stg_schedule_rate_tiers PRIMARY KEY (Id)
    );
    
    CREATE NONCLUSTERED INDEX IX_stg_schedule_rate_tiers_ScheduleRateId
    ON [etl].[stg_schedule_rate_tiers] (ScheduleRateId);
    
    PRINT 'Created [etl].[stg_schedule_rate_tiers]';
END
ELSE
BEGIN
    PRINT 'Table [etl].[stg_schedule_rate_tiers] already exists';
END
GO

-- =============================================================================
-- 3. Hierarchy Participant Product Rates (Product-Specific Rate Overrides)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'stg_hierarchy_participant_product_rates')
BEGIN
    CREATE TABLE [etl].[stg_hierarchy_participant_product_rates] (
        Id BIGINT NOT NULL,
        HierarchyParticipantId NVARCHAR(100) NOT NULL,
        ProductCode NVARCHAR(50) NOT NULL,
        FirstYearRate DECIMAL(18,6) NOT NULL,
        RenewalRate DECIMAL(18,6) NULL,
        EffectiveFrom DATETIME2 NOT NULL DEFAULT '0001-01-01',
        EffectiveTo DATETIME2 NULL,
        Notes NVARCHAR(500) NULL,
        CreationTime DATETIME2 DEFAULT GETUTCDATE(),
        IsDeleted BIT DEFAULT 0,
        CONSTRAINT PK_stg_hierarchy_participant_product_rates PRIMARY KEY (Id)
    );
    
    CREATE NONCLUSTERED INDEX IX_stg_hierarchy_participant_product_rates_ParticipantId
    ON [etl].[stg_hierarchy_participant_product_rates] (HierarchyParticipantId);
    
    CREATE NONCLUSTERED INDEX IX_stg_hierarchy_participant_product_rates_ProductCode
    ON [etl].[stg_hierarchy_participant_product_rates] (ProductCode);
    
    PRINT 'Created [etl].[stg_hierarchy_participant_product_rates]';
END
ELSE
BEGIN
    PRINT 'Table [etl].[stg_hierarchy_participant_product_rates] already exists';
END
GO

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    p.rows AS RowCount
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
WHERE s.name = 'etl'
  AND t.name IN ('stg_special_schedule_rates', 'stg_schedule_rate_tiers', 'stg_hierarchy_participant_product_rates')
ORDER BY t.name;

PRINT '';
PRINT '============================================================';
PRINT 'PATCH COMPLETE';
PRINT '============================================================';

GO
