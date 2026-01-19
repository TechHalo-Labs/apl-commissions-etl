-- =============================================================================
-- Transform: Products (T-SQL)
-- Creates products from unique ProductCodes in certificates and schedule rates
-- Usage: sqlcmd -S server -d database -i sql/transforms/03-products.sql
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Products';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Step 1: Truncate staging table
-- =============================================================================
PRINT 'Step 1: Truncating stg_products...';
TRUNCATE TABLE [etl].[stg_products];

-- =============================================================================
-- Step 2: Extract unique products from certificates
-- =============================================================================
PRINT 'Step 2: Extracting products from input_certificate_info...';

INSERT INTO [etl].[stg_products] (
    Id, ProductCode, ProductName, MasterCategory, Category, 
    CommissionType, IsActive, [Description], CreationTime, IsDeleted
)
SELECT
    LTRIM(RTRIM(Product)) AS Id,
    LTRIM(RTRIM(Product)) AS ProductCode,
    MAX(COALESCE(
        NULLIF(LTRIM(RTRIM(ProductCategory)), ''),
        LTRIM(RTRIM(Product))
    )) AS ProductName,
    MAX(LTRIM(RTRIM(ProductMasterCategory))) AS MasterCategory,
    MAX(LTRIM(RTRIM(ProductCategory))) AS Category,
    MAX(LTRIM(RTRIM(CommissionType))) AS CommissionType,
    1 AS IsActive,
    CONCAT('Product: ', LTRIM(RTRIM(Product)), ' (', 
           COALESCE(NULLIF(MAX(LTRIM(RTRIM(ProductCategory))), ''), 'Unknown'), ')') AS [Description],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[input_certificate_info]
WHERE LTRIM(RTRIM(Product)) <> ''
GROUP BY LTRIM(RTRIM(Product));  -- Only group by Product to avoid duplicates

DECLARE @cert_products INT = @@ROWCOUNT;
PRINT 'Products from certificates: ' + CAST(@cert_products AS VARCHAR);

-- =============================================================================
-- Step 3: Add products from schedule rates not already present
-- =============================================================================
PRINT '';
PRINT 'Step 3: Adding products from raw_schedule_rates...';

INSERT INTO [etl].[stg_products] (
    Id, ProductCode, ProductName, MasterCategory, Category,
    OffGroupLetterDescription, SeriesType, SpecialOffer,
    IsActive, [Description], CreationTime, IsDeleted
)
SELECT
    LTRIM(RTRIM(sr.ProductCode)) AS Id,
    LTRIM(RTRIM(sr.ProductCode)) AS ProductCode,
    MAX(COALESCE(
        NULLIF(LTRIM(RTRIM(sr.Category)), ''),
        LTRIM(RTRIM(sr.ProductCode))
    )) AS ProductName,
    MAX(LTRIM(RTRIM(sr.Category))) AS MasterCategory,
    MAX(LTRIM(RTRIM(sr.Category))) AS Category,
    MAX(LTRIM(RTRIM(sr.OffGroupLetterDescription))) AS OffGroupLetterDescription,
    MAX(LTRIM(RTRIM(sr.SeriesType))) AS SeriesType,
    MAX(LTRIM(RTRIM(sr.SpecialOffer))) AS SpecialOffer,
    1 AS IsActive,
    CONCAT('Product: ', LTRIM(RTRIM(sr.ProductCode)), ' (from schedule rates)') AS [Description],
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM [etl].[raw_schedule_rates] sr
WHERE LTRIM(RTRIM(sr.ProductCode)) <> ''
  AND NOT EXISTS (
      SELECT 1 FROM [etl].[stg_products] p 
      WHERE p.Id = LTRIM(RTRIM(sr.ProductCode))
  )
GROUP BY LTRIM(RTRIM(sr.ProductCode));  -- Only group by ProductCode to avoid duplicates

DECLARE @sched_products INT = @@ROWCOUNT;
PRINT 'Products added from schedules: ' + CAST(@sched_products AS VARCHAR);

-- =============================================================================
-- Step 4: Populate Product Codes (links product codes to their parent category)
-- =============================================================================
PRINT '';
PRINT 'Step 4: Populating stg_product_codes...';

TRUNCATE TABLE [etl].[stg_product_codes];

-- Product codes from certificates
-- First get distinct product/category combinations with their states
;WITH ProductCodeData AS (
    SELECT
        LTRIM(RTRIM(ProductCategory)) AS ProductCategory,
        LTRIM(RTRIM(Product)) AS Product,
        LTRIM(RTRIM(CertIssuedState)) AS CertIssuedState,
        LTRIM(RTRIM(GroupId)) AS GroupId
    FROM [etl].[input_certificate_info]
    WHERE LTRIM(RTRIM(ProductCategory)) <> ''
      AND LTRIM(RTRIM(Product)) <> ''
),
ProductCodeAgg AS (
    SELECT
        ProductCategory,
        Product,
        COUNT(DISTINCT GroupId) AS GroupsCount
    FROM ProductCodeData
    GROUP BY ProductCategory, Product
),
ProductCodeStates AS (
    SELECT DISTINCT
        ProductCategory,
        Product,
        CertIssuedState
    FROM ProductCodeData
    WHERE CertIssuedState <> ''
)
INSERT INTO [etl].[stg_product_codes] (
    Id, ProductId, Code, [Description], AllowedStates, [Status], 
    GroupsCount, SchedulesCount, CreationTime, IsDeleted
)
SELECT
    ROW_NUMBER() OVER (ORDER BY pca.ProductCategory, pca.Product) AS Id,
    pca.ProductCategory AS ProductId,
    pca.Product AS Code,
    CONCAT(pca.ProductCategory, ' - ', pca.Product) AS [Description],
    COALESCE((
        SELECT STRING_AGG(pcs.CertIssuedState, ',') WITHIN GROUP (ORDER BY pcs.CertIssuedState)
        FROM ProductCodeStates pcs
        WHERE pcs.ProductCategory = pca.ProductCategory AND pcs.Product = pca.Product
    ), '') AS AllowedStates,
    'Active' AS [Status],
    pca.GroupsCount AS GroupsCount,
    0 AS SchedulesCount,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM ProductCodeAgg pca;

DECLARE @cert_codes INT = @@ROWCOUNT;
PRINT 'Product codes from certificates: ' + CAST(@cert_codes AS VARCHAR);

-- Add product codes from schedule rates not already present
INSERT INTO [etl].[stg_product_codes] (
    Id, ProductId, Code, [Description], AllowedStates, [Status], 
    GroupsCount, SchedulesCount, CreationTime, IsDeleted
)
SELECT
    (SELECT ISNULL(MAX(Id), 0) FROM [etl].[stg_product_codes]) + ROW_NUMBER() OVER (ORDER BY sub.Category, sub.ProductCode) AS Id,
    sub.Category AS ProductId,
    sub.ProductCode AS Code,
    COALESCE(sub.OffGroupLetterDesc, CONCAT(sub.Category, ' - ', sub.ProductCode)) AS [Description],
    '' AS AllowedStates,
    'Active' AS [Status],
    0 AS GroupsCount,
    sub.ScheduleCount AS SchedulesCount,
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT
        LTRIM(RTRIM(Category)) AS Category,
        LTRIM(RTRIM(ProductCode)) AS ProductCode,
        MAX(LTRIM(RTRIM(OffGroupLetterDescription))) AS OffGroupLetterDesc,
        COUNT(DISTINCT ScheduleName) AS ScheduleCount
    FROM [etl].[raw_schedule_rates]
    WHERE LTRIM(RTRIM(Category)) <> ''
      AND LTRIM(RTRIM(ProductCode)) <> ''
    GROUP BY LTRIM(RTRIM(Category)), LTRIM(RTRIM(ProductCode))
) sub
WHERE NOT EXISTS (
    SELECT 1 FROM [etl].[stg_product_codes] pc 
    WHERE pc.Code = sub.ProductCode
);

DECLARE @sched_codes INT = @@ROWCOUNT;
PRINT 'Product codes added from schedules: ' + CAST(@sched_codes AS VARCHAR);

-- =============================================================================
-- Step 5: Populate Plans (unique plan codes linked to products)
-- Plans link to Products via ProductCode (not ProductCategory)
-- =============================================================================
PRINT '';
PRINT 'Step 5: Populating stg_plans...';

TRUNCATE TABLE [etl].[stg_plans];

-- Create plans from unique (ProductCode, PlanCode) combinations in certificates
-- Use GROUP BY on trimmed values to avoid duplicates from whitespace variations
INSERT INTO [etl].[stg_plans] (
    Id, ProductId, PlanCode, Name, [Description], [Status], CreationTime, IsDeleted
)
SELECT
    -- Id format: ProductCode-PlanCode (matches how we exported to production)
    CONCAT(ProductCode, '-', PlanCode) AS Id,
    ProductCode AS ProductId,
    PlanCode,
    PlanCode AS Name,
    CONCAT('Plan ', PlanCode, ' for product ', ProductCode) AS [Description],
    0 AS [Status],  -- 0 = Active
    GETUTCDATE() AS CreationTime,
    0 AS IsDeleted
FROM (
    SELECT
        LTRIM(RTRIM(Product)) AS ProductCode,
        LTRIM(RTRIM(PlanCode)) AS PlanCode
    FROM [etl].[input_certificate_info]
    WHERE LTRIM(RTRIM(Product)) <> ''
      AND LTRIM(RTRIM(PlanCode)) <> ''
      AND LTRIM(RTRIM(PlanCode)) <> 'N/A'
    GROUP BY LTRIM(RTRIM(Product)), LTRIM(RTRIM(PlanCode))  -- Deduplicate on trimmed values
) AS distinct_plans;

DECLARE @plans_count INT = @@ROWCOUNT;
PRINT 'Plans created: ' + CAST(@plans_count AS VARCHAR);

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 'stg_products' AS entity, COUNT(*) AS cnt FROM [etl].[stg_products]
UNION ALL
SELECT 'stg_product_codes' AS entity, COUNT(*) AS cnt FROM [etl].[stg_product_codes]
UNION ALL
SELECT 'stg_plans' AS entity, COUNT(*) AS cnt FROM [etl].[stg_plans];

PRINT '';
PRINT 'Products by category (top 10):';
SELECT TOP 10 
       Category, 
       COUNT(*) AS product_count 
FROM [etl].[stg_products]
GROUP BY Category
ORDER BY product_count DESC;

PRINT '';
PRINT 'Plans by product (top 10):';
SELECT TOP 10 
       ProductId, 
       COUNT(*) AS plan_count 
FROM [etl].[stg_plans]
GROUP BY ProductId
ORDER BY plan_count DESC;

PRINT '';
PRINT '============================================================';
PRINT 'PRODUCTS TRANSFORM COMPLETED';
PRINT '============================================================';

GO

