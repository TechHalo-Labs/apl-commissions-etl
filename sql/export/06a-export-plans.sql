-- =====================================================
-- Export Plans from etl staging to dbo
-- Only exports plans that don't already exist
-- =====================================================

PRINT '=== Starting Plans Export ===';

-- Check before counts
DECLARE @before_count INT;
SELECT @before_count = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Plans];
PRINT 'Plans before export: ' + CAST(@before_count AS VARCHAR);

DECLARE @staging_count INT;
SELECT @staging_count = COUNT(*) FROM [$(ETL_SCHEMA)].[stg_plans];
PRINT 'Plans in staging: ' + CAST(@staging_count AS VARCHAR);

-- Export plans that exist in staging and have a valid ProductId in Products
INSERT INTO [$(PRODUCTION_SCHEMA)].[Plans] (
    Id, ProductId, PlanCode, Name, [Description], [Status],
    CreationTime, IsDeleted
)
SELECT 
    sp.Id,
    sp.ProductId,
    LEFT(sp.PlanCode, 50) AS PlanCode,  -- PlanCode has max 50 chars in dbo
    LEFT(sp.Name, 255) AS Name,          -- Name has max 255 chars in dbo
    LEFT(sp.[Description], 1000) AS [Description],  -- Description has max 1000 chars
    COALESCE(sp.[Status], 1) AS [Status],  -- 1 = Active
    COALESCE(sp.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sp.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_plans] sp
INNER JOIN [$(PRODUCTION_SCHEMA)].[Products] p ON p.Id = sp.ProductId  -- Must have valid product
WHERE sp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Plans]);    -- Skip existing

DECLARE @exported INT = @@ROWCOUNT;
PRINT 'Plans exported: ' + CAST(@exported AS VARCHAR);

-- Check after counts
DECLARE @after_count INT;
SELECT @after_count = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Plans];
PRINT 'Plans after export: ' + CAST(@after_count AS VARCHAR);

-- Report any staging plans that couldn't be exported due to missing product
DECLARE @orphan_count INT;
SELECT @orphan_count = COUNT(*)
FROM [$(ETL_SCHEMA)].[stg_plans] sp
LEFT JOIN [$(PRODUCTION_SCHEMA)].[Products] p ON p.Id = sp.ProductId
WHERE p.Id IS NULL;

IF @orphan_count > 0
BEGIN
    PRINT 'Warning: ' + CAST(@orphan_count AS VARCHAR) + ' staging plans have no matching product';
    
    -- Show sample of orphaned plans
    SELECT TOP 10 
        sp.Id,
        sp.ProductId AS MissingProductId,
        sp.PlanCode
    FROM [$(ETL_SCHEMA)].[stg_plans] sp
    LEFT JOIN [$(PRODUCTION_SCHEMA)].[Products] p ON p.Id = sp.ProductId
    WHERE p.Id IS NULL;
END

PRINT '=== Plans Export Complete ===';

GO

