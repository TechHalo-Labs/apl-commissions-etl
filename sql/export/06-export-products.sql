-- =====================================================
-- Export Products from etl staging to dbo
-- Only exports products that don't already exist
-- Production schema uses Id as nvarchar, ExpirationDate not TerminationDate
-- =====================================================

PRINT 'Exporting missing Products to dbo.Products...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[Products] (
    Id, ProductCode, ProductName, MasterCategory, Category,
    OffGroupLetterDescription, CommissionType, DefaultCommissionTable,
    IsActive, IsArchived, SeriesType, SpecialOffer, [Description],
    CreationTime, IsDeleted
)
SELECT 
    sp.Id,
    sp.ProductCode,
    sp.ProductName,
    sp.MasterCategory,
    sp.Category,
    sp.OffGroupLetterDescription,
    sp.CommissionType,
    sp.DefaultCommissionTable,
    COALESCE(sp.IsActive, 1) AS IsActive,
    COALESCE(sp.IsArchived, 0) AS IsArchived,
    sp.SeriesType,
    sp.SpecialOffer,
    sp.[Description],
    COALESCE(sp.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(sp.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_products] sp
WHERE sp.Id NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[Products]);

DECLARE @productCount INT;
SELECT @productCount = @@ROWCOUNT;
PRINT 'Products exported: ' + CAST(@productCount AS VARCHAR);

DECLARE @totalProducts INT;
SELECT @totalProducts = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[Products];
PRINT 'Total products in dbo: ' + CAST(@totalProducts AS VARCHAR);
GO

PRINT 'Exporting missing ProductCodes to dbo.ProductCodes...';

INSERT INTO [$(PRODUCTION_SCHEMA)].[ProductCodes] (
    Id, ProductId, Code, [Description], AllowedStates, [Status],
    GroupsCount, SchedulesCount, CreationTime, IsDeleted
)
SELECT 
    CAST(spc.Id AS NVARCHAR(100)) AS Id,
    spc.ProductId,
    spc.Code,
    COALESCE(spc.[Description], spc.Code) AS [Description],
    COALESCE(spc.AllowedStates, '') AS AllowedStates,
    COALESCE(spc.[Status], 'Active') AS [Status],
    COALESCE(spc.GroupsCount, 0) AS GroupsCount,
    COALESCE(spc.SchedulesCount, 0) AS SchedulesCount,
    COALESCE(spc.CreationTime, GETUTCDATE()) AS CreationTime,
    COALESCE(spc.IsDeleted, 0) AS IsDeleted
FROM [$(ETL_SCHEMA)].[stg_product_codes] spc
WHERE CAST(spc.Id AS NVARCHAR(100)) NOT IN (SELECT Id FROM [$(PRODUCTION_SCHEMA)].[ProductCodes]);

DECLARE @productCodeCount INT;
SELECT @productCodeCount = @@ROWCOUNT;
PRINT 'ProductCodes exported: ' + CAST(@productCodeCount AS VARCHAR);

DECLARE @totalProductCodes INT;
SELECT @totalProductCodes = COUNT(*) FROM [$(PRODUCTION_SCHEMA)].[ProductCodes];
PRINT 'Total product codes in dbo: ' + CAST(@totalProductCodes AS VARCHAR);
GO

PRINT '=== Product Export Complete ===';
