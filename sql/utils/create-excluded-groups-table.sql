-- =============================================================================
-- Create and Populate stg_excluded_groups Table
-- =============================================================================
-- Flags groups that should be excluded from export:
-- 1. Groups with Name like 'Universal Truck%'
-- 2. Groups with GroupId in ('00000','0000','G00000','G0000')
-- =============================================================================

SET NOCOUNT ON;
GO

PRINT 'Creating stg_excluded_groups table...';

-- Drop and recreate the table (ensures clean state)
DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[stg_excluded_groups];
GO

CREATE TABLE [$(ETL_SCHEMA)].[stg_excluded_groups] (
    GroupId NVARCHAR(100) NOT NULL,
    GroupName NVARCHAR(500),
    ExclusionReason NVARCHAR(500),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT PK_stg_excluded_groups PRIMARY KEY (GroupId)
);
PRINT '  ✓ Created [$(ETL_SCHEMA)].[stg_excluded_groups]';
GO

-- Clear existing data
TRUNCATE TABLE [$(ETL_SCHEMA)].[stg_excluded_groups];
PRINT '  ✓ Cleared existing data';
GO

-- Populate excluded groups
PRINT '';
PRINT 'Populating stg_excluded_groups...';

-- Insert groups matching exclusion criteria
INSERT INTO [$(ETL_SCHEMA)].[stg_excluded_groups] (
    GroupId,
    GroupName,
    ExclusionReason
)
SELECT DISTINCT
    sg.Id AS GroupId,
    sg.Name AS GroupName,
    CASE 
        WHEN sg.Name LIKE 'Universal Truck%' THEN 'Universal Trucking group'
        WHEN sg.Id IN ('G00000', 'G0000', '00000', '0000') THEN 'DTC/Invalid GroupId'
        ELSE 'Other exclusion criteria'
    END AS ExclusionReason
FROM [$(ETL_SCHEMA)].[stg_groups] sg
WHERE 
    -- Universal Trucking groups
    sg.Name LIKE 'Universal Truck%'
    -- DTC/Invalid GroupIds
    OR sg.Id IN ('G00000', 'G0000', '00000', '0000')

DECLARE @excluded_count INT = @@ROWCOUNT;
PRINT '  ✓ Populated ' + CAST(@excluded_count AS VARCHAR) + ' excluded groups';
GO

-- Show breakdown by exclusion reason
PRINT '';
PRINT 'Excluded groups breakdown:';
SELECT 
    ExclusionReason,
    COUNT(*) AS GroupCount
FROM [$(ETL_SCHEMA)].[stg_excluded_groups]
GROUP BY ExclusionReason
ORDER BY GroupCount DESC;
GO

-- Show sample excluded groups
PRINT '';
PRINT 'Sample excluded groups:';
SELECT TOP 10
    GroupId,
    GroupName,
    ExclusionReason
FROM [$(ETL_SCHEMA)].[stg_excluded_groups]
ORDER BY ExclusionReason, GroupId;
GO

PRINT '';
PRINT '=== stg_excluded_groups table ready ===';
GO
