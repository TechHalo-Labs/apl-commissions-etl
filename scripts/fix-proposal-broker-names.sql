-- =============================================================================
-- Fix Broker Names on Existing Proposals
-- =============================================================================
-- Updates BrokerName in dbo.Proposals from dbo.Brokers table
-- Run this directly against production database
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'FIXING BROKER NAMES ON PROPOSALS';
PRINT '============================================================';
PRINT '';

-- Show current state
PRINT 'Current state:';
SELECT 
    COUNT(*) AS TotalProposals,
    SUM(CASE WHEN BrokerName IS NULL OR LTRIM(RTRIM(BrokerName)) = '' THEN 1 ELSE 0 END) AS MissingNames,
    SUM(CASE WHEN BrokerName LIKE 'Broker %' THEN 1 ELSE 0 END) AS PlaceholderNames
FROM [dbo].[Proposals]
WHERE BrokerId IS NOT NULL;

PRINT '';

-- Update BrokerName from Brokers table
PRINT 'Updating BrokerName from Brokers table...';

UPDATE p
SET 
    p.BrokerName = COALESCE(
        NULLIF(LTRIM(RTRIM(b.Name)), ''),
        CONCAT('Broker P', p.BrokerId)
    ),
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
LEFT JOIN [dbo].[Brokers] b ON b.Id = p.BrokerId
WHERE p.BrokerId IS NOT NULL
    AND (
        p.BrokerName IS NULL 
        OR LTRIM(RTRIM(p.BrokerName)) = ''
        OR p.BrokerName = CONCAT('Broker ', p.BrokerId)  -- "Broker 21649"
        OR p.BrokerName = CONCAT('Broker P', p.BrokerId)  -- "Broker P21649"
    )
    AND b.Name IS NOT NULL
    AND LTRIM(RTRIM(b.Name)) <> '';

DECLARE @updated_count INT = @@ROWCOUNT;
PRINT '✅ Updated BrokerName for ' + CAST(@updated_count AS VARCHAR) + ' proposals';
PRINT '';

-- Show final state
PRINT 'Final state:';
SELECT 
    COUNT(*) AS TotalProposals,
    SUM(CASE WHEN BrokerName IS NULL OR LTRIM(RTRIM(BrokerName)) = '' THEN 1 ELSE 0 END) AS MissingNames,
    SUM(CASE WHEN BrokerName LIKE 'Broker %' THEN 1 ELSE 0 END) AS PlaceholderNames,
    SUM(CASE WHEN BrokerName IS NOT NULL AND LTRIM(RTRIM(BrokerName)) <> '' AND BrokerName NOT LIKE 'Broker %' THEN 1 ELSE 0 END) AS ValidNames
FROM [dbo].[Proposals]
WHERE BrokerId IS NOT NULL;

PRINT '';
PRINT '============================================================';
PRINT 'FIX COMPLETE';
PRINT '============================================================';

GO
