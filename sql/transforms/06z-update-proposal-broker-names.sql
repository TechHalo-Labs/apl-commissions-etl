-- =============================================================================
-- Transform: Update Proposal Broker Names
-- =============================================================================
-- Populates BrokerName in stg_proposals from stg_brokers table
-- This ensures broker names are populated even if the initial JOIN failed
-- Runs after all proposal creation scripts (06a-06g)
-- =============================================================================

SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'TRANSFORM: Update Proposal Broker Names';
PRINT '============================================================';
PRINT '';

-- Update BrokerName from stg_brokers for all proposals with NULL/empty names
UPDATE sp
SET sp.BrokerName = COALESCE(
    NULLIF(LTRIM(RTRIM(b.Name)), ''),
    CONCAT('Broker ', sp.BrokerId)
)
FROM [etl].[stg_proposals] sp
LEFT JOIN [etl].[stg_brokers] b ON b.Id = sp.BrokerId
WHERE sp.BrokerId IS NOT NULL
    AND (
        sp.BrokerName IS NULL 
        OR LTRIM(RTRIM(sp.BrokerName)) = ''
        OR sp.BrokerName = CONCAT('Broker ', sp.BrokerId)  -- Update placeholder names too
    )
    AND b.Name IS NOT NULL
    AND LTRIM(RTRIM(b.Name)) <> '';

DECLARE @updated_count INT = @@ROWCOUNT;
PRINT 'BrokerName updated for ' + CAST(@updated_count AS VARCHAR) + ' proposals';

-- Report proposals still missing broker names
DECLARE @missing_count INT;
SELECT @missing_count = COUNT(*)
FROM [etl].[stg_proposals]
WHERE BrokerId IS NOT NULL
    AND (BrokerName IS NULL OR LTRIM(RTRIM(BrokerName)) = '' OR BrokerName = CONCAT('Broker ', BrokerId));

IF @missing_count > 0
BEGIN
    PRINT '⚠️  Warning: ' + CAST(@missing_count AS VARCHAR) + ' proposals still have missing/placeholder broker names';
    PRINT '   These proposals may have BrokerId that does not exist in stg_brokers';
END
ELSE
BEGIN
    PRINT '✅ All proposals have broker names populated';
END

PRINT '';
PRINT '============================================================';
PRINT 'UPDATE PROPOSAL BROKER NAMES COMPLETED';
PRINT '============================================================';

GO
