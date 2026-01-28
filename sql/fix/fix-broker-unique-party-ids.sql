-- =============================================================================
-- One-Time Fix: Populate BrokerUniquePartyId on Proposals
-- Corrects missing BrokerUniquePartyId on existing production proposals
-- Run once to fix current production, then fixed by ETL for future data
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '============================================================';
PRINT 'ONE-TIME FIX: BROKER UNIQUE PARTY IDS ON PROPOSALS';
PRINT '============================================================';
PRINT '';

-- =============================================================================
-- Backup current state
-- =============================================================================
PRINT 'Step 1: Backing up current state...';

IF OBJECT_ID('new_data.Proposals_broker_backup_20260127', 'U') IS NOT NULL
    DROP TABLE [new_data].[Proposals_broker_backup_20260127];

SELECT * 
INTO [new_data].[Proposals_broker_backup_20260127]
FROM [dbo].[Proposals];

PRINT 'Proposals backed up: ' + CAST(@@ROWCOUNT AS VARCHAR);
PRINT '';

-- =============================================================================
-- Populate BrokerUniquePartyId where NULL
-- =============================================================================
PRINT 'Step 2: Populating BrokerUniquePartyId from BrokerId...';

UPDATE p
SET p.BrokerUniquePartyId = b.ExternalPartyId,
    p.LastModificationTime = GETUTCDATE()
FROM [dbo].[Proposals] p
INNER JOIN [dbo].[Brokers] b ON b.Id = p.BrokerId
WHERE (p.BrokerUniquePartyId IS NULL OR p.BrokerUniquePartyId = '')
  AND b.ExternalPartyId IS NOT NULL
  AND b.ExternalPartyId <> ''
  AND p.BrokerId IS NOT NULL
  AND p.BrokerId != 0;

PRINT 'Proposals updated: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- =============================================================================
-- Handle proposals where BrokerId is 0 or NULL
-- =============================================================================
PRINT '';
PRINT 'Step 3: Checking proposals with BrokerId = 0 or NULL...';

DECLARE @no_broker_count INT = (
    SELECT COUNT(*) 
    FROM [dbo].[Proposals]
    WHERE (BrokerUniquePartyId IS NULL OR BrokerUniquePartyId = '')
      AND (BrokerId IS NULL OR BrokerId = 0)
);

PRINT 'Proposals with BrokerId = 0 or NULL: ' + CAST(@no_broker_count AS VARCHAR);

IF @no_broker_count > 0
BEGIN
    PRINT '';
    PRINT 'Attempting to resolve from EmployerGroups.PrimaryBrokerId...';
    
    UPDATE p
    SET p.BrokerId = g.PrimaryBrokerId,
        p.BrokerUniquePartyId = b.ExternalPartyId,
        p.BrokerName = b.Name,
        p.LastModificationTime = GETUTCDATE()
    FROM [dbo].[Proposals] p
    INNER JOIN [dbo].[EmployerGroups] g ON g.Id = p.GroupId
    INNER JOIN [dbo].[Brokers] b ON b.Id = g.PrimaryBrokerId
    WHERE (p.BrokerUniquePartyId IS NULL OR p.BrokerUniquePartyId = '')
      AND (p.BrokerId IS NULL OR p.BrokerId = 0)
      AND g.PrimaryBrokerId IS NOT NULL;
    
    PRINT 'Proposals resolved from EmployerGroups.PrimaryBrokerId: ' + CAST(@@ROWCOUNT AS VARCHAR);
END

-- =============================================================================
-- Verification
-- =============================================================================
PRINT '';
PRINT '============================================================';
PRINT 'VERIFICATION';
PRINT '============================================================';

SELECT 
    'Proposals by BrokerUniquePartyId status' AS metric,
    CASE 
        WHEN BrokerUniquePartyId IS NOT NULL AND BrokerUniquePartyId <> '' THEN 'Has BrokerUniquePartyId'
        ELSE 'Missing BrokerUniquePartyId'
    END AS status,
    COUNT(*) AS cnt,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [dbo].[Proposals]) AS DECIMAL(5,2)) AS pct
FROM [dbo].[Proposals]
GROUP BY CASE 
    WHEN BrokerUniquePartyId IS NOT NULL AND BrokerUniquePartyId <> '' THEN 'Has BrokerUniquePartyId'
    ELSE 'Missing BrokerUniquePartyId'
END;

-- Show sample fixed proposals
SELECT TOP 5
    Id,
    ProposalNumber,
    GroupName,
    BrokerId,
    BrokerUniquePartyId,
    BrokerName
FROM [dbo].[Proposals]
WHERE BrokerUniquePartyId IS NOT NULL
ORDER BY Id;

PRINT '';
PRINT '============================================================';
PRINT 'BROKER UNIQUE PARTY ID FIX COMPLETED';
PRINT '============================================================';
PRINT '';
PRINT 'Backup created: new_data.Proposals_broker_backup_20260127';

GO
