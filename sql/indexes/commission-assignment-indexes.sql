-- =====================================================
-- Indexes for Commission Assignment Performance
-- =====================================================
-- Run once to create indexes that speed up the commission
-- assignment query on input_certificate_info (1.5M rows)
-- =====================================================

SET NOCOUNT ON;
PRINT '=== Creating Commission Assignment Indexes ===';
PRINT '';

-- Index 1: Filter + Group By columns for assignment detection
-- Covers: WHERE SplitBrokerId != PaidBrokerId, GROUP BY SplitBrokerId, PaidBrokerId
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_input_cert_BrokerAssignments' 
               AND object_id = OBJECT_ID('etl.input_certificate_info'))
BEGIN
    PRINT 'Creating IX_input_cert_BrokerAssignments...';
    CREATE NONCLUSTERED INDEX IX_input_cert_BrokerAssignments
    ON [etl].[input_certificate_info] (SplitBrokerId, PaidBrokerId)
    INCLUDE (CertStatus, RecStatus, CertEffectiveDate)
    WHERE CertStatus = 'A' AND RecStatus = 'A';
    PRINT '  ✓ Created filtered index for broker assignments';
END
ELSE
    PRINT '  Index IX_input_cert_BrokerAssignments already exists';

-- Index 2: HierarchyId on premium_split_participants (for join)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stg_premium_split_participants_HierarchyId' 
               AND object_id = OBJECT_ID('etl.stg_premium_split_participants'))
BEGIN
    PRINT 'Creating IX_stg_premium_split_participants_HierarchyId...';
    CREATE NONCLUSTERED INDEX IX_stg_premium_split_participants_HierarchyId
    ON [etl].[stg_premium_split_participants] (HierarchyId)
    INCLUDE (VersionId, BrokerUniquePartyId);
    PRINT '  ✓ Created index for HierarchyId lookups';
END
ELSE
    PRINT '  Index IX_stg_premium_split_participants_HierarchyId already exists';

-- Index 3: BrokerUniquePartyId on premium_split_participants (for join in original query)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_stg_premium_split_participants_BrokerPartyId' 
               AND object_id = OBJECT_ID('etl.stg_premium_split_participants'))
BEGIN
    PRINT 'Creating IX_stg_premium_split_participants_BrokerPartyId...';
    CREATE NONCLUSTERED INDEX IX_stg_premium_split_participants_BrokerPartyId
    ON [etl].[stg_premium_split_participants] (BrokerUniquePartyId)
    INCLUDE (HierarchyId, VersionId);
    PRINT '  ✓ Created index for BrokerUniquePartyId lookups';
END
ELSE
    PRINT '  Index IX_stg_premium_split_participants_BrokerPartyId already exists';

PRINT '';
PRINT '=== Index Creation Complete ===';
GO
