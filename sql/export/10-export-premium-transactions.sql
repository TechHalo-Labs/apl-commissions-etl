-- =====================================================
-- Export PremiumTransactions from etl staging to dbo
-- Only exports transactions that don't already exist
-- Production schema: certificateId, transactionDate, premiumAmount, 
--   billingPeriodStart, billingPeriodEnd, paymentStatus, sourceSystem, etc.
-- Staging schema: Id, CertificateId, TransactionDate, PremiumAmount,
--   BillingPeriodStart, BillingPeriodEnd, PaymentStatus, SourceSystem, CreatedDate, IsDeleted
-- =====================================================

PRINT 'Exporting missing PremiumTransactions to dbo.PremiumTransactions...';

-- PremiumTransactions has IDENTITY on Id
-- Insert using natural key match (certificateId + transactionDate + premiumAmount)

INSERT INTO [dbo].[PremiumTransactions] (
    certificateId, transactionDate, premiumAmount, 
    billingPeriodStart, billingPeriodEnd, paymentStatus, sourceSystem,
    CreatedDate, isDryRun, sourcePolicyId, sourceTagIds,
    CreationTime, IsDeleted
)
SELECT 
    spt.CertificateId AS certificateId,
    COALESCE(spt.TransactionDate, CAST(GETUTCDATE() AS DATE)) AS transactionDate,
    COALESCE(spt.PremiumAmount, 0) AS premiumAmount,
    COALESCE(spt.BillingPeriodStart, spt.TransactionDate, CAST(GETUTCDATE() AS DATE)) AS billingPeriodStart,
    COALESCE(spt.BillingPeriodEnd, spt.TransactionDate, CAST(GETUTCDATE() AS DATE)) AS billingPeriodEnd,
    COALESCE(spt.PaymentStatus, 'Processed') AS paymentStatus,
    COALESCE(spt.SourceSystem, 'ETL') AS sourceSystem,
    COALESCE(spt.CreatedDate, GETUTCDATE()) AS CreatedDate,
    0 AS isDryRun,
    spt.Id AS sourcePolicyId,
    NULL AS sourceTagIds,
    COALESCE(spt.CreatedDate, GETUTCDATE()) AS CreationTime,
    COALESCE(spt.IsDeleted, 0) AS IsDeleted
FROM [etl].[stg_premium_transactions] spt
WHERE spt.CertificateId IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM [dbo].[PremiumTransactions] pt
    WHERE pt.certificateId = spt.CertificateId
      AND pt.transactionDate = spt.TransactionDate
      AND pt.premiumAmount = spt.PremiumAmount
);

DECLARE @txCount INT = @@ROWCOUNT;
PRINT 'PremiumTransactions exported: ' + CAST(@txCount AS VARCHAR);

DECLARE @totalTx INT;
SELECT @totalTx = COUNT(*) FROM [dbo].[PremiumTransactions];
PRINT 'Total transactions in dbo: ' + CAST(@totalTx AS VARCHAR);
GO

PRINT '=== PremiumTransaction Export Complete ===';
