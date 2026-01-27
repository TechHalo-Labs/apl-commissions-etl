-- Diagnose NULL BrokerId issue
SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'DIAGNOSIS: NULL BrokerId in Non-Conformant Processing';
PRINT '============================================================';
PRINT '';

-- Check 1: How many input_certificate_info records have NULL or empty SplitBrokerId?
PRINT 'Check 1: NULL/Empty SplitBrokerId in input_certificate_info';
PRINT '-----------------------------------------------------------';
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN SplitBrokerId IS NULL OR LTRIM(RTRIM(SplitBrokerId)) = '' THEN 1 ELSE 0 END) as null_or_empty,
    SUM(CASE WHEN SplitBrokerId IS NOT NULL AND LTRIM(RTRIM(SplitBrokerId)) <> '' THEN 1 ELSE 0 END) as has_value
FROM [etl].[input_certificate_info];
PRINT '';

-- Check 2: Sample records with NULL SplitBrokerId
PRINT 'Check 2: Sample records with NULL/Empty SplitBrokerId';
PRINT '------------------------------------------------------';
SELECT TOP 10
    CertificateId,
    GroupId,
    Product,
    PlanCode,
    SplitBrokerId,
    WritingBrokerID,
    CertSplitSeq,
    SplitBrokerSeq
FROM [etl].[input_certificate_info]
WHERE SplitBrokerId IS NULL OR LTRIM(RTRIM(SplitBrokerId)) = ''
ORDER BY CertificateId;
PRINT '';

-- Check 3: Does WritingBrokerID have values?
PRINT 'Check 3: WritingBrokerID vs SplitBrokerId comparison';
PRINT '-----------------------------------------------------';
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN WritingBrokerID IS NOT NULL AND LTRIM(RTRIM(WritingBrokerID)) <> '' THEN 1 ELSE 0 END) as has_writing_broker,
    SUM(CASE WHEN SplitBrokerId IS NOT NULL AND LTRIM(RTRIM(SplitBrokerId)) <> '' THEN 1 ELSE 0 END) as has_split_broker,
    SUM(CASE WHEN (WritingBrokerID IS NOT NULL AND LTRIM(RTRIM(WritingBrokerID)) <> '')
                   AND (SplitBrokerId IS NULL OR LTRIM(RTRIM(SplitBrokerId)) = '') THEN 1 ELSE 0 END) as has_writing_but_no_split
FROM [etl].[input_certificate_info];
PRINT '';

-- Check 4: Can we use WritingBrokerID as fallback?
PRINT 'Check 4: Sample using WritingBrokerID as fallback';
PRINT '--------------------------------------------------';
SELECT TOP 10
    CertificateId,
    GroupId,
    Product,
    SplitBrokerId,
    WritingBrokerID,
    COALESCE(SplitBrokerId, WritingBrokerID) as FallbackBrokerId,
    CertSplitSeq,
    SplitBrokerSeq
FROM [etl].[input_certificate_info]
WHERE SplitBrokerId IS NULL OR LTRIM(RTRIM(SplitBrokerId)) = ''
ORDER BY CertificateId;
PRINT '';

-- Check 5: Check stg_groups columns
PRINT 'Check 5: Check stg_groups columns';
PRINT '-----------------------------------';
SELECT TOP 5 COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'etl' 
AND TABLE_NAME = 'stg_groups'
ORDER BY ORDINAL_POSITION;
PRINT '';

PRINT '============================================================';
PRINT 'DIAGNOSIS COMPLETE';
PRINT '============================================================';
