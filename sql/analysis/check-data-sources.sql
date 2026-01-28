-- Check where certificate data exists
PRINT 'Checking certificate data sources...';

-- Check etl.input_certificate_info
IF OBJECT_ID('etl.input_certificate_info', 'U') IS NOT NULL
BEGIN
    DECLARE @etl_input_count INT = (SELECT COUNT(*) FROM etl.input_certificate_info);
    PRINT 'etl.input_certificate_info: ' + CAST(@etl_input_count AS VARCHAR) + ' rows';
END
ELSE
BEGIN
    PRINT 'etl.input_certificate_info: TABLE DOES NOT EXIST';
END

-- Check poc_etl.raw_certificate_info
IF OBJECT_ID('poc_etl.raw_certificate_info', 'U') IS NOT NULL
BEGIN
    DECLARE @poc_etl_count INT = (SELECT COUNT(*) FROM poc_etl.raw_certificate_info);
    PRINT 'poc_etl.raw_certificate_info: ' + CAST(@poc_etl_count AS VARCHAR) + ' rows';
END
ELSE
BEGIN
    PRINT 'poc_etl.raw_certificate_info: TABLE DOES NOT EXIST';
END

-- Check stg_groups columns
PRINT '';
PRINT 'Checking stg_groups schema...';
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'etl' 
  AND TABLE_NAME = 'stg_groups'
ORDER BY ORDINAL_POSITION;

-- Check stg_proposal_key_mapping
IF OBJECT_ID('etl.stg_proposal_key_mapping', 'U') IS NOT NULL
BEGIN
    DECLARE @mapping_count INT = (SELECT COUNT(*) FROM etl.stg_proposal_key_mapping);
    PRINT '';
    PRINT 'etl.stg_proposal_key_mapping: ' + CAST(@mapping_count AS VARCHAR) + ' rows';
END
ELSE
BEGIN
    PRINT '';
    PRINT 'etl.stg_proposal_key_mapping: TABLE DOES NOT EXIST';
END
