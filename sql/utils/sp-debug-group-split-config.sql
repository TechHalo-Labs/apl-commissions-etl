SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_debug_group_split_config]
  @GroupId NVARCHAR(100)
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @GroupIdClean NVARCHAR(100) = LTRIM(RTRIM(@GroupId));
  DECLARE @GroupIdNumeric NVARCHAR(100) = @GroupIdClean;
  DECLARE @firstDigit INT = PATINDEX('%[0-9]%', @GroupIdClean);
  IF @firstDigit > 0
    SET @GroupIdNumeric = SUBSTRING(@GroupIdClean, @firstDigit, 100);

  -- Stage filtered data into a temp table with indexes to speed JSON aggregation
  CREATE TABLE #Raw (
    GroupId NVARCHAR(100),
    Product NVARCHAR(100),
    PlanCode NVARCHAR(100),
    CertEffectiveDate DATE,
    CertIssuedState CHAR(2),
    CertificateId NVARCHAR(100),
    CertSplitSeq INT,
    CertSplitPercent DECIMAL(18,2),
    SplitBrokerId NVARCHAR(50),
    SplitBrokerSeq INT,
    CommissionsSchedule NVARCHAR(100)
  );

  INSERT INTO #Raw (
    GroupId, Product, PlanCode, CertEffectiveDate, CertIssuedState,
    CertificateId, CertSplitSeq, CertSplitPercent, SplitBrokerId,
    SplitBrokerSeq, CommissionsSchedule
  )
  SELECT
    LTRIM(RTRIM(GroupId)) AS GroupId,
    LTRIM(RTRIM(Product)) AS Product,
    LTRIM(RTRIM(PlanCode)) AS PlanCode,
    TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(CertEffectiveDate)), '')) AS CertEffectiveDate,
    LEFT(LTRIM(RTRIM(CertIssuedState)), 2) AS CertIssuedState,
    LTRIM(RTRIM(CertificateId)) AS CertificateId,
    TRY_CAST(NULLIF(LTRIM(RTRIM(CertSplitSeq)), '') AS INT) AS CertSplitSeq,
    TRY_CAST(NULLIF(LTRIM(RTRIM(CertSplitPercent)), '') AS DECIMAL(18,2)) AS CertSplitPercent,
    LTRIM(RTRIM(SplitBrokerId)) AS SplitBrokerId,
    TRY_CAST(NULLIF(LTRIM(RTRIM(SplitBrokerSeq)), '') AS INT) AS SplitBrokerSeq,
    LTRIM(RTRIM(CommissionsSchedule)) AS CommissionsSchedule
  FROM [etl].[raw_certificate_info]
  WHERE LTRIM(RTRIM(GroupId)) IN (@GroupIdClean, @GroupIdNumeric)
    AND LTRIM(RTRIM(CertStatus)) = 'A'
    AND LTRIM(RTRIM(RecStatus)) = 'A';

  CREATE INDEX IX_Raw_CertSplit ON #Raw (CertificateId, CertSplitSeq, SplitBrokerSeq);
  CREATE INDEX IX_Raw_Group ON #Raw (GroupId);

  ;WITH Raw AS (
    SELECT * FROM #Raw
  ),
  SplitTiers AS (
    SELECT
      r.CertificateId,
      r.CertSplitSeq,
      r.CertSplitPercent,
      r.GroupId,
      tiersJson = (
        SELECT
          r2.SplitBrokerSeq AS [level],
          r2.SplitBrokerId AS brokerId,
          r2.CommissionsSchedule AS schedule
        FROM Raw r2
        WHERE r2.CertificateId = r.CertificateId
          AND r2.CertSplitSeq = r.CertSplitSeq
        ORDER BY r2.SplitBrokerSeq
        FOR JSON PATH
      )
    FROM Raw r
    GROUP BY r.CertificateId, r.CertSplitSeq, r.CertSplitPercent, r.GroupId
  ),
  SplitHierarchy AS (
    SELECT
      st.CertificateId,
      st.CertSplitSeq,
      hierarchyJson = (
        SELECT
          st.GroupId AS groupId,
          st.CertSplitPercent AS splitPercent,
          JSON_QUERY(st.tiersJson) AS tiers
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      )
    FROM SplitTiers st
  ),
  SplitHierarchyHash AS (
    SELECT
      sh.CertificateId,
      sh.CertSplitSeq,
      hierarchyJson = sh.hierarchyJson,
      hierarchyHash = UPPER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(MAX), sh.hierarchyJson)), 2))
    FROM SplitHierarchy sh
  ),
  SplitConfig AS (
    SELECT
      shh.CertificateId,
      configJson = (
        SELECT
          st.CertSplitPercent AS pct,
          shh2.hierarchyHash AS hierarchyHash
        FROM SplitHierarchyHash shh2
        INNER JOIN SplitTiers st
          ON st.CertificateId = shh2.CertificateId
         AND st.CertSplitSeq = shh2.CertSplitSeq
        WHERE shh2.CertificateId = shh.CertificateId
        ORDER BY st.CertSplitSeq
        FOR JSON PATH
      )
    FROM SplitHierarchyHash shh
    GROUP BY shh.CertificateId
  ),
  SplitConfigHash AS (
    SELECT
      sc.CertificateId,
      sc.configJson,
      SplitConfigHash = UPPER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(MAX), sc.configJson)), 2))
    FROM SplitConfig sc
  ),
  CertificateCounts AS (
    SELECT
      r.CertificateId,
      RecordCount = COUNT(*)
    FROM Raw r
    GROUP BY r.CertificateId
  )
  SELECT DISTINCT
    r.CertEffectiveDate,
    r.Product,
    r.PlanCode,
    r.CertIssuedState,
    cc.RecordCount,
    sch.SplitConfigHash,
    sch.configJson AS SplitConfigJson
  FROM Raw r
  INNER JOIN SplitConfigHash sch ON sch.CertificateId = r.CertificateId
  INNER JOIN CertificateCounts cc ON cc.CertificateId = r.CertificateId
  ORDER BY
    r.CertEffectiveDate,
    r.Product,
    r.PlanCode;
END
GO
