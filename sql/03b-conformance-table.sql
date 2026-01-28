-- =============================================================================
-- Group Conformance Statistics Table
-- =============================================================================
-- Captures conformance metrics for each group to guide export filtering
-- =============================================================================

PRINT 'Creating GroupConformanceStatistics table...';

DROP TABLE IF EXISTS [$(ETL_SCHEMA)].[GroupConformanceStatistics];

CREATE TABLE [$(ETL_SCHEMA)].[GroupConformanceStatistics] (
    GroupId NVARCHAR(100) NOT NULL,
    GroupName NVARCHAR(500),
    SitusState NVARCHAR(10),
    TotalCertificates INT NOT NULL,
    ConformantCertificates INT NOT NULL,
    NonConformantCertificates INT NOT NULL,
    ConformancePercentage DECIMAL(5,2) NOT NULL,
    GroupClassification NVARCHAR(50) NOT NULL, -- 'Conformant', 'Nearly Conformant (>=95%)', 'Non-Conformant'
    AnalysisDate DATETIME2 DEFAULT GETUTCDATE(),
    CreationTime DATETIME2 DEFAULT GETUTCDATE(),
    IsDeleted BIT DEFAULT 0,
    CONSTRAINT PK_GroupConformanceStatistics PRIMARY KEY (GroupId)
);

CREATE NONCLUSTERED INDEX IX_GroupConformanceStatistics_Classification
ON [$(ETL_SCHEMA)].[GroupConformanceStatistics] (GroupClassification, ConformancePercentage DESC);

PRINT 'Created [$(ETL_SCHEMA)].[GroupConformanceStatistics]';

GO
