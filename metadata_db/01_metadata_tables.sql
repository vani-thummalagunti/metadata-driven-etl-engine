-- ==============================================================================
-- 01_metadata_tables.sql
-- Database: MetadataControlDB (SQL Server)
-- Control tables for Metadata-Driven Ingestion & Orchestration Framework
-- ==============================================================================

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'MetadataControlDB')
BEGIN
    CREATE DATABASE MetadataControlDB;
END
GO

USE MetadataControlDB;
GO

-- 1. PipelineControl: Registers all datasets to be ingested dynamically
CREATE TABLE dbo.PipelineControl (
    PipelineID INT IDENTITY(1,1) PRIMARY KEY,
    PipelineName NVARCHAR(100) NOT NULL UNIQUE,
    SourceSystem NVARCHAR(50) NOT NULL,            -- e.g., 'ERP_SAP', 'SQL_SERVER', 'REST_API'
    SourcePathOrQuery NVARCHAR(500) NOT NULL,      -- e.g., 'dbo.Orders', 'landing/erp/orders/'
    TargetDeltaTable NVARCHAR(100) NOT NULL,       -- e.g., 'silver_erp_orders'
    PrimaryKeyCol NVARCHAR(100) NOT NULL,          -- e.g., 'OrderID'
    WatermarkCol NVARCHAR(100) NULL,              -- e.g., 'UpdatedAt'
    LastWatermarkValue NVARCHAR(100) NULL,         -- e.g., '2024-01-01 00:00:00'
    FormatType NVARCHAR(20) DEFAULT 'PARQUET',    -- 'JSON', 'CSV', 'PARQUET'
    DataQualityRulesJson NVARCHAR(MAX) NULL,       -- JSON array of validation assertions
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETUTCDATE(),
    UpdatedDate DATETIME DEFAULT GETUTCDATE()
);
GO

-- Seed initial pipelines
INSERT INTO dbo.PipelineControl (PipelineName, SourceSystem, SourcePathOrQuery, TargetDeltaTable, PrimaryKeyCol, WatermarkCol, LastWatermarkValue, DataQualityRulesJson)
VALUES
('ERP_Orders', 'ERP_SAP', 'landing/erp/orders/*.parquet', 'silver_erp_orders', 'OrderID', 'OrderDate', '2024-01-01 00:00:00',
 '[{"rule": "not_null", "column": "OrderID"}, {"rule": "range", "column": "TotalAmount", "min": 0}]'),
('ERP_Suppliers', 'ERP_SAP', 'landing/erp/suppliers/*.parquet', 'silver_erp_suppliers', 'SupplierID', 'UpdatedDate', '2024-01-01 00:00:00',
 '[{"rule": "not_null", "column": "SupplierID"}, {"rule": "not_null", "column": "SupplierName"}]'),
('ERP_Inventory', 'ERP_SAP', 'landing/erp/inventory/*.parquet', 'silver_erp_inventory', 'InventoryID', 'SnapshotDate', '2024-01-01 00:00:00',
 '[{"rule": "not_null", "column": "InventoryID"}, {"rule": "range", "column": "QuantityOnHand", "min": 0}]');
GO

-- 2. PipelineExecutionLogs: Central audit trail for all runs
CREATE TABLE dbo.PipelineExecutionLogs (
    LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
    PipelineID INT NOT NULL FOREIGN KEY REFERENCES dbo.PipelineControl(PipelineID),
    ADFRunID NVARCHAR(100) NOT NULL,
    StartTime DATETIME NOT NULL DEFAULT GETUTCDATE(),
    EndTime DATETIME NULL,
    Status NVARCHAR(30) NOT NULL, -- 'RUNNING', 'SUCCEEDED', 'FAILED'
    RowsRead BIGINT NULL,
    RowsWritten BIGINT NULL,
    RowsQuarantined BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL
);
CREATE NONCLUSTERED INDEX IX_Logs_Pipeline_Status ON dbo.PipelineExecutionLogs(PipelineID, Status);
GO
