-- ==============================================================================
-- 02_metadata_stored_procs.sql
-- Control stored procedures for Azure Data Factory integration
-- ==============================================================================

USE MetadataControlDB;
GO

-- 1. Returns all active pipelines for ADF Lookup / ForEach activity
CREATE OR ALTER PROCEDURE dbo.sp_GetActivePipelines
AS
BEGIN
    SET NOCOUNT ON;
    SELECT 
        PipelineID,
        PipelineName,
        SourceSystem,
        SourcePathOrQuery,
        TargetDeltaTable,
        PrimaryKeyCol,
        WatermarkCol,
        ISNULL(LastWatermarkValue, '1900-01-01') AS LastWatermarkValue,
        FormatType,
        DataQualityRulesJson
    FROM dbo.PipelineControl
    WHERE IsActive = 1;
END;
GO

-- 2. Logs pipeline initiation
CREATE OR ALTER PROCEDURE dbo.sp_LogPipelineStart
    @PipelineID INT,
    @ADFRunID NVARCHAR(100),
    @LogID BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.PipelineExecutionLogs (PipelineID, ADFRunID, StartTime, Status)
    VALUES (@PipelineID, @ADFRunID, GETUTCDATE(), 'RUNNING');

    SET @LogID = SCOPE_IDENTITY();
END;
GO

-- 3. Updates pipeline completion status, audit counts, and watermark
CREATE OR ALTER PROCEDURE dbo.sp_LogPipelineEnd
    @LogID BIGINT,
    @PipelineID INT,
    @Status NVARCHAR(30),
    @RowsRead BIGINT,
    @RowsWritten BIGINT,
    @RowsQuarantined BIGINT,
    @NewWatermark NVARCHAR(100) = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Update execution log
    UPDATE dbo.PipelineExecutionLogs
    SET 
        EndTime = GETUTCDATE(),
        Status = @Status,
        RowsRead = @RowsRead,
        RowsWritten = @RowsWritten,
        RowsQuarantined = @RowsQuarantined,
        ErrorMessage = @ErrorMessage
    WHERE LogID = @LogID;

    -- Update watermark on success
    IF @Status = 'SUCCEEDED' AND @NewWatermark IS NOT NULL
    BEGIN
        UPDATE dbo.PipelineControl
        SET 
            LastWatermarkValue = @NewWatermark,
            UpdatedDate = GETUTCDATE()
        WHERE PipelineID = @PipelineID;
    END
END;
GO
