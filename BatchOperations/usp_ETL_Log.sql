
CREATE   PROCEDURE dbo.usp_ETL_Log
(
    @ProcessID INT,
    @Stage VARCHAR(20),           -- 'START', 'END', or 'ERROR'
    @RowsInserted INT = NULL,
    @ErrorMessage VARCHAR(500) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @ProcessName VARCHAR(100),
        @TargetTable VARCHAR(100),
        @SourceTable VARCHAR(200);
 
    SELECT
        @ProcessName = ProcessName,
        @TargetTable = TargetTable,
        @SourceTable = SourceTable
    FROM dbo.ETL_Metadata
    WHERE ProcessID = @ProcessID;
    IF @Stage = 'START'
    BEGIN
        INSERT INTO dbo.ETL_Log
            (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, Status)
        VALUES
            (@ProcessID, @ProcessName, @TargetTable, @SourceTable, GETDATE(), 'Started');
    END
 
    ELSE IF @Stage = 'END'
    BEGIN
        UPDATE dbo.ETL_Log
        SET EndTime = GETDATE(),
            RowsInserted = @RowsInserted,
            Status = 'Success'
        WHERE ProcessID = @ProcessID
          AND Status = 'Started'
          AND EndTime IS NULL;
    END
 
    ELSE IF @Stage = 'ERROR'
    BEGIN
        UPDATE dbo.ETL_Log
        SET EndTime = GETDATE(),
            Status = 'Failed',
            ErrorMessage = @ErrorMessage
        WHERE ProcessID = @ProcessID
          AND Status = 'Started'
          AND EndTime IS NULL;
    END
END