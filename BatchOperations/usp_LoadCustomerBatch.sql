CREATE PROCEDURE usp_LoadCustomerBatch
    @Reset BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @ProcessID INT,
        @BatchSize INT = 20,     
        @StartRow INT,
        @EndRow INT,
        @MaxID INT,
        @RowsInserted INT = 0,
        @ErrorMessage NVARCHAR(500);

    SELECT @ProcessID = ProcessID 
    FROM dbo.ETL_Metadata 
    WHERE ProcessName = OBJECT_NAME(@@PROCID);

    EXEC usp_ETL_Log @ProcessID = @ProcessID, @Stage = 'START';

    SELECT @MaxID = MAX(CustomerID) FROM dbo.StgCustomerSimple;

    IF @Reset = 1
        SET @StartRow = 1;
    ELSE
     SELECT @StartRow = MIN(StartRow)
FROM dbo.ETL_BatchLog
WHERE ProcessID = @ProcessID AND Status = 'Failed';


    WHILE @StartRow <= @MaxID
    BEGIN
        SET @EndRow = @StartRow + @BatchSize - 1;

        BEGIN TRY
            BEGIN TRANSACTION;

            MERGE stgCustomerSimple AS T
            USING (
                SELECT CustomerID, CustomerName, City
                FROM dbo.DimCustomerSimple
                WHERE CustomerID BETWEEN @StartRow AND @EndRow
            ) AS S
            ON T.CustomerID = S.CustomerID
            WHEN MATCHED THEN 
                UPDATE SET 
                    T.CustomerName = S.CustomerName,
                    T.City = S.City
            WHEN NOT MATCHED THEN 
                INSERT (CustomerID, CustomerName, City)
                VALUES (S.CustomerID, S.CustomerName, S.City);

            SET @RowsInserted += @@ROWCOUNT;

            IF @EndRow = 40
                THROW 50001, 'Simulated error in batch 2', 1;

            COMMIT TRANSACTION;

            INSERT INTO dbo.ETL_BatchLog (ProcessID, StartRow, EndRow, RowsInserted, Status)
            VALUES (@ProcessID, @StartRow, @EndRow, @@ROWCOUNT, 'Success');
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION;

            SET @ErrorMessage = ERROR_MESSAGE();

            INSERT INTO dbo.ETL_BatchLog (ProcessID, StartRow, EndRow, RowsInserted, Status, ErrorMessage)
            VALUES (@ProcessID, @StartRow, @EndRow, NULL, 'Failed', @ErrorMessage);

            EXEC dbo.usp_ETL_Log 
                @ProcessID = @ProcessID, 
                @Stage = 'ERROR', 
                @ErrorMessage = @ErrorMessage;

            RETURN;
        END CATCH;

        SET @StartRow = @EndRow + 1;
    END

    EXEC dbo.usp_ETL_Log 
        @ProcessID = @ProcessID, 
        @Stage = 'END', 
        @RowsInserted = @RowsInserted;
END; 