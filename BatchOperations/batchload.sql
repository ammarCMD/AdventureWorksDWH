CREATE PROCEDURE dbo.batchload
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @BatchSize INT = 100,
        @StartRow INT,
        @EndRow INT,
        @TotalRows INT,
        @RowsInserted INT,
        @ProcessID int = 1;

    -------------------------------------------------------
    -- 1. Determine restart point
    -------------------------------------------------------
    SELECT
        @StartRow = ISNULL(MAX(EndRow), 0) + 1
    FROM etl_batchlog
    WHERE ProcessID = @ProcessID
      AND Status = 'SUCCESS';

    -------------------------------------------------------
    -- 2. Prepare source data with row numbers
    -------------------------------------------------------
    IF OBJECT_ID('tempdb..#SrcData') IS NOT NULL
        DROP TABLE #SrcData;

    SELECT
    	dcs.CustomerID,
        dcs.CustomerName,
        dcs.City,
        ROW_NUMBER() OVER (ORDER BY dcs.CustomerName) AS RowNum
    INTO #SrcData
    FROM AdventureWorksDW_ammar.dbo.DimCustomerSimple dcs;

    SELECT @TotalRows = COUNT(*) FROM #SrcData;

    -------------------------------------------------------
    -- 3. Batch loop
    -------------------------------------------------------
    WHILE @StartRow <= @TotalRows
    BEGIN
        SET @EndRow = @StartRow + @BatchSize - 1;
        SET @RowsInserted = 0;

        BEGIN TRY
            BEGIN TRANSACTION;

            MERGE AdventureWorksDW_ammar.dbo.StgCustomerSimple AS tgt
            USING
            (
                SELECT customerid, CustomerName, City
                FROM #SrcData
                WHERE RowNum BETWEEN @StartRow AND @EndRow
            ) AS src
            ON tgt.CustomerName = src.CustomerName

            WHEN MATCHED THEN
                UPDATE SET
                	tgt.customerid = src.customerid,
                    tgt.CustomerName = src.CustomerName,
                    tgt.City = src.City

            WHEN NOT MATCHED THEN
                INSERT (customerid, CustomerName, City)
                VALUES (src.customerid, src.CustomerName, src.City);

            SET @RowsInserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            ---------------------------------------------------
            -- Log SUCCESS
            ---------------------------------------------------
            INSERT INTO etl_batchlog
            (
                ProcessID,
                StartRow,
                EndRow,
                RowsInserted,
                Status,
                ErrorMessage,
                LoggedAt
            )
            VALUES
            (
                @ProcessID,
                @StartRow,
                @EndRow,
                @RowsInserted,
                'SUCCESS',
                NULL,
                GETDATE()
            );
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;

            ---------------------------------------------------
            -- Log FAILURE
            ---------------------------------------------------
            INSERT INTO etl_batchlog
            (
                ProcessID,
                StartRow,
                EndRow,
                RowsInserted,
                Status,
                ErrorMessage,
                LoggedAt
            )
            VALUES
            (
                @ProcessID,
                @StartRow,
                @EndRow,
                0,
                'FAILED',
                ERROR_MESSAGE(),
                GETDATE()
            );

            -- Stop processing on failure (restartable behavior)
            RETURN;
        END CATCH;

        SET @StartRow = @EndRow + 1;
    END;

    PRINT 'Batch load completed successfully.';
END; 