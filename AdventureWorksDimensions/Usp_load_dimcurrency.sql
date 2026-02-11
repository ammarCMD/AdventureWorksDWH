CREATE PROCEDURE dbo.Usp_load_dimcurrency
    @BatchSize INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsAffected INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        WITH BatchSource AS
        (
            SELECT TOP (@BatchSize)
                   src.CurrencyCode,
                   src.Name
            FROM AdventureWorks.Sales.Currency src
            ORDER BY src.CurrencyCode
        )
        MERGE INTO AdventureWorksDW_ammar.dbo.DimCurrency AS tgt
        USING BatchSource AS src
            ON tgt.CurrencyAlternateKey = src.CurrencyCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.CurrencyName = src.Name
        WHEN NOT MATCHED THEN
            INSERT (CurrencyAlternateKey, CurrencyName)
            VALUES (src.CurrencyCode, src.Name);

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH

    SELECT @RowsAffected AS RowsInserted;
END