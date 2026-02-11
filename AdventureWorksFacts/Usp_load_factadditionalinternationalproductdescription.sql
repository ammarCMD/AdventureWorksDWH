CREATE PROCEDURE dbo.usp_Load_FactAdditionalInternationalProductDescription
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO AdventureWorksDW_ammar.dbo.FactAdditionalInternationalProductDescription AS tgt
        USING (
            SELECT sub.ProductKey,
                   sub.CultureName,
                   sub.ProductDescription
            FROM   (
                SELECT dp.ProductKey,
                       c.Name AS CultureName,
                       pd.Description AS ProductDescription,
                       ROW_NUMBER() OVER (
                           PARTITION BY dp.ProductKey, c.Name 
                           ORDER BY pd.ProductDescriptionID
                       ) AS EntryRank
                FROM AdventureWorks.Production.Product p
                JOIN AdventureWorks.Production.ProductModelProductDescriptionCulture pmc
                    ON p.ProductModelID = pmc.ProductModelID
                JOIN AdventureWorks.Production.Culture c
                    ON pmc.CultureID = c.CultureID
                JOIN AdventureWorks.Production.ProductDescription pd
                    ON pmc.ProductDescriptionID = pd.ProductDescriptionID
                JOIN AdventureWorksDW_ammar.dbo.DimProduct dp
                    ON p.ProductID = dp.ProductAlternateKey
            ) AS sub
            WHERE sub.EntryRank = 1
        ) AS src
        -- FIX: Added CultureName to the ON clause
        ON (tgt.ProductKey = src.ProductKey AND tgt.CultureName = src.CultureName)

        WHEN MATCHED THEN
            UPDATE SET 
                tgt.ProductDescription = src.ProductDescription

        WHEN NOT MATCHED THEN
            INSERT (ProductKey, CultureName, ProductDescription)
            VALUES (src.ProductKey, src.CultureName, src.ProductDescription);

        COMMIT TRANSACTION;
        PRINT 'FactAdditionalInternationalProductDescription loaded successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'Error: ' + ERROR_MESSAGE();
    END CATCH
END;