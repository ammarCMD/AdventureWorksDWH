
CREATE PROCEDURE dbo.Usp_load_dimproductcategory
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimproductcategory AS tgt
          using adventureworks.production.productcategory AS src
          ON src.productcategoryid = tgt.productcategoryalternatekey
          WHEN matched THEN
            UPDATE SET tgt.productcategoryalternatekey = src.productcategoryid,
                       tgt.englishproductcategoryname = src.NAME,
                       tgt.spanishproductcategoryname = 'unknown',
                       tgt.frenchproductcategoryname = 'unknown'
          WHEN NOT matched THEN
            INSERT (productcategoryalternatekey,
                    englishproductcategoryname,
                    spanishproductcategoryname,
                    frenchproductcategoryname)
            VALUES (src.productcategoryid,
                    src.NAME,
                    'unknown',
                    'unknown');

          COMMIT TRANSACTION;

          PRINT 'DimProductCategory loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimProductCategory: '
                + Error_message();
      END catch
  END; 
          