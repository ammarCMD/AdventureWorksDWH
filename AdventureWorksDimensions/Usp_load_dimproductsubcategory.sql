CREATE PROCEDURE dbo.Usp_load_dimproductsubcategory
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE adventureworksdw_ammar.dbo.dimproductsubcategory AS tgt
          using (SELECT psc.productsubcategoryid AS altkey,
                        psc.NAME                 AS engsubcatname,
                        dpc.productcategorykey   AS prdcatkey
                 FROM   adventureworks.production.productsubcategory psc
                        INNER JOIN adventureworksdw_ammar.dbo.dimproductcategory
                                   dpc
                                ON psc.productcategoryid =
                dpc.productcategoryalternatekey) AS src
          ON tgt.productsubcategoryalternatekey = src.altkey
          WHEN matched THEN
            UPDATE SET tgt.productsubcategoryalternatekey = src.altkey,
                       tgt.englishproductsubcategoryname = src.engsubcatname,
                       tgt.spanishproductsubcategoryname = 'N/A',
                       tgt.frenchproductsubcategoryname = 'N/A',
                       tgt.productcategorykey = src.prdcatkey
          WHEN NOT matched THEN
            INSERT (productsubcategoryalternatekey,
                    englishproductsubcategoryname,
                    spanishproductsubcategoryname,
                    frenchproductsubcategoryname,
                    productcategorykey)
            VALUES (src.altkey,
                    src.engsubcatname,
                    'N/A',
                    'N/A',
                    src.prdcatkey);

          COMMIT TRANSACTION;

          PRINT 'DimProductSubcategory loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimProductSubcategory: '
                + Error_message();
      END catch
  END; 