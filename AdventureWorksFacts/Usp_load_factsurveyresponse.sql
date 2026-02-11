CREATE PROCEDURE dbo.Usp_load_factsurveyresponse
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factsurveyresponse AS tgt
          using (SELECT d.datekey,
                        c.customerkey,
                        ps.productcategorykey,
                        pc.englishproductcategoryname,
                        ps.productsubcategorykey,
                        ps.englishproductsubcategoryname,
                        d.fulldatealternatekey AS [Date]
                 FROM   adventureworksdw_ammar.dbo.dimcustomer c
                        CROSS JOIN
                        adventureworksdw_ammar.dbo.dimproductsubcategory ps
                        INNER JOIN adventureworksdw_ammar.dbo.dimproductcategory
                                   pc
                                ON ps.productcategorykey = pc.productcategorykey
                        INNER JOIN adventureworksdw_ammar.dbo.dimdate d
                                ON d.fulldatealternatekey =
                                   Cast(Getdate() AS DATE)
                 -- Sets survey date to today
                 WHERE  c.customerkey <= 1000 -- Limiting for performance
                ) AS src
          ON ( tgt.customerkey = src.customerkey
               AND tgt.productsubcategorykey = src.productsubcategorykey
               AND tgt.datekey = src.datekey )
          WHEN matched THEN
            UPDATE SET tgt.englishproductcategoryname =
                       src.englishproductcategoryname,
                       tgt.englishproductsubcategoryname =
                       src.englishproductsubcategoryname,
                       tgt.[date] = src.[date]
          WHEN NOT matched THEN
            INSERT (datekey,
                    customerkey,
                    productcategorykey,
                    englishproductcategoryname,
                    productsubcategorykey,
                    englishproductsubcategoryname,
                    [date])
            VALUES (src.datekey,
                    src.customerkey,
                    src.productcategorykey,
                    src.englishproductcategoryname,
                    src.productsubcategorykey,
                    src.englishproductsubcategoryname,
                    src.[date]);

          COMMIT TRANSACTION;

          PRINT 'FactSurveyResponse loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactSurveyResponse: '
                + Error_message();
      END catch
  END; 