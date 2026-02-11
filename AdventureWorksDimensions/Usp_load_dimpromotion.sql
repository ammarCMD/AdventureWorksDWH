CREATE PROCEDURE dbo.Usp_load_dimpromotion
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimpromotion AS tgt
          using adventureworks.sales.specialoffer AS src
          ON tgt.promotionalternatekey = src. specialofferid
          WHEN matched THEN
            UPDATE SET tgt.promotionalternatekey = src.specialofferid,
                       tgt.englishpromotionname = src.description,
                       tgt.spanishpromotionname = 'NA',
                       tgt.frenchpromotionname = 'NA',
                       tgt.discountpct = src.discountpct,
                       tgt.englishpromotiontype = src.[type],
                       tgt.spanishpromotiontype = 'NA',
                       tgt.frenchpromotiontype = 'NA',
                       tgt.englishpromotioncategory = src.category,
                       tgt.spanishpromotioncategory = 'NA',
                       tgt.frenchpromotioncategory = 'NA',
                       tgt.startdate = src.startdate,
                       tgt.enddate = src.enddate,
                       tgt.minqty = src.minqty,
                       tgt.maxqty = src.maxqty
          WHEN NOT matched THEN
            INSERT ( promotionalternatekey,
                     englishpromotionname,
                     spanishpromotionname,
                     frenchpromotionname,
                     discountpct,
                     englishpromotiontype,
                     spanishpromotiontype,
                     frenchpromotiontype,
                     englishpromotioncategory,
                     spanishpromotioncategory,
                     frenchpromotioncategory,
                     startdate,
                     enddate,
                     minqty,
                     maxqty)
            VALUES (src.specialofferid,
                    src.description,
                    'NA',
                    'NA',
                    src.discountpct,
                    src.[type],
                    'NA',
                    'NA',
                    src.category,
                    'NA',
                    'NA',
                    src.startdate,
                    src.enddate,
                    src.minqty,
                    src.maxqty );

          COMMIT TRANSACTION;

          PRINT 'DimPromotion loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimPromotion: '
                + Error_message();
      END catch
  END;