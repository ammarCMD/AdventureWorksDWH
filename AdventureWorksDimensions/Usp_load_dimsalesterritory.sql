CREATE PROCEDURE dbo.Usp_load_dimsalesterritory
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimsalesterritory AS tgt
          using adventureworks.sales.salesterritory AS src
          ON tgt.salesterritorykey = src.territoryid
          WHEN matched THEN
            UPDATE SET tgt.salesterritoryalternatekey = src.territoryid
          WHEN NOT matched THEN
            INSERT (salesterritoryalternatekey,
                    salesterritoryregion,
                    salesterritorycountry,
                    salesterritorygroup,
                    salesterritoryimage)
            VALUES (src.territoryid,
                    src.countryregioncode,
                    src.NAME,
                    src.[group],
                    Cast('unknown' AS VARBINARY));

          COMMIT TRANSACTION;

          PRINT 'DimSalesTerritory loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimSalesTerritory: '
                + Error_message();
      END catch
  END;