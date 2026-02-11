CREATE PROCEDURE dbo.Usp_load_dimsalesreason
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimsalesreason AS tgt
          using adventureworks.sales.salesreason AS src
          ON src.salesreasonid = tgt.salesreasonalternatekey
          WHEN matched THEN
            UPDATE SET tgt.salesreasonalternatekey = src.salesreasonid,
                       tgt.salesreasonname = src.NAME,
                       tgt.salesreasonreasontype = src.reasontype
          WHEN NOT matched THEN
            INSERT (salesreasonalternatekey,
                    salesreasonname,
                    salesreasonreasontype)
            VALUES (src.salesreasonid,
                    src.NAME,
                    src.reasontype);

          COMMIT TRANSACTION;

          PRINT 'DimSalesReason loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimSalesReason: '
                + Error_message();
      END catch
  END; 