
CREATE PROCEDURE dbo.Usp_load_factcurrencyrate
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factcurrencyrate AS tgt
          using(SELECT dc.currencykey      AS ck,
                       d.datekey           AS dk,
                       cr.averagerate      AS ar,
                       cr.endofdayrate     AS edr,
                       cr.currencyratedate AS dt
                FROM   adventureworks.sales.currencyrate cr
                       INNER JOIN adventureworksdw_ammar.dbo.dimcurrency dc
                               ON dc.currencyalternatekey = cr.tocurrencycode
                       INNER JOIN adventureworksdw_ammar.dbo.dimdate d
                               ON CAST(cr.currencyratedate AS DATE) = d.fulldatealternatekey)
               AS src
          ON (tgt.currencykey = src.ck AND tgt.datekey = src.dk) 
          
          WHEN matched THEN
            UPDATE SET tgt.averagerate = src.ar,
                       tgt.endofdayrate = src.edr,
                       tgt.[date] = src.dt
                       
          WHEN NOT matched THEN
            INSERT (currencykey,
                    datekey,
                    averagerate,
                    endofdayrate,
                    [date])
            VALUES (src.ck,
                    src.dk,
                    src.ar,
                    src.edr,
                    src.dt);

          COMMIT TRANSACTION;
          PRINT 'FactCurrencyRate loaded successfully.';
      END try
      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
          PRINT 'Error occurred in usp_Load_FactCurrencyRate: ' + Error_message();
      END catch
  END;