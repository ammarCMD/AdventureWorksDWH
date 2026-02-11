
CREATE PROCEDURE dbo.Usp_load_newfactcurrencyrate
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          WITH cte
               AS (SELECT dc.currencyalternatekey                  AS cid,
                          fcr.averagerate                          AS ar,
                          fcr.[date]                               AS cd,
                          fcr.endofdayrate                         AS edr,
                          dc.currencykey                           AS ck,
                          Row_number()
                            OVER (
                              ORDER BY fcr.[date], dc.currencykey) AS RowID
                   FROM   adventureworksdw_ammar.dbo.factcurrencyrate fcr
                          INNER JOIN adventureworksdw_ammar.dbo.dimcurrency dc
                                  ON dc.currencykey = fcr.currencykey),
               cte_2
               AS (SELECT datekey               AS dk,
                          Row_number()
                            OVER (
                              ORDER BY datekey) AS RowID
                   FROM   adventureworksdw_ammar.dbo.dimdate)
          MERGE INTO adventureworksdw_ammar.dbo.newfactcurrencyrate AS tgt
          using (SELECT cte.ar,
                        cte.cid,
                        cte.cd,
                        cte.edr,
                        cte.ck,
                        cte_2.dk
                 FROM   cte
                        LEFT JOIN cte_2
                               ON cte.rowid = cte_2.rowid) AS src
          ON ( tgt.currencykey = src.ck
               AND tgt.datekey = src.dk )
          WHEN matched THEN
            UPDATE SET tgt.averagerate = src.ar,
                       tgt.currencyid = src.cid,
                       tgt.currencydate = src.cd,
                       tgt.endofdayrate = src.edr,
                       tgt.currencykey = src.ck,
                       tgt.datekey = src.dk
          WHEN NOT matched THEN
            INSERT (averagerate,
                    currencyid,
                    currencydate,
                    endofdayrate,
                    currencykey,
                    datekey)
            VALUES (src.ar,
                    src.cid,
                    src.cd,
                    src.edr,
                    src.ck,
                    src.dk);

          COMMIT TRANSACTION;

          PRINT 'NewFactCurrencyRate loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_NewFactCurrencyRate: '
                + Error_message();
      END catch
  END;