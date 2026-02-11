CREATE PROCEDURE dbo.Usp_load_factinternetsalesreason
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          WITH cte
               AS (SELECT salesreasonkey,
                          Row_number()
                            OVER (
                              ORDER BY salesreasonkey) - 1 AS ReasonIdx,
                          Count(*)
                            OVER ()                        AS TotalReasons
                   FROM   adventureworksdw_ammar.dbo.dimsalesreason),
               cte2
               AS (SELECT salesordernumber,
                          salesorderlinenumber,
                          Row_number()
                            OVER (
                              ORDER BY salesordernumber, salesorderlinenumber)
                          - 1
                          AS
                          SaleIdx
                   FROM   adventureworksdw_ammar.dbo.factinternetsales)
          MERGE INTO adventureworksdw_ammar.dbo.factinternetsalesreason AS tgt
          using (SELECT c2.salesordernumber     AS son,
                        c2.salesorderlinenumber AS soln,
                        c1.salesreasonkey       AS srk
                 FROM   cte2 c2
                        INNER JOIN cte c1
                                ON ( c2.saleidx % c1.totalreasons ) =
                                   c1.reasonidx
                ) AS
                src
          ON ( tgt.salesordernumber = src.son
               AND tgt.salesorderlinenumber = src.soln
               AND tgt.salesreasonkey = src.srk )
          WHEN matched THEN
            UPDATE SET tgt.salesreasonkey = src.srk
          WHEN NOT matched THEN
            INSERT (salesordernumber,
                    salesorderlinenumber,
                    salesreasonkey)
            VALUES (src.son,
                    src.soln,
                    src.srk);

          COMMIT TRANSACTION;

          PRINT 'FactInternetSalesReason loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactInternetSalesReason: '
                + Error_message();
      END catch
  END; 