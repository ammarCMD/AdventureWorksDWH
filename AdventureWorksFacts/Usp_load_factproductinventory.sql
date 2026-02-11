CREATE PROCEDURE dbo.Usp_load_factproductinventory
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factproductinventory AS tgt
          using (SELECT p.productkey,
                        d.datekey,
                        Cast(th.transactiondate AS DATE) AS MovementDate,
                        Avg(th.actualcost)               AS UnitCost,
                        Sum(CASE
                              WHEN th.transactiontype IN ( 'P', 'W' ) THEN
                              th.quantity
                              ELSE 0
                            END)                         AS UnitsIn,
                        Sum(CASE
                              WHEN th.transactiontype = 'S' THEN th.quantity
                              ELSE 0
                            END)                         AS UnitsOut,
                        Max(Isnull(inv.totalbalance, 0)) AS UnitsBalance
                 FROM   adventureworks.production.transactionhistory th
                        INNER JOIN adventureworksdw_ammar.dbo.dimproduct p
                                ON th.productid = p.productalternatekey
                        INNER JOIN adventureworksdw_ammar.dbo.dimdate d
                                ON Cast(th.transactiondate AS DATE) =
                                   d.fulldatealternatekey
                        LEFT JOIN (SELECT productid,
                                          Sum(quantity) AS TotalBalance
                                   FROM
                        adventureworks.production.productinventory
                                   GROUP  BY productid) inv
                               ON th.productid = inv.productid
                 GROUP  BY p.productkey,
                           d.datekey,
                           Cast(th.transactiondate AS DATE)) AS src
          ON ( tgt.productkey = src.productkey
               AND tgt.datekey = src.datekey )
          WHEN matched THEN
            UPDATE SET tgt.unitcost = src.unitcost,
                       tgt.unitsin = src.unitsin,
                       tgt.unitsout = src.unitsout,
                       tgt.unitsbalance = src.unitsbalance,
                       tgt.movementdate = src.movementdate
          WHEN NOT matched THEN
            INSERT (productkey,
                    datekey,
                    movementdate,
                    unitcost,
                    unitsin,
                    unitsout,
                    unitsbalance)
            VALUES (src.productkey,
                    src.datekey,
                    src.movementdate,
                    src.unitcost,
                    src.unitsin,
                    src.unitsout,
                    src.unitsbalance);

          COMMIT TRANSACTION;

          PRINT 'FactProductInventory loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactProductInventory: '
                + Error_message();
      END catch
  END; 