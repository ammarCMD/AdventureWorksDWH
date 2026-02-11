CREATE PROCEDURE dbo.Usp_load_factinternetsales
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          WITH salessource
               AS (SELECT p.productkey,
                          d1.datekey
                             AS OrderDateKey,
                          d2.datekey
                             AS DueDateKey,
                          d3.datekey
                             AS ShipDateKey,
                          c.customerkey,
                          pr.promotionkey,
                          Isnull(curr.currencykey, 1)
                             AS CurrencyKey,
                          st.salesterritorykey,
                          soh.salesordernumber
                             AS SalesOrderNumber,
                          Cast(sod.salesorderdetailid AS INT)
                             AS SalesOrderLineNumber,
                          Cast(soh.revisionnumber AS INT)
                             AS RevisionNumber,
                          sod.orderqty
                             AS OrderQuantity,
                          sod.unitprice,
                          ( sod.orderqty * sod.unitprice )
                             AS ExtendedAmount,
                          sod.unitpricediscount
                             AS UnitPriceDiscountPct,
                          ( sod.orderqty * sod.unitprice *
                          sod.unitpricediscount )
                             AS DiscountAmount,
                          p.standardcost
                             AS ProductStandardCost,
                          ( sod.orderqty * p.standardcost )
                             AS TotalProductCost,
                          sod.linetotal
                             AS SalesAmount,
                          Cast(soh.taxamt * ( sod.linetotal /
                                              NULLIF(soh.subtotal,
                                              0)
                                            ) AS
                               MONEY)
                             AS
                          TaxAmt,
                          Cast(soh.freight * ( sod.linetotal /
                                               NULLIF(soh.subtotal, 0)
                                             )
                               AS
                               MONEY)
                             AS
                          Freight,
                          sod.carriertrackingnumber,-- Found in SalesOrderDetail
                          soh.purchaseordernumber
                             AS CustomerPONumber,-- Found in SalesOrderHeader
                          Cast(soh.orderdate AS DATE)
                             AS OrderDate,
                          Cast(soh.duedate AS DATE)
                             AS DueDate,
                          Cast(soh.shipdate AS DATE)
                             AS ShipDate
                   FROM   adventureworks.sales.salesorderheader soh
                          INNER JOIN adventureworks.sales.salesorderdetail sod
                                  ON soh.salesorderid = sod.salesorderid
                          INNER JOIN adventureworksdw_ammar.dbo.dimproduct p
                                  ON sod.productid = p.productalternatekey
                          INNER JOIN adventureworksdw_ammar.dbo.dimcustomer c
                                  ON soh.customerid = c.customeralternatekey
                          INNER JOIN adventureworksdw_ammar.dbo.dimpromotion pr
                                  ON sod.specialofferid =
                                     pr.promotionalternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimcurrency curr
                                 ON soh.currencyrateid = curr.currencykey
                          INNER JOIN
                          adventureworksdw_ammar.dbo.dimsalesterritory
                          st
                                  ON soh.territoryid =
                                     st.salesterritoryalternatekey
                          INNER JOIN adventureworksdw_ammar.dbo.dimdate d1
                                  ON Cast(soh.orderdate AS DATE) =
                                     d1.fulldatealternatekey
                          INNER JOIN adventureworksdw_ammar.dbo.dimdate d2
                                  ON Cast(soh.duedate AS DATE) =
                                     d2.fulldatealternatekey
                          INNER JOIN adventureworksdw_ammar.dbo.dimdate d3
                                  ON Cast(soh.shipdate AS DATE) =
                                     d3.fulldatealternatekey)
          MERGE INTO adventureworksdw_ammar.dbo.factinternetsales AS tgt
          using salessource AS src
          ON ( tgt.salesordernumber = src.salesordernumber
               AND tgt.salesorderlinenumber = src.salesorderlinenumber )
          WHEN matched THEN
            UPDATE SET tgt.orderquantity = src.orderquantity,
                       tgt.salesamount = src.salesamount,
                       tgt.taxamt = src.taxamt
          WHEN NOT matched THEN
            INSERT (productkey,
                    orderdatekey,
                    duedatekey,
                    shipdatekey,
                    customerkey,
                    promotionkey,
                    currencykey,
                    salesterritorykey,
                    salesordernumber,
                    salesorderlinenumber,
                    revisionnumber,
                    orderquantity,
                    unitprice,
                    extendedamount,
                    unitpricediscountpct,
                    discountamount,
                    productstandardcost,
                    totalproductcost,
                    salesamount,
                    taxamt,
                    freight,
                    carriertrackingnumber,
                    customerponumber,
                    orderdate,
                    duedate,
                    shipdate)
            VALUES (src.productkey,
                    src.orderdatekey,
                    src.duedatekey,
                    src.shipdatekey,
                    src.customerkey,
                    src.promotionkey,
                    src.currencykey,
                    src.salesterritorykey,
                    src.salesordernumber,
                    src.salesorderlinenumber,
                    src.revisionnumber,
                    src.orderquantity,
                    src.unitprice,
                    src.extendedamount,
                    src.unitpricediscountpct,
                    src.discountamount,
                    src.productstandardcost,
                    src.totalproductcost,
                    src.salesamount,
                    src.taxamt,
                    src.freight,
                    src.carriertrackingnumber,
                    src.customerponumber,
                    src.orderdate,
                    src.duedate,
                    src.shipdate);

          COMMIT TRANSACTION;

          PRINT 'FactInternetSales loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactInternetSales: '
                + Error_message();
      END catch
  END; 