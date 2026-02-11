CREATE PROCEDURE dbo.Usp_load_factresellersales
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          WITH resellersalessource
               AS (SELECT d1.datekey
                          AS
                             OrderDateKey,
                          d2.datekey
                             AS DueDateKey,
                          d3.datekey
                             AS ShipDateKey,
                          Isnull(r.resellerkey, 0)
                             AS ResellerKey,
                          Isnull(e.employeekey, 0)
                             AS EmployeeKey,
                          Isnull(p.productkey, 0)
                             AS ProductKey,
                          Isnull(st.salesterritorykey, 0)
                             AS SalesTerritoryKey,
                          Isnull(curr.currencykey, 100)
                             AS CurrencyKey,
                          soh.salesordernumber,
                          sod.salesorderdetailid
                             AS SalesOrderLineNumber,
                          soh.revisionnumber,
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
                          sod.carriertrackingnumber,
                          soh.purchaseordernumber
                             AS CustomerPONumber,
                          Cast(soh.orderdate AS DATE)
                             AS OrderDate,
                          Cast(soh.duedate AS DATE)
                             AS DueDate,
                          Cast(soh.shipdate AS DATE)
                             AS ShipDate
                   FROM   adventureworks.sales.salesorderheader soh
                          INNER JOIN adventureworks.sales.salesorderdetail sod
                                  ON soh.salesorderid = sod.salesorderid
                          LEFT JOIN adventureworksdw_ammar.dbo.dimproduct p
                                 ON sod.productid = p.productalternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimreseller r
                                 ON Cast(soh.customerid AS NVARCHAR(50)) =
                                    r.reselleralternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimemployee e
                                 ON soh.salespersonid = e.parentemployeekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimpromotion pr
                                 ON sod.specialofferid =
                                    pr.promotionalternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimdate d1
                                 ON Cast(soh.orderdate AS DATE) =
                                    d1.fulldatealternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimdate d2
                                 ON Cast(soh.duedate AS DATE) =
                                    d2.fulldatealternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimdate d3
                                 ON Cast(soh.shipdate AS DATE) =
                                    d3.fulldatealternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimsalesterritory
                                    st
                                 ON soh.territoryid =
                                    st.salesterritoryalternatekey
                          LEFT JOIN adventureworksdw_ammar.dbo.dimcurrency curr
                                 ON soh.currencyrateid = curr.currencykey
                   WHERE  soh.onlineorderflag = 0)
          MERGE INTO adventureworksdw_ammar.dbo.factresellersales AS tgt
          using resellersalessource AS src
          ON ( tgt.salesordernumber = src.salesordernumber
               AND tgt.salesorderlinenumber = src.salesorderlinenumber )
          WHEN matched THEN
            UPDATE SET tgt.salesamount = src.salesamount,
                       tgt.orderquantity = src.orderquantity,
                       tgt.taxamt = src.taxamt
          WHEN NOT matched THEN
            INSERT (productkey,
                    orderdatekey,
                    duedatekey,
                    shipdatekey,
                    resellerkey,
                    employeekey,
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
                    src.resellerkey,
                    src.employeekey,
                    Abs(Checksum(Newid())) % 16 + 1,
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

          PRINT 'FactResellerSales loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactResellerSales: '
                + Error_message();
      END catch
  END; 