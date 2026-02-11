CREATE PROCEDURE dbo.Usp_load_dimproduct
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimproduct AS tgt
          using(SELECT p.productid                         AS pak,
                       dpsc.productsubcategoryalternatekey AS psck,
                       p.weightunitmeasurecode             AS wumc,
                       p.sizeunitmeasurecode               AS sumc,
                       p.NAME                              AS epn,
                       p.standardcost                      AS sc,
                       p.finishedgoodsflag                 AS fgf,
                       Isnull(p.color, 'NA')               AS col,
                       p.safetystocklevel                  AS ssl,
                       p.reorderpoint                      AS rp,
                       p.listprice                         AS lp,
                       p.[size]                            AS siz,
                       p.weight                            AS wt,
                       p.daystomanufacture                 AS dtm,
                       p.productline                       AS pl,
                       p.class                             AS class,
                       p.[style]                           AS st,
                       ph.largephoto                       AS lp_binary,
                       p.sellstartdate                     AS sd,
                       p.sellenddate                       AS ed
                FROM   adventureworks.production.product p
                       LEFT JOIN adventureworks.production.productproductphoto
                                 ppp
                              ON p.productid = ppp.productid
                       LEFT JOIN adventureworks.production.productphoto ph
                              ON ppp.productphotoid = ph.productphotoid
                       LEFT JOIN
                       adventureworksdw_ammar.dbo.dimproductsubcategory
                       dpsc
                              ON p.productsubcategoryid =
                                 dpsc.productsubcategoryalternatekey) AS src
          ON tgt.productalternatekey = src.pak
          WHEN matched THEN
            UPDATE SET tgt.productalternatekey = src.pak,
                       tgt.productsubcategorykey = src.psck,
                       tgt.weightunitmeasurecode = src.wumc,
                       tgt.sizeunitmeasurecode = src.sumc,
                       tgt.englishproductname = src.epn,
                       tgt.spanishproductname = 'NA',
                       tgt.frenchproductname = 'NA',
                       tgt.standardcost = src.sc,
                       tgt.finishedgoodsflag = src.fgf,
                       tgt.color = src.col,
                       tgt.safetystocklevel = src.ssl,
                       tgt.reorderpoint = src.rp,
                       tgt.listprice = src.lp,
                       tgt.[size] = src.siz,
                       tgt.sizerange = 'NA',
                       tgt.weight = src.wt,
                       tgt.daystomanufacture = src.dtm,
                       tgt.productline = src.pl,
                       tgt.dealerprice = NULL,
                       tgt.class = src.class,
                       tgt.[style] = src.st,
                       tgt.modelname = 'NA',
                       tgt.largephoto = src.lp_binary,
                       tgt.englishdescription = 'NA',
                       tgt.frenchdescription = 'NA',
                       tgt.chinesedescription = 'NA',
                       tgt.arabicdescription = 'NA',
                       tgt.hebrewdescription = 'NA',
                       tgt.thaidescription = 'NA',
                       tgt.germandescription = 'NA',
                       tgt.japanesedescription = 'NA',
                       tgt.turkishdescription = 'NA',
                       tgt.startdate = src.sd,
                       tgt.enddate = src.ed,
                       tgt.status = NULL
          WHEN NOT matched THEN
            INSERT ( productalternatekey,
                     productsubcategorykey,
                     weightunitmeasurecode,
                     sizeunitmeasurecode,
                     englishproductname,
                     spanishproductname,
                     frenchproductname,
                     standardcost,
                     finishedgoodsflag,
                     color,
                     safetystocklevel,
                     reorderpoint,
                     listprice,
                     [size],
                     sizerange,
                     weight,
                     daystomanufacture,
                     productline,
                     dealerprice,
                     class,
                     [style],
                     modelname,
                     largephoto,
                     englishdescription,
                     frenchdescription,
                     chinesedescription,
                     arabicdescription,
                     hebrewdescription,
                     thaidescription,
                     germandescription,
                     japanesedescription,
                     turkishdescription,
                     startdate,
                     enddate,
                     status )
            VALUES ( src.pak,
                     src.psck,
                     src.wumc,
                     src.sumc,
                     src.epn,
                     'NA',
                     'NA',
                     src.sc,
                     src.fgf,
                     src.col,
                     src.ssl,
                     src.rp,
                     src.lp,
                     src.siz,
                     'NA',
                     src.wt,
                     src.dtm,
                     src.pl,
                     NULL,
                     src.class,
                     src.st,
                     'NA',
                     src.lp_binary,
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     'NA',
                     src.sd,
                     src.ed,
                     'NA' );

          COMMIT TRANSACTION;

          PRINT 'DimProduct loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimProduct: '
                + Error_message();
      END catch
  END; 