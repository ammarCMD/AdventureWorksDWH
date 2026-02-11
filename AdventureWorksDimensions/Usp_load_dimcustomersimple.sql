
CREATE PROCEDURE dbo.Usp_load_dimcustomersimple
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimcustomersimple AS tgt
          using(SELECT dc.customerkey                   AS custid,
                       dc.firstname + ' ' + dc.lastname AS custname,
                       dg.city                          AS city
                FROM   adventureworksdw_ammar.dbo.dimcustomer dc
                       LEFT JOIN adventureworksdw_ammar.dbo.dimgeography dg
                              ON dc.geographykey = dg.geographykey) AS src
          ON tgt.customerid = src.custid
          WHEN matched THEN
            UPDATE SET tgt.customerid = src.custid,
                       tgt.customername = src.custname,
                       tgt.city = src.city,
                       tgt.loaddate = Getdate()
          WHEN NOT matched THEN
            INSERT (customerid,
                    customername,
                    city,
                    loaddate)
            VALUES (src.custid,
                    src.custname,
                    src.city,
                    Getdate());

          COMMIT TRANSACTION;

          PRINT 'DimCustomerSimple loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimCustomerSimple: '
                + Error_message();
      END catch
  END; 