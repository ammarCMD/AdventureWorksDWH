CREATE PROCEDURE dbo.Usp_load_factsalesquota
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factsalesquota AS tgt
          using(SELECT de.employeekey           AS ek,
                       Isnull(sp.salesquota, 0) AS saq,
                       dd.datekey               AS dk,
                       dd.calendaryear          AS cy,
                       dd.calendarquarter       AS cq,
                       dd.fulldatealternatekey  AS d
                FROM   adventureworksdw_ammar.dbo.dimemployee de
                       INNER JOIN adventureworksdw_ammar.dbo.dimdate dd
                               ON de.hiredate = dd.fulldatealternatekey
                       LEFT JOIN adventureworks.sales.salesperson sp
                              ON de.employeenationalidalternatekey =
                                 sp.businessentityid)
               AS src
          ON tgt.employeekey = src.ek
          WHEN matched THEN
            UPDATE SET tgt.employeekey = src.ek,
                       tgt.datekey = src.dk,
                       tgt.calendaryear = src.cy,
                       tgt.calendarquarter = src.cq,
                       tgt.salesamountquota = src. saq,
                       tgt.[date] = src.d
          WHEN NOT matched THEN
            INSERT (employeekey,
                    datekey,
                    calendaryear,
                    calendarquarter,
                    salesamountquota,
                    [date])
            VALUES (ek,
                    dk,
                    cy,
                    cq,
                    saq,
                    d);

          COMMIT TRANSACTION;

          PRINT 'FactSalesQuota loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactSalesQuota: '
                + Error_message();
      END catch
  END; 