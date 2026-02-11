CREATE PROCEDURE dbo.Usp_load_dimorganization
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimorganization AS tgt
          using (SELECT d.departmentid AS pok,
                        d.NAME         AS orgn,
                        dc.currencykey AS ck
                 FROM   adventureworks.humanresources.department d
                        LEFT JOIN adventureworksdw_ammar.dbo.dimcurrency dc
                               ON dc.currencyalternatekey = 'USD') AS src
          ON tgt.parentorganizationkey = src.pok
          WHEN matched THEN
            UPDATE SET tgt.parentorganizationkey = src.pok,
                       tgt.percentageofownership = 100 / 17,
                       tgt.organizationname = src.orgn,
                       tgt.currencykey = src.ck
          WHEN NOT matched THEN
            INSERT (parentorganizationkey,
                    percentageofownership,
                    organizationname,
                    currencykey)
            VALUES (src.pok,
                    100 / 17,
                    src.orgn,
                    src.ck);

          COMMIT TRANSACTION;

          PRINT 'DimOrganization loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimOrganization: '
                + Error_message();
      END catch
  END;