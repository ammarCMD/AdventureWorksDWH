CREATE PROCEDURE dbo.Usp_load_dimscenario
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimscenario AS tgt
          using ( VALUES (1,
                'Actual'),
                         (2,
                'Budget'),
                         (3,
                'Forecast') ) AS src (scenariokey, scenarioname)
          ON ( tgt.scenarioname = src.scenarioname )
          WHEN NOT matched THEN
            INSERT (scenarioname)
            VALUES (src.scenarioname);

          COMMIT TRANSACTION;

          PRINT 'DimScenario loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimScenario: '
                + Error_message();
      END catch
  END; 