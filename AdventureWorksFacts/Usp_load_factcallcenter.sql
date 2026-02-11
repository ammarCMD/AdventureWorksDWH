CREATE PROCEDURE dbo.Usp_load_factcallcenter
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factcallcenter AS tgt
          using (
                -- Simulating the source data
                SELECT d.datekey,
                       d.fulldatealternatekey                               AS
                       [Date],
                       s.shift,
                       -- Logic to differentiate weekday vs holiday (weekend) pay
                       CASE
                         WHEN d.daynumberofweek IN ( 1, 7 ) THEN 'holiday'
                         ELSE 'weekday'
                       END                                                  AS
                       WageType,
                       5                                                    AS
                       LevelOneOperators
                       ,
                       2                                                    AS
                       LevelTwoOperators,
                       7                                                    AS
                       TotalOperators,
                       Cast(Rand(Checksum(Newid())) * 100 + 50 AS INT)      AS
                       Calls,
                       Cast(Rand(Checksum(Newid())) * 20 AS INT)            AS
                       AutomaticResponses,
                       Cast(Rand(Checksum(Newid())) * 15 AS INT)            AS
                       Orders,
                       Cast(Rand(Checksum(Newid())) * 5 AS INT)             AS
                       IssuesRaised,
                       Cast(Rand(Checksum(Newid())) * 60 + 120 AS SMALLINT) AS
                       AverageTimePerIssue,
                       -- Using ServiceGrade as requested
                       0.85 + ( Rand(Checksum(Newid())) * 0.1 )             AS
                       ServiceGrade
                 FROM   adventureworksdw_ammar.dbo.dimdate d
                        CROSS JOIN (SELECT 'AM' AS Shift
                                    UNION ALL
                                    SELECT 'PM' AS Shift
                                    UNION ALL
                                    SELECT 'Midnight' AS Shift) s
                 WHERE  d.fulldatealternatekey BETWEEN
                        '2023-01-01' AND '2023-12-31')
                AS
                src
          ON ( tgt.datekey = src.datekey
               AND tgt.shift = src.shift )
          WHEN matched THEN
            UPDATE SET tgt.[date] = src.[date],
                       tgt.wagetype = src.wagetype,
                       tgt.calls = src.calls,
                       tgt.orders = src.orders,
                       tgt.servicegrade = src.servicegrade
          WHEN NOT matched THEN
            INSERT ( datekey,
                     [date],
                     wagetype,
                     shift,
                     leveloneoperators,
                     leveltwooperators,
                     totaloperators,
                     calls,
                     automaticresponses,
                     orders,
                     issuesraised,
                     averagetimeperissue,
                     servicegrade )
            VALUES ( src.datekey,
                     src.[date],
                     src.wagetype,
                     src.shift,
                     src.leveloneoperators,
                     src.leveltwooperators,
                     src.totaloperators,
                     src.calls,
                     src.automaticresponses,
                     src.orders,
                     src.issuesraised,
                     src.averagetimeperissue,
                     src.servicegrade );

          COMMIT TRANSACTION;

          PRINT 'FactCallCenter loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactCallCenter: '
                + Error_message();
      END catch
  END; 