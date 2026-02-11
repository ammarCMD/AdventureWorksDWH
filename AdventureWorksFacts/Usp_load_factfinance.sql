CREATE PROCEDURE dbo.Usp_load_factfinance
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.factfinance AS tgt
          using (SELECT d.datekey,
                        d.fulldatealternatekey                             AS
                        [Date],
                        org.organizationkey,
                        dep.departmentgroupkey,
                        scn.scenariokey,
                        acc.accountkey,
                        -- Generates a random financial amount
                        Cast(( Rand(Checksum(Newid())) * 10000 ) AS MONEY) AS
                        Amount
                 FROM   adventureworksdw_ammar.dbo.dimdate d
                        CROSS JOIN adventureworksdw_ammar.dbo.dimorganization
                                   org
                        CROSS JOIN adventureworksdw_ammar.dbo.dimdepartmentgroup
                                   dep
                        CROSS JOIN adventureworksdw_ammar.dbo.dimscenario scn
                        CROSS JOIN adventureworksdw_ammar.dbo.dimaccount acc
                 -- Filter to keep the data volume reasonable (1st of every month in 2025)
                 WHERE  d.daynumberofmonth = 1
                        AND d.fulldatealternatekey BETWEEN
                            '2025-01-01' AND '2025-12-31'
                --AND acc.AccountType = 'Expenditure' 
                ) AS src
          ON ( tgt.datekey = src.datekey
               AND tgt.organizationkey = src.organizationkey
               AND tgt.departmentgroupkey = src.departmentgroupkey
               AND tgt.scenariokey = src.scenariokey
               AND tgt.accountkey = src.accountkey )
          WHEN matched THEN
            UPDATE SET tgt.amount = src.amount,
                       tgt.[date] = src.[date]
          WHEN NOT matched THEN
            INSERT (datekey,
                    organizationkey,
                    departmentgroupkey,
                    scenariokey,
                    accountkey,
                    amount,
                    [date])
            VALUES (src.datekey,
                    src.organizationkey,
                    src.departmentgroupkey,
                    src.scenariokey,
                    src.accountkey,
                    src.amount,
                    src.[date]);

          COMMIT TRANSACTION;

          PRINT 'FactFinance loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_FactFinance: '
                + Error_message();
      END catch
  END; 