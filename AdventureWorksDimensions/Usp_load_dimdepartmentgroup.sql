CREATE PROCEDURE dbo.Usp_load_dimdepartmentgroup
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimdepartmentgroup AS tgt
          using(SELECT d.departmentid AS deptid,
                       d.groupname    AS grpname
                FROM   adventureworks.humanresources.department d) AS src
          ON tgt.departmentgroupkey = src.deptid
          WHEN matched THEN
            UPDATE SET tgt.parentdepartmentgroupkey = src.deptid,
                       tgt.departmentgroupname = src.grpname
          WHEN NOT matched THEN
            INSERT (parentdepartmentgroupkey,
                    departmentgroupname)
            VALUES (src.deptid,
                    src.grpname);

          COMMIT TRANSACTION;

          PRINT 'DimDepartmentGroup loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimDepartmentGroup: '
                + Error_message();
      END catch
  END;