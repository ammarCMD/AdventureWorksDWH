CREATE PROCEDURE dbo.Usp_load_dimemployee
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimemployee AS tgt
          using(SELECT e.businessentityid    AS pek,
                       e.nationalidnumber    AS enid,
                       p.firstname           AS fn,
                       p.middlename          AS mn,
                       p.lastname            AS lsn,
                       p.namestyle           AS ns,
                       p.title               AS title,
                       e.hiredate            AS hd,
                       e.birthdate           AS bd,
                       e.loginid             AS lid,
                       ea.emailaddress       AS ea,
                       pp.phonenumber        AS ph,
                       e.maritalstatus       AS ms,
                       e.salariedflag        AS sf,
                       e.gender              AS gndr,
                       eph.payfrequency      AS pf,
                       eph.rate              AS br,
                       e.vacationhours       AS vh,
                       e.sickleavehours      AS slh,
                       e.currentflag         AS cf,
                       CASE
                         WHEN sp.businessentityid IS NOT NULL THEN 1
                         ELSE 0
                       END                   AS SPF,
                       d.NAME                AS dn,
                       edh.startdate         AS std,
                       edh.enddate           AS edd,
                       CASE
                         WHEN edh.enddate IS NOT NULL THEN 'inactive'
                         ELSE 'active'
                       END                   AS St,
                       dst.salesterritorykey AS stk
                FROM   adventureworks.humanresources.employee e
                       INNER JOIN adventureworks.person.person p
                               ON e.businessentityid = p.businessentityid
                       INNER JOIN adventureworks.person.personphone pp
                               ON e.businessentityid = pp.businessentityid
                       INNER JOIN adventureworks.person.emailaddress ea
                               ON e.businessentityid = ea.businessentityid
                       JOIN adventureworks.humanresources.employeepayhistory eph
                         ON e.businessentityid = eph.businessentityid
                            AND eph.ratechangedate = (SELECT Max(ratechangedate)
                                                      FROM
                                adventureworks.humanresources.employeepayhistory
                                                      WHERE
                                businessentityid = e.businessentityid)
                       LEFT JOIN adventureworks.sales.salesperson sp
                              ON e.businessentityid = sp.businessentityid
                       INNER JOIN
                       adventureworks.humanresources.employeedepartmenthistory
                       edh
                               ON e.businessentityid =
                                  (SELECT edh.businessentityid
                                   WHERE  edh.enddate IS NULL)
                       INNER JOIN adventureworks.humanresources.department d
                               ON edh.departmentid = d.departmentid
                       LEFT JOIN adventureworksdw_ammar.dbo.dimsalesterritory
                                 dst
                              ON sp.territoryid =
               dst.salesterritoryalternatekey)
               AS
               src
          ON tgt.parentemployeekey = src.pek
          WHEN matched THEN
            UPDATE SET tgt.parentemployeekey = src.pek,
                       tgt.employeenationalidalternatekey = src.enid,
                       tgt.parentemployeenationalidalternatekey = 'NA',
                       tgt.salesterritorykey = src.stk,
                       tgt.firstname = src.fn,
                       tgt.lastname = src.lsn,
                       tgt.middlename = src.mn,
                       tgt.namestyle = src.ns,
                       tgt.title = src.title,
                       tgt.hiredate = src.hd,
                       tgt.birthdate = src.bd,
                       tgt.loginid = src.lid,
                       tgt.emailaddress = src.ea,
                       tgt.phone = src.ph,
                       tgt.maritalstatus = src.ms,
                       tgt.emergencycontactname = 'Unknown',
                       tgt.emergencycontactphone = 'Unknown',
                       tgt.salariedflag = src.sf,
                       tgt.gender = src.gndr,
                       tgt.payfrequency = src.pf,
                       tgt.baserate = src.br,
                       tgt.vacationhours = src.vh,
                       tgt.sickleavehours = src.slh,
                       tgt.currentflag = src.cf,
                       tgt.salespersonflag = src.spf,
                       tgt.departmentname = src.dn,
                       tgt.startdate = src.std,
                       tgt.enddate = src.edd,
                       tgt.status = src.st,
                       tgt.employeephoto = Cast ('NA' AS VARBINARY)
          WHEN NOT matched THEN
            INSERT (parentemployeekey,
                    employeenationalidalternatekey,
                    salesterritorykey,
                    firstname,
                    lastname,
                    middlename,
                    namestyle,
                    title,
                    hiredate,
                    birthdate,
                    loginid,
                    emailaddress,
                    phone,
                    maritalstatus,
                    emergencycontactname,
                    emergencycontactphone,
                    salariedflag,
                    gender,
                    payfrequency,
                    baserate,
                    vacationhours,
                    sickleavehours,
                    currentflag,
                    salespersonflag,
                    departmentname,
                    startdate,
                    enddate,
                    status,
                    employeephoto)
            VALUES (src.pek,
                    src.enid,
                    src.stk,
                    src.fn,
                    src.lsn,
                    src.mn,
                    src.ns,
                    src.title,
                    src.hd,
                    src.bd,
                    src.lid,
                    src.ea,
                    src.ph,
                    src.ms,
                    'unknown',
                    'unknown',
                    src.sf,
                    src.gndr,
                    src.pf,
                    src.br,
                    src.vh,
                    src.slh,
                    src.cf,
                    src.spf,
                    src.dn,
                    src.std,
                    src.edd,
                    src.st,
                    Cast ('NA' AS VARBINARY));

          COMMIT TRANSACTION;

          PRINT 'dimemployee loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_dimemployee: '
                + Error_message();
      END catch
  END; 