CREATE PROCEDURE dbo.usp_load_dimcustomer
AS
  BEGIN
    SET nocount ON;
    BEGIN try
      BEGIN TRANSACTION;
      WITH uniquecustomersource AS
      (
                 SELECT     c.customerid AS custaltkey,
                            p.title,
                            p.firstname,
                            p.lastname,
                            p.middlename,
                            Cast(p.namestyle AS   BIT)  AS namestyle,
                            Cast(vpd.birthdate AS DATE) AS birthdate,
                            vpd.maritalstatus,
                            p.suffix,
                            vpd.gender,
                            ea.emailaddress,
                            addr.addressline1 AS                   line1,
                            addr.addressline2 AS                   line2,
                            pp.phonenumber    AS                   phone,
                            try_cast(vpd.yearlyincome as money) AS yearlyincome,
                            vpd.totalchildren,
                            vpd.numberchildrenathome AS noofchildathome,
                            vpd.education            AS engedu,
                            vpd.occupation           AS engocu,
                            vpd.homeownerflag        AS houseflag,
                            vpd.numbercarsowned      AS carsowned,
                            vpd.datefirstpurchase    AS firstdate,
                            dg.geographykey,
                            row_number() OVER ( partition BY c.customerid ORDER BY bea.addresstypeid ASC, ea.emailaddressid ASC, pp.phonenumbertypeid ASC ) AS rownum
                 FROM       AdventureWorks.Person.Person p
                 INNER JOIN AdventureWorks.Sales.Customer c
                 ON         p.businessentityid = c.personid
                 LEFT JOIN  AdventureWorks.Person.personphone pp
                 ON         p.businessentityid = pp.businessentityid
                 LEFT JOIN  AdventureWorks.Person.emailaddress ea
                 ON         p.businessentityid = ea.businessentityid
                 LEFT JOIN  AdventureWorks.Person.businessentityaddress bea
                 ON         p.businessentityid = bea.businessentityid
                 LEFT JOIN  AdventureWorks.Person.address addr
                 ON         bea.addressid = addr.addressid
                 LEFT JOIN  adventureworks.sales.vpersondemographics vpd
                 ON         p.businessentityid = vpd.businessentityid
                 LEFT JOIN  adventureworksdw_ammar.dbo.dimgeography dg
                 ON         dg.postalcode = addr.postalcode
                 AND        dg.city = addr.city )
      MERGE
      INTO         adventureworksdw_ammar.dbo.dimcustomer AS tgt
      using        (
                          SELECT *
                          FROM   uniquecustomersource
                          WHERE  rownum = 1) AS src
      ON (
                                tgt.customeralternatekey = Cast(src.custaltkey AS NVARCHAR(15)))
      WHEN matched THEN
      UPDATE
      SET              tgt.geographykey = src.geographykey,
                       tgt.title = src.title,
                       tgt.firstname = src.firstname,
                       tgt.middlename = src.middlename,
                       tgt.lastname = src.lastname,
                       tgt.namestyle = src.namestyle,
                       tgt.birthdate = src.birthdate,
                       tgt.maritalstatus = src.maritalstatus,
                       tgt.suffix = src.suffix,
                       tgt.gender = src.gender,
                       tgt.emailaddress = src.emailaddress,
                       tgt.yearlyincome = src.yearlyincome,
                       tgt.totalchildren = src.totalchildren,
                       tgt.numberchildrenathome = src.noofchildathome,
                       tgt.englisheducation = src.engedu,
                       tgt.englishoccupation = src.engocu,
                       tgt.houseownerflag = src.houseflag,
                       tgt.numbercarsowned = src.carsowned,
                       tgt.addressline1 = src.line1,
                       tgt.addressline2 = src.line2,
                       tgt.phone = src.phone,
                       tgt.datefirstpurchase = src.firstdate
      WHEN NOT matched THEN
      INSERT
             (
                    customeralternatekey,
                    geographykey,
                    title,
                    firstname,
                    middlename,
                    lastname,
                    namestyle,
                    birthdate,
                    maritalstatus,
                    suffix,
                    gender,
                    emailaddress,
                    yearlyincome,
                    totalchildren,
                    numberchildrenathome,
                    englisheducation,
                    spanisheducation,
                    frencheducation,
                    englishoccupation,
                    spanishoccupation,
                    frenchoccupation,
                    houseownerflag,
                    numbercarsowned,
                    addressline1,
                    addressline2,
                    phone,
                    datefirstpurchase,
                    commutedistance
             )
             VALUES
             (
                    Cast(src.custaltkey AS NVARCHAR(15)),
                    src.geographykey,
                    src.title,
                    src.firstname,
                    src.middlename,
                    src.lastname,
                    src.namestyle,
                    src.birthdate,
                    src.maritalstatus,
                    src.suffix,
                    src.gender,
                    src.emailaddress,
                    src.yearlyincome,
                    src.totalchildren,
                    src.noofchildathome,
                    src.engedu,
                    'N/A',
                    'N/A',
                    src.engocu,
                    'N/A',
                    'N/A',
                    src.houseflag,
                    src.carsowned,
                    src.line1,
                    src.line2,
                    src.phone,
                    src.firstdate,
                    'N/A'
             );
      
      COMMIT TRANSACTION;
      PRINT 'DimCustomer loaded successfully.';
    END try
    BEGIN catch
      IF @@TRANCOUNT > 0
      ROLLBACK TRANSACTION;
      PRINT 'Error occurred in usp_Load_DimCustomer: ' + Error_message();
    END catch
  END
