
CREATE  PROCEDURE dbo.Usp_load_dimreseller
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          MERGE INTO adventureworksdw_ammar.dbo.dimreseller AS tgt
          using (SELECT *
                 FROM   (SELECT c.accountnumber
                                        AS ResellerAlternateKey,
                                dg.geographykey,
                                pp.phonenumber
                                        AS Phone,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:BusinessType)[1]', 'nvarchar(20)') AS BusinessType,
s.NAME
                AS ResellerName,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:NumberEmployees)[1]', 'int')       AS NumberEmployees,
Cast(NULL AS NVARCHAR(20))
                AS OrderFrequency,
Cast(NULL AS NVARCHAR(20))
                AS OrderMonth,
Cast(NULL AS INT)
                AS MinPaymentType,
Cast(NULL AS MONEY)
                AS MinPaymentAmount,
Year(oh.firstorder)
                AS FirstOrderYear,
Year(oh.lastorder)
                AS LastOrderYear,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:Specialty)[1]', 'nvarchar(50)')    AS ProductLine,
addr.addressline1,
addr.addressline2,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:AnnualSales)[1]', 'money')         AS AnnualSales,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:BankName)[1]', 'nvarchar(50)')     AS BankName,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:AnnualRevenue)[1]', 'money')       AS AnnualRevenue,
s.demographics.value('declare namespace ns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; (/ns:StoreSurvey/ns:YearOpened)[1]', 'int')            AS YearOpened,
Row_number()
  OVER(
    partition BY c.accountnumber
    ORDER BY addr.addressid DESC)
                AS RowNum
 FROM   AdventureWorks.sales.customer c
        INNER JOIN AdventureWorks.Sales.Store s
                ON c.storeid = s.businessentityid
        LEFT JOIN AdventureWorks.person.personphone pp
               ON s.businessentityid = pp.businessentityid
        LEFT JOIN AdventureWorks.person.businessentityaddress bea
               ON s.businessentityid = bea.businessentityid
        LEFT JOIN AdventureWorks.person.address addr
               ON bea.addressid = addr.addressid
        LEFT JOIN adventureworksdw_ammar.dbo.dimgeography dg
               ON addr.postalcode = dg.postalcode
                  AND addr.city = dg.city
        LEFT JOIN (SELECT customerid,
                          Min(orderdate) AS FirstOrder,
                          Max(orderdate) AS LastOrder
                   FROM   AdventureWorks.sales.salesorderheader
                   GROUP  BY customerid) oh
               ON c.customerid = oh.customerid
 WHERE  c.storeid IS NOT NULL) AS InnerSrc
 WHERE  rownum = 1
) AS src
ON ( tgt.reselleralternatekey = src.reselleralternatekey )
WHEN matched THEN
  UPDATE SET tgt.geographykey = src.geographykey,
             tgt.phone = src.phone,
             tgt.businesstype = src.businesstype,
             tgt.resellername = src.resellername,
             tgt.numberemployees = src.numberemployees,
             tgt.orderfrequency = src.orderfrequency,
             tgt.ordermonth = src.ordermonth,
             tgt.firstorderyear = src.firstorderyear,
             tgt.lastorderyear = src.lastorderyear,
             tgt.productline = src.productline,
             tgt.addressline1 = src.addressline1,
             tgt.addressline2 = src.addressline2,
             tgt.annualsales = src.annualsales,
             tgt.bankname = src.bankname,
             tgt.minpaymenttype = src.minpaymenttype,
             tgt.minpaymentamount = src.minpaymentamount,
             tgt.annualrevenue = src.annualrevenue,
             tgt.yearopened = src.yearopened
WHEN NOT matched THEN
  INSERT ( geographykey,
           reselleralternatekey,
           phone,
           businesstype,
           resellername,
           numberemployees,
           orderfrequency,
           ordermonth,
           firstorderyear,
           lastorderyear,
           productline,
           addressline1,
           addressline2,
           annualsales,
           bankname,
           minpaymenttype,
           minpaymentamount,
           annualrevenue,
           yearopened )
  VALUES ( src.geographykey,
           src.reselleralternatekey,
           src.phone,
           src.businesstype,
           src.resellername,
           src.numberemployees,
           src.orderfrequency,
           src.ordermonth,
           src.firstorderyear,
           src.lastorderyear,
           src.productline,
           src.addressline1,
           src.addressline2,
           src.annualsales,
           src.bankname,
           src.minpaymenttype,
           src.minpaymentamount,
           src.annualrevenue,
           src.yearopened );

    COMMIT TRANSACTION;

    PRINT 'dimreseller loaded successfully.';
END try

    BEGIN catch
        IF @@TRANCOUNT > 0
          ROLLBACK TRANSACTION;

        PRINT 'Error occurred in usp_Load_dimreseller: '
              + Error_message();
    END catch
END;