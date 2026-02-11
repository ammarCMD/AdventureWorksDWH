CREATE PROCEDURE dbo.usp_Master_ETL_Load
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentStep NVARCHAR(255) = 'Starting ETL';
    DECLARE @Start DATETIME;
    DECLARE @ProcessID int; -- Groups all entries for this specific run

    BEGIN TRY
        PRINT '--- Starting Full ETL Process ---';

        /* ==========================================================
           1. LOADING DIMENSIONS
           ==========================================================
        */

        -- DimCurrency
        SET @CurrentStep = 'Usp_load_dimcurrency'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimcurrency;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimCurrency', 'Sales.Currency', @Start, GETDATE(), 'Success');

        -- DimCustomer
        SET @CurrentStep = 'usp_load_dimcustomer'; SET @Start = GETDATE();
        EXEC dbo.usp_load_dimcustomer;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimCustomer', 'Sales.Customer', @Start, GETDATE(), 'Success');
        
        -- DimCustomerSimple
        SET @CurrentStep = 'Usp_load_dimcustomersimple'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimcustomersimple;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimCustomerSimple', 'Sales.Customer', @Start, GETDATE(), 'Success');

        -- DimDate
        SET @CurrentStep = 'Usp_load_dimdate'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimdate;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimDate', 'Calculated Range', @Start, GETDATE(), 'Success');

        -- DimDepartmentGroup
        SET @CurrentStep = 'Usp_load_dimdepartmentgroup'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimdepartmentgroup;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimDepartmentGroup', 'HumanResources.Department', @Start, GETDATE(), 'Success');

        -- DimEmployee
        SET @CurrentStep = 'Usp_load_dimemployee'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimemployee;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimEmployee', 'HumanResources.Employee', @Start, GETDATE(), 'Success');

        -- DimGeography
        SET @CurrentStep = 'Usp_load_dimgeography'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimgeography;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimGeography', 'Person.Address', @Start, GETDATE(), 'Success');

        -- DimOrganization
        SET @CurrentStep = 'Usp_load_dimorganization'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimorganization;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimOrganization', 'HumanResources.Organization', @Start, GETDATE(), 'Success');

        -- DimProduct
        SET @CurrentStep = 'Usp_load_dimproduct'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimproduct;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimProduct', 'Production.Product', @Start, GETDATE(), 'Success');

        -- DimProductCategory
        SET @CurrentStep = 'Usp_load_dimproductcategory'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimproductcategory;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimProductCategory', 'Production.ProductCategory', @Start, GETDATE(), 'Success');

        -- DimProductSubcategory
        SET @CurrentStep = 'Usp_load_dimproductsubcategory'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimproductsubcategory;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimProductSubcategory', 'Production.ProductSubcategory', @Start, GETDATE(), 'Success');

        -- DimPromotion
        SET @CurrentStep = 'Usp_load_dimpromotion'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimpromotion;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimPromotion', 'Sales.SpecialOffer', @Start, GETDATE(), 'Success');

        -- DimReseller
        SET @CurrentStep = 'Usp_load_dimreseller'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimreseller;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimReseller', 'Sales.Store', @Start, GETDATE(), 'Success');

        -- DimSalesReason
        SET @CurrentStep = 'Usp_load_dimsalesreason'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimsalesreason;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimSalesReason', 'Sales.SalesReason', @Start, GETDATE(), 'Success');

        -- DimSalesTerritory
        SET @CurrentStep = 'Usp_load_dimsalesterritory'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimsalesterritory;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimSalesTerritory', 'Sales.SalesTerritory', @Start, GETDATE(), 'Success');

        -- DimScenario
        SET @CurrentStep = 'Usp_load_dimscenario'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_dimscenario;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'DimScenario', 'Finance.Scenario', @Start, GETDATE(), 'Success');

        /* ==========================================================
           2. LOADING FACTS
           ==========================================================
        */

        -- FactAdditionalInternationalProductDescription
        SET @CurrentStep = 'Usp_load_factadditionalinternationalproductdescription'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factadditionalinternationalproductdescription;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactAdditionalInternationalProductDescription', 'Production.ProductDescription', @Start, GETDATE(), 'Success');

        -- FactCallCenter
        SET @CurrentStep = 'Usp_load_factcallcenter'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factcallcenter;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactCallCenter', 'Source.CallCenter', @Start, GETDATE(), 'Success');

        -- FactCurrencyRate
        SET @CurrentStep = 'Usp_load_factcurrencyrate'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factcurrencyrate;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactCurrencyRate', 'Sales.CurrencyRate', @Start, GETDATE(), 'Success');

        -- FactFinance
        SET @CurrentStep = 'Usp_load_factfinance'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factfinance;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactFinance', 'Accounting.Finance', @Start, GETDATE(), 'Success');

        -- FactInternetSales
        SET @CurrentStep = 'Usp_load_factinternetsales'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factinternetsales;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactInternetSales', 'Sales.SalesOrderHeader', @Start, GETDATE(), 'Success');

        -- FactInternetSalesReason
        SET @CurrentStep = 'Usp_load_factinternetsalesreason'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factinternetsalesreason;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactInternetSalesReason', 'Sales.SalesOrderReason', @Start, GETDATE(), 'Success');

        -- FactProductInventory
        SET @CurrentStep = 'Usp_load_factproductinventory'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factproductinventory;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactProductInventory', 'Production.ProductInventory', @Start, GETDATE(), 'Success');

        -- FactResellerSales
        SET @CurrentStep = 'Usp_load_factresellersales'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factresellersales;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactResellerSales', 'Sales.SalesOrderHeader', @Start, GETDATE(), 'Success');

        -- FactSalesQuota
        SET @CurrentStep = 'Usp_load_factsalesquota'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factsalesquota;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactSalesQuota', 'Sales.SalesPersonQuota', @Start, GETDATE(), 'Success');

        -- FactSurveyResponse
        SET @CurrentStep = 'Usp_load_factsurveyresponse'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_factsurveyresponse;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'FactSurveyResponse', 'Sales.SurveyResponse', @Start, GETDATE(), 'Success');

        -- NewFactCurrencyRate
        SET @CurrentStep = 'Usp_load_newfactcurrencyrate'; SET @Start = GETDATE();
        EXEC dbo.Usp_load_newfactcurrencyrate;
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, TargetTable, SourceTable, StartTime, EndTime, Status)
        VALUES (@ProcessID, @CurrentStep, 'NewFactCurrencyRate', 'Sales.CurrencyRate', @Start, GETDATE(), 'Success');

        PRINT '--- All Procedures Completed Successfully ---';

    END TRY
    BEGIN CATCH
        -- Log the Failure
        INSERT INTO dbo.ETL_Log (ProcessID, ProcessName, StartTime, EndTime, Status, ErrorMessage)
        VALUES (@ProcessID, @CurrentStep, @Start, GETDATE(), 'Failed', ERROR_MESSAGE());

        PRINT '##########################################################';
        PRINT 'ETL FAILURE DETECTED AT: ' + @CurrentStep;
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '##########################################################';
        
        THROW; 
    END CATCH
END;

