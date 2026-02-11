CREATE PROCEDURE dbo.usp_Load_DimGeography
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO AdventureWorksDW_ammar.dbo.DimGeography AS tgt
        USING (
            -- Added DISTINCT to ensure we only have one row per geography unit
            SELECT DISTINCT
                ad.City,
                sp.StateProvinceCode AS spc,
                cr.Name AS ctname,
                sp.Name AS spname,
                cr.CountryRegionCode AS ctrcode,
                ad.PostalCode AS pstcode,
                dst.SalesTerritoryKey AS stkey
            FROM AdventureWorks.Person.Address AS ad
            INNER JOIN AdventureWorks.Person.StateProvince sp 
                ON ad.StateProvinceID = sp.StateProvinceID
            INNER JOIN AdventureWorks.Person.CountryRegion cr 
                ON cr.CountryRegionCode = sp.CountryRegionCode
            INNER JOIN AdventureWorksDW_ammar.dbo.DimSalesTerritory dst 
                ON dst.SalesTerritoryKey = sp.TerritoryID
        ) AS src
        ON (tgt.City = src.City 
            AND tgt.PostalCode = src.pstcode 
            AND tgt.StateProvinceCode = src.spc)

        WHEN MATCHED THEN
            -- Since the keys match, usually you'd update names or territory keys
            UPDATE SET 
                tgt.EnglishCountryRegionName = src.ctname,
                tgt.SalesTerritoryKey = src.stkey

        WHEN NOT MATCHED THEN
            INSERT (
                City, StateProvinceCode, StateProvinceName, 
                CountryRegionCode, EnglishCountryRegionName, SpanishCountryRegionName, 
                FrenchCountryRegionName, PostalCode, SalesTerritoryKey, IpAddressLocator
            )
            VALUES (
                src.City, src.spc, src.spname, 
                src.ctrcode, src.ctname, 'Unknown', 
                'Unknown', src.pstcode, src.stkey, '-1'
            );

        COMMIT TRANSACTION;
        PRINT 'DimGeography loaded successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'Error occurred in usp_Load_DimGeography: ' + ERROR_MESSAGE();
    END CATCH
END;