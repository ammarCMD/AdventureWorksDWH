# AdventureWorksDWH

Overview
--------
This repository contains an end-to-end ETL implementation for the AdventureWorks data warehouse. The solution loads dimension and fact tables using metadata-driven stored procedures. It includes logging, restartability, and batch-loading support to enable robust, repeatable ETL runs.

Key Features
------------
- Metadata-driven loading: Stored procedures read table/column metadata to populate dimensions and facts.
- Full set of stored procedures: Separate SPs for dimensions and facts for clear separation of concerns.
- Logging: Centralized ETL logging via `usp_ETL_Log.sql` and related procedures.
- Restartability: Procedures are designed to resume from checkpoints on failure.
- Batch loading: Support for batch loads and master batch orchestration.

Repository Structure (visible)
-----------------------------
Top-level folders and important scripts:

AdventureWorksDimensions/
	Usp_load_dimcurrency.sql
	usp_load_dimcustomer.sql
	usp_load_dimcustomersimple.sql
	Usp_load_dimdate.sql
	Usp_load_dimdepartmentgroup.sql
	Usp_load_dimemployee.sql
	Usp_load_dimgeography.sql
	Usp_load_dimorganization.sql
	Usp_load_dimproduct.sql
	Usp_load_dimproductcategory.sql
	Usp_load_dimproductsubcategory.sql
	Usp_load_dimpromotion.sql
	Usp_load_dimreseller.sql
	Usp_load_dimsalesreason.sql
	Usp_load_dimsalesterritory.sql
	Usp_load_dimscenario.sql

AdventureWorksFacts/
	Usp_load_factadditionalinternationalproductdescription.sql
	Usp_load_factcallcenter.sql
	Usp_load_factcurrencyrate.sql
	Usp_load_factfinance.sql
	Usp_load_factinternetsales.sql
	Usp_load_factinternetsalesreason.sql
	Usp_load_factproductinventory.sql
	Usp_load_factresellersales.sql
	Usp_load_factsalesquota.sql
	Usp_load_factsurveyresponse.sql
	Usp_load_newfactcurrencyrate.sql

BatchOperations/
	batchload.sql
	usp_ETL_Log.sql
	usp_LoadCustomerBatch.sql
	usp_Master_ETL_Load.sql
	uspAllProcDynamic.sql

How it works
------------
- Dimension loaders: Each SP in `AdventureWorksDimensions/` is responsible for loading or updating a dimension table using the defined metadata.
- Fact loaders: SPs in `AdventureWorksFacts/` load fact tables and handle integrity and surrogate key lookups.
- Batch orchestration: `BatchOperations/` contains scripts to run batches, track progress, and orchestrate restarts.
- Logging & monitoring: Use `usp_ETL_Log.sql` to record start/end times, row counts, and error messages for each job.

Running the ETL
---------------
1. Ensure the target database is available and the metadata tables required by the SPs are populated.
2. Execute batch orchestration stored procedure(s) from `BatchOperations/`, for example `usp_Master_ETL_Load.sql`.
3. For single-table runs, call the specific loader stored procedure from `AdventureWorksDimensions/` or `AdventureWorksFacts/`.

Example (SQL Server):

```
EXEC dbo.usp_Master_ETL_Load; -- master orchestrator for batch loads
EXEC dbo.Usp_load_dimcustomer; -- run individual dimension load
```

Notes on Restartability and Logging
----------------------------------
- Checkpoints: Procedures write progress information to the ETL log so processing can resume from the last successful checkpoint.
- Idempotence: Loaders are written to be safe for re-run where possible (updates instead of blind inserts when appropriate).
- Errors: Errors are logged with detail; use the log to identify and re-run failed batches.

Contributing
------------
If you extend or adapt the loaders, follow these guidelines:
- Keep metadata-driven patterns consistent for new loaders.
- Add or update logging entries to ensure restartability.
- Test batch runs using a subset of data before full production runs.

Contact
-------
For questions or collaboration, connect on LinkedIn: https://www.linkedin.com/in/iammarrasheed

License
-------
See the `LICENSE` file for license terms.
