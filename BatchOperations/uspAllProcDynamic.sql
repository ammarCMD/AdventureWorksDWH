CREATE PROCEDURE dbo.uspAllProcDynamic
as
begin
	declare @processname nvarchar (255)
	declare @currentstep nvarchar (255)
	declare @starttime datetime
	declare @endtime datetime
	declare @status nvarchar (50)
	declare @errormessage nvarchar (max)
	declare @processid int = 1
	declare @targettable nvarchar(50)
	declare @sourcetable nvarchar(50)
	declare @maxPid int = (select max(processid) from AdventureWorksDW_ammar.dbo.ETL_Metadata)
	while @processid  <= @maxPid
	begin
		select 
		@currentstep = processname, 
		@targettable = targettable,
		@sourcetable = sourcetable
		from AdventureWorksDW_ammar.dbo.ETL_Metadata where processid = @processid
		set @starttime = getdate()
		begin try
			DECLARE @sql NVARCHAR(MAX) = 'EXEC ' + @currentstep;
            EXEC sp_executesql @sql;
            SET @status = 'Success';
            SET @errormessage = NULL;
			set @endtime = getdate()
		end try
		begin catch
			SET @status = 'Failed';
            SET @errormessage = Error_Message();
			set @endtime = getdate();
			INSERT INTO AdventureWorksDW_ammar.dbo.ETL_Log (ProcessID, ProcessName, StartTime, EndTime, RowsInserted, Status, ErrorMessage)
        	VALUES (@ProcessID, @CurrentStep, @Starttime, GETDATE(),@@rowcount, 'Failed', ERROR_MESSAGE())
		end catch
		insert into AdventureWorksDW_ammar.dbo.ETL_Log (processid, processname, targettable, sourcetable, starttime, endtime,RowsInserted, status, errormessage)
		values (@processid, @currentstep, @targettable, @sourcetable, @starttime, @endtime,@@rowcount, @status, Error_Message())
		SET @processid = @processid + 1;
	end
end;