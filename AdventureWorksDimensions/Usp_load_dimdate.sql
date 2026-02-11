CREATE PROCEDURE dbo.Usp_load_dimdate
AS
  BEGIN
      SET nocount ON;

      BEGIN try
          BEGIN TRANSACTION;

          DECLARE @StartDate DATE = '2010-01-01';
          DECLARE @EndDate DATE = '2030-12-31';

          WHILE @StartDate <= @EndDate
            BEGIN
                INSERT INTO dbo.dimdate
                            (datekey,
                             fulldatealternatekey,
                             daynumberofweek,
                             englishdaynameofweek,
                             spanishdaynameofweek,
                             frenchdaynameofweek,
                             daynumberofmonth,
                             daynumberofyear,
                             weeknumberofyear,
                             englishmonthname,
                             spanishmonthname,
                             frenchmonthname,
                             monthnumberofyear,
                             calendarquarter,
                             calendaryear,
                             calendarsemester,
                             fiscalquarter,
                             fiscalyear,
                             fiscalsemester)
                SELECT CONVERT(INT, CONVERT(VARCHAR(8), @StartDate, 112)) AS
                       DateKey,
                       @StartDate                                         AS
                       FullDateAlternateKey,
                       Datepart(weekday, @StartDate)                      AS
                       DayNumberOfWeek
                       ,
                       Datename(weekday, @StartDate)                      AS
                       EnglishDayNameOfWeek,
                       -- Manual mapping for Spanish/French if your collation is English
                       CASE Datename(weekday, @StartDate)
                         WHEN 'Monday' THEN 'Lunes'
                         WHEN 'Tuesday' THEN 'Martes'
                         WHEN 'Wednesday' THEN 'Miércoles'
                         WHEN 'Thursday' THEN 'Jueves'
                         WHEN 'Friday' THEN 'Viernes'
                         WHEN 'Saturday' THEN 'Sábado'
                         ELSE 'Domingo'
                       END                                                AS
                       SpanishDayNameOfWeek,
                       CASE Datename(weekday, @StartDate)
                         WHEN 'Monday' THEN 'Lundi'
                         WHEN 'Tuesday' THEN 'Mardi'
                         WHEN 'Wednesday' THEN 'Mercredi'
                         WHEN 'Thursday' THEN 'Jeudi'
                         WHEN 'Friday' THEN 'Vendredi'
                         WHEN 'Saturday' THEN 'Samedi'
                         ELSE 'Dimanche'
                       END                                                AS
                       FrenchDayNameOfWeek
                       ,
                       Datepart(day, @StartDate)                          AS
                       DayNumberOfMonth,
                       Datepart(dayofyear, @StartDate)                    AS
                       DayNumberOfYear
                       ,
                       Datepart(week, @StartDate)                         AS
                       WeekNumberOfYear,
                       Datename(month, @StartDate)                        AS
                       EnglishMonthName,
                       CASE Datename(month, @StartDate)
                         WHEN 'January' THEN 'Enero'
                         WHEN 'February' THEN 'Febrero' -- ... add others
                         ELSE 'Diciembre'
                       END                                                AS
                       SpanishMonthName,
                       'N/A'                                              AS
                       FrenchMonthName
                       ,
                       -- Simplified for example
                       Datepart(month, @StartDate)                        AS
                       MonthNumberOfYear,
                       Datepart(quarter, @StartDate)                      AS
                       CalendarQuarter
                       ,
                       Datepart(year, @StartDate)                         AS
                       CalendarYear,
                       CASE
                         WHEN Datepart(month, @StartDate) <= 6 THEN 1
                         ELSE 2
                       END                                                AS
                       CalendarSemester,
                       Datepart(quarter, Dateadd(month, 6, @StartDate))   AS
                       FiscalQuarter
                       ,
                       Datepart(year, Dateadd(month, 6, @StartDate))      AS
                       FiscalYear,
                       CASE
                         WHEN Datepart(month, Dateadd(month, 6, @StartDate)) <=
                              6
                       THEN
                         1
                         ELSE 2
                       END                                                AS
                       FiscalSemester;

                SET @StartDate = Dateadd(day, 1, @StartDate);
            END;

          COMMIT TRANSACTION;

          PRINT 'DimDate loaded successfully.';
      END try

      BEGIN catch
          IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

          PRINT 'Error occurred in usp_Load_DimDate: '
                + Error_message();
      END catch
  END;