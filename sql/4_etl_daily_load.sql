-- ============================================================
-- FILE: 4_etl_daily_load.sql
-- PURPOSE: Full ETL pipeline stored procedure that:
--            1. Clears staging + OLTP tables
--            2. BULK INSERTs fresh CSV data into staging
--            3. Cleans and moves data into OLTP tables
--            4. Calls the DW reload procedure
--            5. Logs success/failure to ETL_LoadLog
--            6. Sends an email notification
--          Also creates the ETL_LoadLog table and SQL Agent Job.
-- DATABASE: Football_Analysis
-- ============================================================
-- NOTE: Update the file paths below to match your machine
--       before running.
-- ============================================================

USE Football_Analysis;
GO

-- ------------------------------------------------------------
-- ETL Logging Table
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.ETL_LoadLog') IS NULL
BEGIN
    CREATE TABLE dbo.ETL_LoadLog (
        LoadID       INT IDENTITY(1,1) PRIMARY KEY,
        LoadStart    DATETIME2(0) NOT NULL,
        LoadEnd      DATETIME2(0) NULL,
        Status       NVARCHAR(20) NOT NULL,  -- 'SUCCESS' or 'FAILED'
        ErrorMessage NVARCHAR(4000) NULL
    );
END;
GO

-- ------------------------------------------------------------
-- Stored Procedure: usp_DW_DailyLoad_FromCsv
-- ------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_DW_DailyLoad_FromCsv
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime    DATETIME2(0)   = SYSDATETIME();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @BodySuccess  NVARCHAR(MAX);
    DECLARE @BodyFailure  NVARCHAR(MAX);

    BEGIN TRY

        -- --------------------------------------------------------
        -- 1) Clear staging and OLTP tables
        -- --------------------------------------------------------
        TRUNCATE TABLE dbo.all_match_results;
        TRUNCATE TABLE dbo.all_players_stats;
        DELETE  FROM   dbo.points_table;

        TRUNCATE TABLE dbo.stg_all_match_results;
        TRUNCATE TABLE dbo.stg_all_players_stats;

        -- --------------------------------------------------------
        -- 2) BULK INSERT raw CSV data into staging / OLTP
        --    UPDATE THESE PATHS to match your file location
        -- --------------------------------------------------------

        -- points_table has no identity column → load directly
        BULK INSERT dbo.points_table
        FROM 'C:\Users\DEll\Desktop\Year 3\Advanced Database\Assignment 1\points_table.csv'
        WITH (
            FIRSTROW       = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR   = '\n',
            CODEPAGE        = '65001',
            TABLOCK
        );

        -- all_match_results → staging first (excludes MatchID identity column)
        BULK INSERT dbo.stg_all_match_results
        FROM 'C:\Users\DEll\Desktop\Year 3\Advanced Database\Assignment 1\all_match_results.csv'
        WITH (
            FIRSTROW       = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR   = '\n',
            CODEPAGE        = '65001',
            TABLOCK
        );

        -- all_players_stats → staging first (excludes PlayerID identity column)
        BULK INSERT dbo.stg_all_players_stats
        FROM 'C:\Users\DEll\Desktop\Year 3\Advanced Database\Assignment 1\all_players_stats.csv'
        WITH (
            FIRSTROW       = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR   = '\n',
            CODEPAGE        = '65001',
            TABLOCK
        );

        -- --------------------------------------------------------
        -- 3) Clean and move staging data into real OLTP tables
        --    Identity columns (MatchID, PlayerID) auto-generated
        -- --------------------------------------------------------
        INSERT INTO dbo.all_match_results ([Date], HomeTeam, [Result], AwayTeam)
        SELECT [Date], HomeTeam, [Result], AwayTeam
        FROM dbo.stg_all_match_results;

        INSERT INTO dbo.all_players_stats (
            Team, JerseyNo, Player, [Position],
            Apearances, Substitutions, Goals,
            Penalties, YellowCards, RedCards
        )
        SELECT
            Team,
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(JerseyNo)),      '') AS INT), 0),
            Player,
            [Position],
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(Apearances)),    '') AS INT), 0),
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(Substitutions)), '') AS INT), 0),
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(Goals)),         '') AS INT), 0),
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(Penalties)),     '') AS INT), 0),
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(YellowCards)),   '') AS INT), 0),
            ISNULL(TRY_CAST(NULLIF(LTRIM(RTRIM(RedCards)),      '') AS INT), 0)
        FROM dbo.stg_all_players_stats;

        -- --------------------------------------------------------
        -- 4) Reload the Data Warehouse from updated OLTP tables
        -- --------------------------------------------------------
        EXEC dbo.usp_DW_Reload_FromSource;

        -- --------------------------------------------------------
        -- 5) Log SUCCESS
        -- --------------------------------------------------------
        INSERT INTO dbo.ETL_LoadLog (LoadStart, LoadEnd, Status, ErrorMessage)
        VALUES (@StartTime, SYSDATETIME(), N'SUCCESS', NULL);

        -- --------------------------------------------------------
        -- 6) Send SUCCESS email
        -- --------------------------------------------------------
        SET @BodySuccess = N'The EPL data warehouse daily load completed successfully.';

        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'DW_Mail_Profile',
            @recipients   = 'haneens899@gmail.com',
            @subject      = 'EPL DW Daily Load - SUCCESS',
            @body         = @BodySuccess;

    END TRY
    BEGIN CATCH

        SET @ErrorMessage = ERROR_MESSAGE();

        -- Log FAILURE
        INSERT INTO dbo.ETL_LoadLog (LoadStart, LoadEnd, Status, ErrorMessage)
        VALUES (@StartTime, SYSDATETIME(), N'FAILED', @ErrorMessage);

        -- Send FAILURE email
        SET @BodyFailure =
            N'EPL DW daily load FAILED with error: ' + ISNULL(@ErrorMessage, N'Unknown error');

        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'DW_Mail_Profile',
            @recipients   = 'haneens899@gmail.com',
            @subject      = 'EPL DW Daily Load - FAILED',
            @body         = @BodyFailure;

        THROW;

    END CATCH
END;
GO

-- ------------------------------------------------------------
-- SQL Agent Job: EPL_DW_Daily_Load
-- Runs every day at 02:00 AM
-- ------------------------------------------------------------
USE msdb;
GO

EXEC sp_add_job
    @job_name = N'EPL_DW_Daily_Load';

EXEC sp_add_jobstep
    @job_name    = N'EPL_DW_Daily_Load',
    @step_name   = N'Run ETL Procedure',
    @subsystem   = N'TSQL',
    @command     = N'EXEC Football_Analysis.dbo.usp_DW_DailyLoad_FromCsv;',
    @database_name = N'Football_Analysis';

EXEC sp_add_schedule
    @schedule_name     = N'Daily_2AM',
    @freq_type         = 4,       -- Daily
    @freq_interval     = 1,
    @active_start_time = 020000;  -- 02:00:00 AM

EXEC sp_attach_schedule
    @job_name      = N'EPL_DW_Daily_Load',
    @schedule_name = N'Daily_2AM';

EXEC sp_add_jobserver
    @job_name = N'EPL_DW_Daily_Load';
GO
