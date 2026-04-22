-- ============================================================
-- FILE: 2_staging_tables.sql
-- PURPOSE: Create staging tables used during ETL to safely
--          load raw CSV data before cleaning and inserting
--          into the main OLTP tables.
-- DATABASE: Football_Analysis
-- ============================================================
-- WHY STAGING TABLES?
--   - All columns stored as NVARCHAR to avoid BULK INSERT failures
--   - Dirty/malformed CSV data is handled here before type casting
-- ============================================================

USE Football_Analysis;
GO

-- ------------------------------------------------------------
-- Staging table for all_match_results (no MatchID column)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.stg_all_match_results') IS NULL
BEGIN
    CREATE TABLE dbo.stg_all_match_results (
        [Date]   DATE         NULL,
        HomeTeam NVARCHAR(50) NULL,
        [Result] TIME(0)      NULL,
        AwayTeam NVARCHAR(50) NULL
    );
END;

-- ------------------------------------------------------------
-- Staging table for all_players_stats (no PlayerID column)
-- Recreated fresh each load to avoid stale data
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.stg_all_players_stats') IS NOT NULL
    DROP TABLE dbo.stg_all_players_stats;

CREATE TABLE dbo.stg_all_players_stats (
    Team          NVARCHAR(50)  NULL,
    JerseyNo      NVARCHAR(20)  NULL,
    Player        NVARCHAR(100) NULL,
    [Position]    NVARCHAR(50)  NULL,
    Apearances    NVARCHAR(20)  NULL,
    Substitutions NVARCHAR(20)  NULL,
    Goals         NVARCHAR(20)  NULL,
    Penalties     NVARCHAR(20)  NULL,
    YellowCards   NVARCHAR(20)  NULL,
    RedCards      NVARCHAR(20)  NULL
);
