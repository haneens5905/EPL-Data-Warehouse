-- ============================================================
-- FILE: 5_dw_reload.sql
-- PURPOSE: Stored procedure that fully reloads the Data
--          Warehouse from the current OLTP source tables.
--          Called automatically by the ETL daily load procedure.
-- DATABASE: Football_Analysis
-- ============================================================
-- FLOW:
--   1. Truncate fact tables
--   2. Delete dimension tables (DELETE used due to FKs)
--   3. Reload dimensions from OLTP
--   4. Reload fact tables via joins to dimension keys
-- ============================================================

USE Football_Analysis;
GO

CREATE OR ALTER PROCEDURE dbo.usp_DW_Reload_FromSource
AS
BEGIN
    SET NOCOUNT ON;

    -- --------------------------------------------------------
    -- 1) Clear existing DW data
    -- --------------------------------------------------------
    IF OBJECT_ID('dw.FactMatchResults')       IS NOT NULL TRUNCATE TABLE dw.FactMatchResults;
    IF OBJECT_ID('dw.FactPlayerSeasonStats')  IS NOT NULL TRUNCATE TABLE dw.FactPlayerSeasonStats;

    IF OBJECT_ID('dw.DimTeam')     IS NOT NULL DELETE FROM dw.DimTeam;
    IF OBJECT_ID('dw.DimPlayer')   IS NOT NULL DELETE FROM dw.DimPlayer;
    IF OBJECT_ID('dw.DimPosition') IS NOT NULL DELETE FROM dw.DimPosition;

    -- --------------------------------------------------------
    -- 2) Reload Dimensions
    -- --------------------------------------------------------
    INSERT INTO dw.DimTeam (TeamName, Pos, Pld, W, D, L, GF, GA, GD, Pts)
    SELECT Team, Pos, Pld, W, D, L, GF, GA, GD, Pts
    FROM dbo.points_table;

    INSERT INTO dw.DimPlayer (PlayerName, JerseyNo, FullPositionText)
    SELECT DISTINCT Player, JerseyNo, Position
    FROM dbo.all_players_stats;

    INSERT INTO dw.DimPosition (PositionName, PositionGroup)
    SELECT DISTINCT Position, NULL
    FROM dbo.all_players_stats;

    -- --------------------------------------------------------
    -- 3) Reload FactPlayerSeasonStats
    -- --------------------------------------------------------
    INSERT INTO dw.FactPlayerSeasonStats (
        DateKey, LeagueKey, TeamKey, PlayerKey, PositionKey, Goals, Appearances
    )
    SELECT
        d.DateKey,
        l.LeagueKey,
        t.TeamKey,
        p.PlayerKey,
        pos.PositionKey,
        s.Goals,
        s.Apearances
    FROM dbo.all_players_stats s
    JOIN dw.DimTeam     t   ON t.TeamName      = s.Team
    JOIN dw.DimPlayer   p   ON p.PlayerName    = s.Player AND p.JerseyNo = s.JerseyNo
    JOIN dw.DimPosition pos ON pos.PositionName = s.Position
    JOIN dw.DimDate     d   ON d.SeasonName    = 'EPL 2021-2022'
    JOIN dw.DimLeague   l   ON l.SeasonName    = 'EPL 2021-2022'
                           AND l.LeagueName    = 'English Premier League';

    -- --------------------------------------------------------
    -- 4) Reload FactMatchResults
    -- --------------------------------------------------------
    INSERT INTO dw.FactMatchResults (
        DateKey, LeagueKey, HomeTeamKey, AwayTeamKey, HomeGoals, AwayGoals
    )
    SELECT
        d.DateKey,
        l.LeagueKey,
        ht.TeamKey,
        at.TeamKey,
        CAST(LEFT(CONVERT(VARCHAR(8), m.Result, 108), 2) AS INT)        AS HomeGoals,
        CAST(SUBSTRING(CONVERT(VARCHAR(8), m.Result, 108), 4, 2) AS INT) AS AwayGoals
    FROM dbo.all_match_results m
    JOIN dw.DimTeam   ht ON ht.TeamName  = m.HomeTeam
    JOIN dw.DimTeam   at ON at.TeamName  = m.AwayTeam
    JOIN dw.DimDate   d  ON d.SeasonName = 'EPL 2021-2022'
    JOIN dw.DimLeague l  ON l.SeasonName = 'EPL 2021-2022'
                        AND l.LeagueName = 'English Premier League';

END;
GO
