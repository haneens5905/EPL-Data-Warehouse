-- ============================================================
-- FILE: 3_dw_tables.sql
-- PURPOSE: Create the Data Warehouse schema (star schema) with
--          5 dimension tables and 2 fact tables, then load
--          initial data from the OLTP source tables.
-- DATABASE: Football_Analysis
-- ============================================================
-- STAR SCHEMA OVERVIEW:
--
--   Dimensions:
--     - DimDate     : Season-level date info
--     - DimLeague   : League identity (supports future leagues)
--     - DimTeam     : Team info from points_table
--     - DimPlayer   : Player info from all_players_stats
--     - DimPosition : Distinct player positions
--
--   Facts:
--     - FactPlayerSeasonStats : Goals & Appearances per player
--     - FactMatchResults      : HomeGoals & AwayGoals per match
-- ============================================================

USE Football_Analysis;
GO

-- ------------------------------------------------------------
-- STEP 0: Create the DW schema
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw');
END;
GO

-- ------------------------------------------------------------
-- DIMENSION TABLES
-- ------------------------------------------------------------

-- DimDate: season-level granularity
CREATE TABLE dw.DimDate (
    DateKey         INT          NOT NULL PRIMARY KEY,  -- e.g. 20210801
    FullDate        DATE         NOT NULL,
    SeasonName      NVARCHAR(50) NOT NULL,
    SeasonYearStart INT          NOT NULL,
    SeasonYearEnd   INT          NOT NULL
);
GO

-- DimLeague: supports multiple competitions in the future
CREATE TABLE dw.DimLeague (
    LeagueKey  INT IDENTITY(1,1) PRIMARY KEY,
    LeagueName NVARCHAR(100) NOT NULL,
    Country    NVARCHAR(50)  NULL,
    SeasonName NVARCHAR(50)  NULL
);
GO

-- DimTeam: sourced from points_table
CREATE TABLE dw.DimTeam (
    TeamKey  INT IDENTITY(1,1) PRIMARY KEY,
    TeamName NVARCHAR(50) NOT NULL,
    Pos      TINYINT      NULL,
    Pld      TINYINT      NULL,
    W        TINYINT      NULL,
    D        TINYINT      NULL,
    L        TINYINT      NULL,
    GF       TINYINT      NULL,
    GA       TINYINT      NULL,
    GD       SMALLINT     NULL,
    Pts      TINYINT      NULL
);
GO

-- DimPlayer: sourced from all_players_stats
CREATE TABLE dw.DimPlayer (
    PlayerKey        INT IDENTITY(1,1) PRIMARY KEY,
    PlayerName       NVARCHAR(100) NOT NULL,
    JerseyNo         TINYINT       NULL,
    FullPositionText NVARCHAR(50)  NULL
);
GO

-- DimPosition: distinct positions from all_players_stats
CREATE TABLE dw.DimPosition (
    PositionKey   INT IDENTITY(1,1) PRIMARY KEY,
    PositionName  NVARCHAR(50) NOT NULL,
    PositionGroup NVARCHAR(50) NULL
);
GO

-- ------------------------------------------------------------
-- LOAD DIMENSIONS
-- ------------------------------------------------------------

-- Load DimDate with one row for EPL 2021-2022 season
INSERT INTO dw.DimDate (DateKey, FullDate, SeasonName, SeasonYearStart, SeasonYearEnd)
VALUES (20210801, '2021-08-01', 'EPL 2021-2022', 2021, 2022);
GO

-- Load DimLeague
INSERT INTO dw.DimLeague (LeagueName, Country, SeasonName)
VALUES ('English Premier League', 'England', 'EPL 2021-2022');
GO

-- Load DimTeam from OLTP points_table
INSERT INTO dw.DimTeam (TeamName, Pos, Pld, W, D, L, GF, GA, GD, Pts)
SELECT Team, Pos, Pld, W, D, L, GF, GA, GD, Pts
FROM dbo.points_table;
GO

-- Load DimPlayer from OLTP all_players_stats
INSERT INTO dw.DimPlayer (PlayerName, JerseyNo, FullPositionText)
SELECT DISTINCT Player, JerseyNo, Position
FROM dbo.all_players_stats;
GO

-- Load DimPosition from OLTP all_players_stats
INSERT INTO dw.DimPosition (PositionName, PositionGroup)
SELECT DISTINCT Position, NULL
FROM dbo.all_players_stats;
GO

-- ------------------------------------------------------------
-- FACT TABLES
-- ------------------------------------------------------------

-- FactPlayerSeasonStats: Measures = Goals, Appearances
CREATE TABLE dw.FactPlayerSeasonStats (
    PlayerSeasonStatsKey INT IDENTITY(1,1) PRIMARY KEY,

    DateKey     INT NOT NULL,
    LeagueKey   INT NOT NULL,
    TeamKey     INT NOT NULL,
    PlayerKey   INT NOT NULL,
    PositionKey INT NOT NULL,

    Goals       INT NOT NULL,
    Appearances INT NOT NULL,

    CONSTRAINT FK_FactPlayerSeasonStats_Date
        FOREIGN KEY (DateKey)     REFERENCES dw.DimDate (DateKey),
    CONSTRAINT FK_FactPlayerSeasonStats_League
        FOREIGN KEY (LeagueKey)   REFERENCES dw.DimLeague (LeagueKey),
    CONSTRAINT FK_FactPlayerSeasonStats_Team
        FOREIGN KEY (TeamKey)     REFERENCES dw.DimTeam (TeamKey),
    CONSTRAINT FK_FactPlayerSeasonStats_Player
        FOREIGN KEY (PlayerKey)   REFERENCES dw.DimPlayer (PlayerKey),
    CONSTRAINT FK_FactPlayerSeasonStats_Position
        FOREIGN KEY (PositionKey) REFERENCES dw.DimPosition (PositionKey)
);
GO

-- FactMatchResults: Measures = HomeGoals, AwayGoals
CREATE TABLE dw.FactMatchResults (
    MatchKey    INT IDENTITY(1,1) PRIMARY KEY,

    DateKey     INT NOT NULL,
    LeagueKey   INT NOT NULL,
    HomeTeamKey INT NOT NULL,
    AwayTeamKey INT NOT NULL,

    HomeGoals   INT NOT NULL,
    AwayGoals   INT NOT NULL,

    CONSTRAINT FK_FactMatchResults_Date
        FOREIGN KEY (DateKey)     REFERENCES dw.DimDate (DateKey),
    CONSTRAINT FK_FactMatchResults_League
        FOREIGN KEY (LeagueKey)   REFERENCES dw.DimLeague (LeagueKey),
    CONSTRAINT FK_FactMatchResults_HomeTeam
        FOREIGN KEY (HomeTeamKey) REFERENCES dw.DimTeam (TeamKey),
    CONSTRAINT FK_FactMatchResults_AwayTeam
        FOREIGN KEY (AwayTeamKey) REFERENCES dw.DimTeam (TeamKey)
);
GO

-- ------------------------------------------------------------
-- LOAD FACT TABLES
-- ------------------------------------------------------------

-- Load FactPlayerSeasonStats
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
JOIN dw.DimTeam     t   ON t.TeamName     = s.Team
JOIN dw.DimPlayer   p   ON p.PlayerName   = s.Player AND p.JerseyNo = s.JerseyNo
JOIN dw.DimPosition pos ON pos.PositionName = s.Position
JOIN dw.DimDate     d   ON d.SeasonName   = 'EPL 2021-2022'
JOIN dw.DimLeague   l   ON l.SeasonName   = 'EPL 2021-2022'
                       AND l.LeagueName   = 'English Premier League';
GO

-- Load FactMatchResults
-- Note: Result column is stored as TIME, converted to extract HomeGoals/AwayGoals
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
JOIN dw.DimTeam   ht ON ht.TeamName   = m.HomeTeam
JOIN dw.DimTeam   at ON at.TeamName   = m.AwayTeam
JOIN dw.DimDate   d  ON d.SeasonName  = 'EPL 2021-2022'
JOIN dw.DimLeague l  ON l.SeasonName  = 'EPL 2021-2022'
                    AND l.LeagueName  = 'English Premier League';
GO
