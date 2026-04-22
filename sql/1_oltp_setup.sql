-- ============================================================
-- FILE: 1_oltp_setup.sql
-- PURPOSE: Set up the OLTP source tables with primary keys,
--          foreign keys, and data integrity checks.
-- DATABASE: Football_Analysis
-- ============================================================

USE Football_Analysis;
GO

-- ------------------------------------------------------------
-- STEP 1: Check for duplicate teams in points_table
--         (should return zero rows before creating PK)
-- ------------------------------------------------------------
SELECT Team, COUNT(*) AS Cnt
FROM dbo.points_table
GROUP BY Team
HAVING COUNT(*) > 1;

-- ------------------------------------------------------------
-- STEP 2: Make Team the primary key in points_table
-- ------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE name = 'PK_points_table'
      AND parent_object_id = OBJECT_ID('dbo.points_table')
)
BEGIN
    ALTER TABLE dbo.points_table
    ALTER COLUMN Team NVARCHAR(50) NOT NULL;

    ALTER TABLE dbo.points_table
    ADD CONSTRAINT PK_points_table PRIMARY KEY CLUSTERED (Team);
END;

-- ------------------------------------------------------------
-- STEP 3: Add MatchID + primary key to all_match_results
-- ------------------------------------------------------------
IF COL_LENGTH('dbo.all_match_results', 'MatchID') IS NULL
BEGIN
    ALTER TABLE dbo.all_match_results
    ADD MatchID INT IDENTITY(1,1) NOT NULL;

    ALTER TABLE dbo.all_match_results
    ADD CONSTRAINT PK_all_match_results PRIMARY KEY CLUSTERED (MatchID);
END;

-- ------------------------------------------------------------
-- STEP 4: Add PlayerID + primary key to all_players_stats
-- ------------------------------------------------------------
IF COL_LENGTH('dbo.all_players_stats', 'PlayerID') IS NULL
BEGIN
    ALTER TABLE dbo.all_players_stats
    ADD PlayerID INT IDENTITY(1,1) NOT NULL;

    ALTER TABLE dbo.all_players_stats
    ADD CONSTRAINT PK_all_players_stats PRIMARY KEY CLUSTERED (PlayerID);
END;

-- ------------------------------------------------------------
-- STEP 5: Standardize Team column data types across all tables
-- ------------------------------------------------------------
ALTER TABLE dbo.points_table      ALTER COLUMN Team     NVARCHAR(50) NOT NULL;
ALTER TABLE dbo.all_match_results ALTER COLUMN HomeTeam NVARCHAR(50) NOT NULL;
ALTER TABLE dbo.all_match_results ALTER COLUMN AwayTeam NVARCHAR(50) NOT NULL;
ALTER TABLE dbo.all_players_stats ALTER COLUMN Team     NVARCHAR(50) NOT NULL;

-- ------------------------------------------------------------
-- STEP 6: Verify team name consistency (should return 0 rows)
-- ------------------------------------------------------------
-- Home teams not found in points_table
SELECT DISTINCT m.HomeTeam
FROM dbo.all_match_results m
LEFT JOIN dbo.points_table p ON m.HomeTeam = p.Team
WHERE p.Team IS NULL;

-- Away teams not found in points_table
SELECT DISTINCT m.AwayTeam
FROM dbo.all_match_results m
LEFT JOIN dbo.points_table p ON m.AwayTeam = p.Team
WHERE p.Team IS NULL;

-- Player teams not found in points_table
SELECT DISTINCT s.Team
FROM dbo.all_players_stats s
LEFT JOIN dbo.points_table p ON s.Team = p.Team
WHERE p.Team IS NULL;

-- ------------------------------------------------------------
-- STEP 7: Create foreign keys (only if not already created)
-- ------------------------------------------------------------

-- FK: HomeTeam -> points_table
IF NOT EXISTS (
    SELECT 1 FROM sys.foreign_keys
    WHERE name = 'FK_match_home_team'
      AND parent_object_id = OBJECT_ID('dbo.all_match_results')
)
BEGIN
    ALTER TABLE dbo.all_match_results
    ADD CONSTRAINT FK_match_home_team
        FOREIGN KEY (HomeTeam) REFERENCES dbo.points_table (Team);
END;

-- FK: AwayTeam -> points_table
IF NOT EXISTS (
    SELECT 1 FROM sys.foreign_keys
    WHERE name = 'FK_match_away_team'
      AND parent_object_id = OBJECT_ID('dbo.all_match_results')
)
BEGIN
    ALTER TABLE dbo.all_match_results
    ADD CONSTRAINT FK_match_away_team
        FOREIGN KEY (AwayTeam) REFERENCES dbo.points_table (Team);
END;

-- FK: Player Team -> points_table
IF NOT EXISTS (
    SELECT 1 FROM sys.foreign_keys
    WHERE name = 'FK_players_team'
      AND parent_object_id = OBJECT_ID('dbo.all_players_stats')
)
BEGIN
    ALTER TABLE dbo.all_players_stats
    ADD CONSTRAINT FK_players_team
        FOREIGN KEY (Team) REFERENCES dbo.points_table (Team);
END;
