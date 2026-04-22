-- ============================================================
-- FILE: 7_scd_type6_procedure.sql
-- PURPOSE: Stored procedure implementing SCD Type 6 incremental
--          load for DimTeam. Detects changed teams, closes old
--          history records, inserts new ones, and updates
--          current value columns.
-- DATABASE: Football_Analysis
-- ============================================================
-- HOW IT WORKS:
--   1. Find teams where Pts or Pos changed vs current DW record
--   2. Close old record: set ValidTo = yesterday, IsCurrent = 0
--   3. Insert new record: ValidFrom = today, IsCurrent = 1
--   4. Update CurrentPts / CurrentPos on ALL rows for that team
--      (Type 1 aspect — always reflects the latest values)
-- ============================================================

USE Football_Analysis;
GO

CREATE OR ALTER PROCEDURE dbo.usp_SCD_Type6_UpdateDimensions
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadDate DATE = CAST(GETDATE() AS DATE);

    -- --------------------------------------------------------
    -- 1) Detect teams with updated Pts or Pos
    -- --------------------------------------------------------
    SELECT
        s.Team AS TeamName,
        s.Pos  AS NewPos,
        s.Pts  AS NewPts
    INTO #Changes
    FROM dbo.points_table s
    JOIN dw.DimTeam t ON s.Team = t.TeamName
    WHERE t.IsCurrent = 1
      AND (s.Pts <> t.Pts OR s.Pos <> t.Pos);

    -- If no changes detected, exit early
    IF NOT EXISTS (SELECT 1 FROM #Changes)
    BEGIN
        DROP TABLE #Changes;
        RETURN;
    END;

    -- --------------------------------------------------------
    -- 2) Close previous version (SCD Type 2)
    -- --------------------------------------------------------
    UPDATE t
    SET
        ValidTo   = DATEADD(DAY, -1, @LoadDate),
        IsCurrent = 0
    FROM dw.DimTeam t
    JOIN #Changes c ON t.TeamName = c.TeamName
    WHERE t.IsCurrent = 1;

    -- --------------------------------------------------------
    -- 3) Insert new updated version (SCD Type 2)
    -- --------------------------------------------------------
    INSERT INTO dw.DimTeam (
        TeamName, Pos, Pld, W, D, L, GF, GA, GD, Pts,
        ValidFrom, ValidTo, IsCurrent, CurrentPts, CurrentPos
    )
    SELECT
        c.TeamName,
        c.NewPos,
        t_old.Pld, t_old.W, t_old.D, t_old.L,
        t_old.GF,  t_old.GA, t_old.GD,
        c.NewPts,
        @LoadDate,    -- ValidFrom = today
        '9999-12-31', -- ValidTo   = open-ended
        1,            -- IsCurrent = true
        c.NewPts,     -- CurrentPts (Type 1)
        c.NewPos      -- CurrentPos (Type 1)
    FROM #Changes c
    JOIN dw.DimTeam t_old ON t_old.TeamName = c.TeamName
    WHERE t_old.ValidTo = DATEADD(DAY, -1, @LoadDate);

    -- --------------------------------------------------------
    -- 4) Update CurrentPts / CurrentPos on ALL rows (Type 1)
    --    So any historical row always shows the latest values
    -- --------------------------------------------------------
    UPDATE t
    SET
        CurrentPts = c.NewPts,
        CurrentPos = c.NewPos
    FROM dw.DimTeam t
    JOIN #Changes c ON t.TeamName = c.TeamName;

    DROP TABLE #Changes;

END;
GO

-- ------------------------------------------------------------
-- Test: Run the procedure and verify Liverpool history
-- ------------------------------------------------------------
-- EXEC dbo.usp_SCD_Type6_UpdateDimensions;
--
-- SELECT
--     TeamName,
--     Pts,
--     CurrentPts,
--     ValidFrom,
--     ValidTo,
--     IsCurrent
-- FROM dw.DimTeam
-- WHERE TeamName = 'Liverpool'
-- ORDER BY ValidFrom;
