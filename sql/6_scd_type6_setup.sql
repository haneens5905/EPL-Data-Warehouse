-- ============================================================
-- FILE: 6_scd_type6_setup.sql
-- PURPOSE: Add SCD Type 6 columns to DimTeam and initialize
--          their values.
-- DATABASE: Football_Analysis
-- ============================================================
-- SCD TYPE 6 COMBINES:
--   Type 1 - Overwrite current values  (CurrentPts, CurrentPos)
--   Type 2 - Track full history        (ValidFrom, ValidTo, IsCurrent)
--   Type 3 - (implied via current cols)
--
-- Applied on: dw.DimTeam
-- Tracked fields: Pts (Points), Pos (League Position)
-- ============================================================

USE Football_Analysis;
GO

-- ------------------------------------------------------------
-- Add SCD Type 6 columns to DimTeam
-- ------------------------------------------------------------
ALTER TABLE dw.DimTeam
ADD ValidFrom  DATE    DEFAULT '2021-08-01',
    ValidTo    DATE    DEFAULT '9999-12-31',
    IsCurrent  BIT     DEFAULT 1;

ALTER TABLE dw.DimTeam
ADD CurrentPts TINYINT,
    CurrentPos TINYINT;

-- ------------------------------------------------------------
-- Initialize SCD columns for all existing rows
-- ------------------------------------------------------------
UPDATE dw.DimTeam
SET
    ValidFrom  = '2021-08-01',
    ValidTo    = '9999-12-31',
    IsCurrent  = 1,
    CurrentPts = Pts,
    CurrentPos = Pos;
GO

-- ------------------------------------------------------------
-- (Optional) Test: simulate a points update for Liverpool
-- to verify the SCD procedure works correctly
-- ------------------------------------------------------------
-- UPDATE dbo.points_table
-- SET Pts = Pts + 3, W = W + 1
-- WHERE Team = 'Liverpool';
