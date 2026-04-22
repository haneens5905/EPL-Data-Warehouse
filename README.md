# EPL Data Warehouse — English Premier League 2021/22

> **Course:** SIS314 Advanced Databases — Cairo University, Faculty of Computers and Artificial Intelligence  
> **Assignment:** Data Warehouse Design & Implementation

---

## Project Overview

This project implements a full **Data Warehouse** for the English Premier League (EPL) 2021–2022 season using **SQL Server**. It covers the complete pipeline from raw CSV data through to a star schema DW, with automated ETL, logging, email notifications, and slowly changing dimensions.

**Dataset:** [EPL 21-22 on Kaggle](https://www.kaggle.com/datasets/azminetoushikwasi/epl-21-22-matches-players)

---

## Business Questions Answered

- Who are the top scorers of the season?
- Which teams have the weakest defensive record?
- Which players should be considered for sale based on performance?
- How did match results vary across home and away games?
- Which players deserve nomination for Player of the Season?

---

## Star Schema Design

```
                    ┌─────────────┐
                    │  DimDate    │
                    └──────┬──────┘
                           │
 ┌───────────┐    ┌────────┴────────┐    ┌────────────┐
 │ DimLeague │────│ FactPlayerStats │────│  DimPlayer │
 └───────────┘    └────────┬────────┘    └────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
        ┌─────┴──────┐           ┌──────┴─────┐
        │  DimTeam   │           │ DimPosition│
        └────────────┘           └────────────┘

                    ┌─────────────────────┐
                    │  FactMatchResults   │
                    │  (HomeTeam/AwayTeam │
                    │   both → DimTeam)  │
                    └─────────────────────┘
```

### Dimensions

| Table | Description |
|---|---|
| `DimDate` | Season-level date info (EPL 2021-2022) |
| `DimLeague` | League identity — designed to support future leagues |
| `DimTeam` | Team stats sourced from `points_table` |
| `DimPlayer` | Player info sourced from `all_players_stats` |
| `DimPosition` | Distinct playing positions |

### Fact Tables

| Table | Measures |
|---|---|
| `FactPlayerSeasonStats` | Goals, Appearances |
| `FactMatchResults` | HomeGoals, AwayGoals |

---

## OLTP Source Tables

| Table | Description |
|---|---|
| `points_table` | Season standings for all 20 teams |
| `all_players_stats` | Individual player statistics |
| `all_match_results` | All match scores and results |

---

## ETL Pipeline

```
CSV Files
  └─→ BULK INSERT → Staging Tables (NVARCHAR)
        └─→ Data Cleaning (TRY_CAST, ISNULL)
              └─→ OLTP Tables
                    └─→ DW Reload Procedure
                          └─→ ETL Log + Email Notification
```

- **Staging tables** absorb raw CSV data as strings to prevent load failures
- **TRY_CAST + ISNULL** safely converts and defaults dirty values (e.g. empty strings → 0)
- **Full reload strategy** truncates and reloads the DW on each run for simplicity
- **SQL Agent Job** (`EPL_DW_Daily_Load`) runs the pipeline automatically at 2:00 AM daily
- **Email alerts** sent on both success and failure via `sp_send_dbmail`

---

## SCD Type 6 — DimTeam

SCD Type 6 (hybrid of Types 1, 2, and 3) is applied to `DimTeam` to track changes in team **Points** and **League Position** over time.

| Column | Purpose |
|---|---|
| `ValidFrom` | Date this record version became active |
| `ValidTo` | Date this record version expired (`9999-12-31` = current) |
| `IsCurrent` | `1` = active record, `0` = historical |
| `CurrentPts` | Always reflects the latest points (Type 1) |
| `CurrentPos` | Always reflects the latest position (Type 1) |

---

## Repository Structure

```
EPL-Data-Warehouse/
│
├── README.md
│
├── data/
│   ├── points_table.csv          # Team season standings
│   ├── all_players_stats.csv     # Player statistics
│   └── all_match_results.csv     # Match results
│
├── docs/
│   ├── ERD.jpg                   # Source database ERD
│   └── Assigment1_EPL.pdf        # Full assignment report
│
└── sql/
    ├── 1_oltp_setup.sql          # PKs, FKs, data integrity
    ├── 2_staging_tables.sql      # Staging tables for ETL
    ├── 3_dw_tables.sql           # Star schema creation + initial load
    ├── 4_etl_daily_load.sql      # ETL procedure + SQL Agent Job
    ├── 5_dw_reload.sql           # DW reload stored procedure
    ├── 6_scd_type6_setup.sql     # SCD Type 6 column setup
    └── 7_scd_type6_procedure.sql # SCD Type 6 incremental load procedure
```

---

## How to Run

> Requires: **SQL Server** with **SQL Server Agent** and **Database Mail** configured.

1. Create a database named `Football_Analysis` in SSMS
2. Run the SQL scripts **in order** (1 → 7)
3. Update the CSV file paths in `4_etl_daily_load.sql` to match your machine
4. Configure a Database Mail profile named `DW_Mail_Profile`
5. The SQL Agent Job will run the full pipeline daily at 2:00 AM

---

## Technologies Used

- **SQL Server** (T-SQL)
- **SQL Server Management Studio (SSMS)**
- **SQL Server Agent** — job scheduling
- **Database Mail** — email notifications
- **BULK INSERT** — CSV ingestion
