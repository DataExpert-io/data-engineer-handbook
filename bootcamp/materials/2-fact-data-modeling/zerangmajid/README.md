
# NBA Game Data Analysis

## Overview
This project focuses on analyzing NBA game data, including deduplication, fact table creation, and advanced SQL analytics for player and team performance insights.

 
---


## 1. Deduplication Process
### Objective
The deduplication process ensures that duplicate records in the `game_details` table are removed based on `game_id`, `team_id`, and `player_id`.

### Steps
1. **Identify Duplicates:** Use `GROUP BY` and `COUNT` to find records with more than one occurrence.
2. **Assign Row Numbers:** Utilize the `ROW_NUMBER` function to assign unique numbers to each duplicate record.
3. **Filter:** Retain only the first occurrence of each record and eliminate the rest.

### SQL Example
```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
    FROM game_details
)
SELECT *
FROM deduped
WHERE row_num = 1;
```
## Advanced Deduplication Query
### Query Example
```sql
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        g.visitor_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    game_date_est,
    season,
    team_id,
    team_id = home_team_id AS dim_is_playing_at_home,
    player_id,
    player_name,
    start_position,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" AS turnovers,
    pf,
    pts,
    plus_minus
FROM deduped
WHERE row_num = 1;
```


## Splitting Time and Deduplication
### Objective
Deduplicate records and split time fields into fractional minutes for enhanced analysis.

### SQL Example
```sql
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        g.visitor_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus
FROM deduped
WHERE row_num = 1;
```

---
---

## 2. Fact Table Creation
### Objective
Create a structured fact table (`fct_game_details`) that organizes game data into measures and dimensions for efficient analysis.

### Fact Table Columns
- **Dimensions:**
  - `dim_game_date`: Date of the game.
  - `dim_season`: Season of the game.
  - `dim_team_id`: Team identifier.
  - `dim_player_id`: Player identifier.
  - `dim_player_name`: Player name.
  - `dim_start_position`: Player's starting position.
  - `dim_is_playing_at_home`: Boolean indicating if the player’s team is the home team.
  - `dim_did_not_play`, `dim_did_not_dress`, `dim_not_with_team`: Boolean flags derived from comments.

- **Measures:**
  - `m_minutes`: Minutes played (calculated as fractional time).
  - `m_pts`: Points scored.
  - `m_reb`, `m_ast`, `m_blk`: Performance metrics (rebounds, assists, blocks, etc.).

### SQL Example
```sql
CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_pts INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);
```



## Inserting Data into the Fact Table
### Objective
Insert deduplicated and transformed data into a structured fact table for better analysis.

### SQL Example
```sql
INSERT INTO fct_game_details
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        g.visitor_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- Verify data insertion
SELECT * FROM fct_game_details;
---

## 3. Analytical Queries
### Objective
Gain insights into player and team performance using aggregated metrics from the fact table.

### Example Queries

#### 1. Players Who Missed the Most Games
```sql
SELECT
    dim_player_name,
    COUNT(1) AS num_games,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS missed_games,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS miss_ratio
FROM fct_game_details
GROUP BY dim_player_name
ORDER BY miss_ratio DESC;
```

#### 2. Analyze Home vs. Away Performance
```sql
SELECT
    dim_player_name,
    dim_is_playing_at_home,
    COUNT(1) AS num_games,
    SUM(m_pts) AS total_points,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS missed_games,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS miss_ratio
FROM fct_game_details
GROUP BY dim_player_name, dim_is_playing_at_home
ORDER BY miss_ratio DESC;
```

#### 3. Player Performance Metrics
```sql
SELECT
    dim_player_name,
    SUM(m_pts) AS total_points,
    SUM(m_ast) AS total_assists,
    SUM(m_reb) AS total_rebounds,
    AVG(m_plus_minus) AS avg_plus_minus
FROM fct_game_details
GROUP BY dim_player_name
ORDER BY total_points DESC;
```
 
 
 

## Conclusion
This project demonstrates how to organize, deduplicate, and analyze NBA game data efficiently. The SQL scripts can be adapted to various other datasets with similar structures.
