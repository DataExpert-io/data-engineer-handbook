# Day 1 - Lab

In this lab, we will work mostly with `game_details` table.

```sql
SELECT * FROM game_details;
```

This table is quite terrible.

When you’re working with fact data, the **grain** of the data matters a lot. The grain is considered the lowest common denominator, the unique ID of the table.

For `game_details`, the grain is

```sql
SELECT
    game_id, team_id, player_id, count(1)
FROM game_details
GROUP BY 1, 2, 3
HAVING count(1) > 1
```

You can see that we actually have a lot of dupes. So we want to create a filter here to get rid of them.

```sql
-- this is gonna be our start query we will work with
WITH deduped AS (
    SELECT
        *, ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details
)

SELECT * FROM deduped
WHERE row_num = 1;
```

One things about this fact data is that it’s very denormalized, and probably a lot of the things here aren’t really necessary. At the same time, there are also missing columns. Remember from Lecture 1, facts need a **when**, and there’s no when here at all.

The **when** column we get it from `game_id`.

```sql
WITH deduped AS (
    SELECT
        *,
      g.game_date_est,
      ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
      JOIN games g ON gd.game_id = g.game_id
)
SELECT * FROM deduped
WHERE row_num = 1;
```

> Note: regarding unnecessary data, look that we have both `team_abbreviation` and `team_city` in here. But `team`s is never going to be big data, even in the next 250 years. So having those two columns in an abomination, as you can easily join the `teams` table at any given time.
>

Game however, is different, as `games` is gonna grow much more than `teams`. If we have to join it every time, it will become very slow, especially over time.

Let’s select and parse just the columns we care about.

```sql
WITH deduped AS (
    SELECT
      g.game_date_est,
      g.season,
      g.home_team_id,  *,
      ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
      JOIN games g ON gd.game_id = g.game_id
)

SELECT -- we don't put `game_id` because we already pull the needed info from `games`
    game_date_est,
  season,
  team_id,
  team_id = home_team_id AS dim_is_playing_at_home,
  player_id,
  player_name,
  start_position,
  -- the comment column is quite high in cardinality
  -- so we parse the most important situations
  COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
  COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
  COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_did_not_with_team,
  -- minutes were a string like "12:56:, so we transformed it
  -- into a proper decimal number
  CAST(SPLIT_PART(min, ':', 1) AS REAL)
      + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS minutes,
  fgm, -- some basketball jargon 👇
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

Let’s now create the DDL for the above data

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

One thing with **fact data**, is that a lot of the times you want to label the columns either as **measures** or **dimensions**. See next.

</aside>

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
    m_minutes REAL, -- `m_` prefix stands for `measure`, to distinguish from `dim_`
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
    -- team_id is a bit redundant but we add to the PK cause of indexing reasons
    -- (in postgres case in specific)
)
```

Using the `dim_` and `m_` naming convention is useful because it indicates that `dim_` are columns you should do group by on and filter on, whereas `m_` are columns that you should aggregate and do math on.

Now we take the previous `SELECT` query and insert the results in the table we just created:

```sql
INSERT INTO fct_game_details
WITH deduped AS (
    SELECT
      g.game_date_est,
      g.season,
      g.home_team_id,
      gd.*,
      ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
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
  COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
  COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
  COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_did_not_with_team,
  CAST(SPLIT_PART(min, ':', 1) AS REAL)
      + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
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
```

In a real setting, you should be changing the name of things, because sometimes the column names are terrible.

Let’s do some example analytics on this data.

```sql
-- query to see who has the highest bail % on games
SELECT
    dim_player_name,
  COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
  COUNT(CASE WHEN dim_not_with_team THEN 1 END) * 1.0 / COUNT(1) AS bailed_pct
FROM fct_game_details
GROUP BY 1
ORDER BY 3 DESC;
```
