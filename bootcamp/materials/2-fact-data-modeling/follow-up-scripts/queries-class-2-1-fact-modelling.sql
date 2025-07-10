SELECT * FROM game_details LIMIT 100;

SELECT
  game_id, team_id, player_id, COUNT(*)
FROM game_details
GROUP BY game_id, team_id, player_id
HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC;


WITH game_details_deduped AS (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS rn
  FROM game_details
)
SELECT * FROM game_details_deduped WHERE rn = 1;

-- game details observation: very denormalized table, but it's not a fact table, as it don't have the
-- "when" condition of a fact table - the when is contained in "games" table

SELECT * FROM games LIMIT 100;

WITH game_details_deduped AS (
  SELECT g.game_date_est, gd.*,
         ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS rn
  FROM game_details gd
  JOIN games g ON gd.game_id = g.game_id
)
SELECT * FROM game_details_deduped WHERE rn = 1;


WITH game_details_deduped AS (
  SELECT g.game_date_est, g.season, g.home_team_id,
         gd.*,
         ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS rn
  FROM game_details gd
         JOIN games g ON gd.game_id = g.game_id
)
SELECT game_date_est, season, team_id, team_id = home_team_id AS dim_playing_home,
       player_id, player_name, start_position, comment,
       COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_didnt_play,
       COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_didnt_dress,
       COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_w_team,
       CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS minutes,
       min, fgm, fga, fg3m, fg3a,
       ftm, fta, oreb, dreb, reb, ast, stl, blk, "TO" as turnovers, pf, pts, plus_minus
FROM game_details_deduped WHERE rn = 1;


CREATE TABLE fact_game_detail (
  dim_game_date DATE,
  dim_season INTEGER,
  dim_team_id INTEGER,
  dim_player_id INTEGER,
  dim_player_name TEXT,
  dim_start_position TEXT,
  dim_playing_home BOOLEAN,
  dim_didnt_play BOOLEAN,
  dim_didnt_dress BOOLEAN,
  dim_not_w_team BOOLEAN,
  m_minutes REAL,
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
  m_plus_minus INTEGER,
  PRIMARY KEY (dim_game_date, dim_player_id)
);

INSERT INTO fact_game_detail
WITH game_details_deduped AS (
  SELECT g.game_date_est, g.season, g.home_team_id,
         gd.*,
         ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS rn
  FROM game_details gd
         JOIN games g ON gd.game_id = g.game_id
)
SELECT game_date_est AS dim_game_date, season AS dim_season,
       team_id AS dim_team_id, player_id AS dim_player_id, player_name AS dim_player_name,
       start_position AS dim_start_position, team_id = home_team_id AS dim_playing_home,
       COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_didnt_play,
       COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_didnt_dress,
       COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_w_team,
       CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
       fgm AS m_fgm, fga AS m_fga, fg3m AS m_fg3m, fg3a AS m_fg3a, ftm AS m_ftm, fta AS m_fta,
       oreb AS m_oreb, dreb AS m_dreb, reb AS m_reb,
       ast AS m_ast, stl AS m_stl, blk AS m_blk, "TO" as m_turnovers,
       pf AS m_pf, pts AS m_pts, plus_minus AS m_plus_minus
FROM game_details_deduped WHERE rn = 1;

SELECT * FROM fact_game_detail;

SELECT *
FROM fact_game_detail fgd
JOIN teams t ON t.team_id = fgd.dim_team_id;


SELECT dim_player_name,
COUNT(*) AS num_games,
COUNT(CASE WHEN dim_not_w_team THEN 1 END) AS bailed_num,
CAST(COUNT(CASE WHEN dim_not_w_team THEN 1 END) AS REAL) / COUNT(*) AS bail_pct
FROM fact_game_detail fgd
GROUP BY dim_player_name
ORDER BY 4 DESC;


SELECT dim_player_name, dim_playing_home,
       COUNT(*) AS num_games,
       SUM(m_pts) AS total_pts,
       COUNT(CASE WHEN dim_not_w_team THEN 1 END) AS bailed_num,
       CAST(COUNT(CASE WHEN dim_not_w_team THEN 1 END) AS REAL) / COUNT(*) AS bail_pct
FROM fact_game_detail fgd
GROUP BY dim_player_name, dim_playing_home
ORDER BY 4 DESC;

-- conclusions: fact table like that very good to answer questions in a fast way,
-- consider multiple aggregations, very powerful