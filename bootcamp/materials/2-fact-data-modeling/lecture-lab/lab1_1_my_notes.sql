/* In this lab we look at the game_details table. What should be unique to every
 row is the game_id, team_id, and player_id. Note that a player can play for more
 than one team. */
/* First, we check for duplicates. */
-- select game_id,
--     team_id,
--     player_id,
--     count(1)
-- from game_details
-- group by 1,
--     2,
--     3
-- HAVING count(1) > 1;
/* ---------------------------------------------------------------------------*/
/* Second, get rid of them. */
/* deduped adds to game_details a column which is the row_num. In case of a row
 duplicated N times, the row_num will be 1 in the first row,..., and
 N in the Nth row. */
-- with deduped AS (
--     SELECT *,
--         ROW_NUMBER() OVER (
--             PARTITION BY game_id,
--             team_id,
--             player_id
--         ) as row_num
--     from game_details
-- )
-- SELECT * -- here we get rid of duplicated rows
-- from deduped
-- where row_num = 1;
/* ---------------------------------------------------------------------------*/
/* Third, we add the time of the games from the games table (the WHEN)
 and choose the columns we are interested in. */
-- with deduped AS (
--     SELECT g.game_date_est,
--         g.season,
--         g.home_team_id,
--         gd.*,
--         ROW_NUMBER() OVER (
--             PARTITION BY gd.game_id,
--             gd.team_id,
--             gd.player_id
--             ORDER BY g.game_date_est -- doesn't make sense here because each game has only ONE date.
--         ) as row_num
--     from game_details gd
--         join games g on gd.game_id = g.game_id
--     where g.game_date_est = '2016-10-04' -- this is just so the query doesn't take much time.
-- )
-- SELECT game_date_est,
--     -- game_id,
--     season,
--     team_id,
--     team_id = home_team_id AS dim_is_playing_at_home,
--     player_id,
--     player_name,
--     start_position,
--     COALESCE(POSITION('DNP' in comment), 0) > 0 as dim_did_not_play,
--     COALESCE(POSITION('DND' in comment), 0) > 0 as dim_did_not_dress,
--     COALESCE(POSITION('NWT' in comment), 0) > 0 as dim_not_with_team,
--     -- comment,
--     CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS minutes,
--     -- min,
--     fgm,
--     fga,
--     fg3m,
--     fg3a,
--     ftm,
--     fta,
--     oreb,
--     dreb,
--     reb,
--     ast,
--     stl,
--     blk,
--     "TO" as turnovers,
--     pf,
--     pts,
--     plus_minus
-- from deduped
-- where row_num = 1;
/* ---------------------------------------------------------------------------*/
/* Fourth, create the fct_game_details table */
-- CREATE TABLE fct_game_details (
--     dim_game_date DATE,
--     dim_season INTEGER,
--     dim_team_id INTEGER,
--     dim_player_id INTEGER,
--     dim_player_name TEXT,
--     dim_start_position TEXT,
--     dim_is_playing_at_home BOOLEAN,
--     dim_did_not_play BOOLEAN,
--     dim_did_not_dress BOOLEAN,
--     dim_not_with_team BOOLEAN,
--     m_minutes REAL,
--     -- m for measure
--     m_fgm INTEGER,
--     m_fga INTEGER,
--     m_fg3m INTEGER,
--     m_fg3a INTEGER,
--     m_ftm INTEGER,
--     m_fta INTEGER,
--     m_oreb INTEGER,
--     m_dreb INTEGER,
--     m_reb INTEGER,
--     m_ast INTEGER,
--     m_stl INTEGER,
--     m_blk INTEGER,
--     m_turnovers INTEGER,
--     m_pf INTEGER,
--     m_pts INTEGER,
--     m_plus_minus INTEGER,
--     PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
-- )
/* ---------------------------------------------------------------------------*/
/* FIFTH STEP: populate the fct_game_details table using the deduped query. */
-- INSERT INTO fct_game_details with deduped AS (
--         SELECT g.game_date_est,
--             g.season,
--             g.home_team_id,
--             gd.*,
--             ROW_NUMBER() OVER (
--                 PARTITION BY gd.game_id,
--                 gd.team_id,
--                 gd.player_id
--                 ORDER BY g.game_date_est -- doesn't make sense here because each game has only ONE date.
--             ) as row_num
--         from game_details gd
--             join games g on gd.game_id = g.game_id
--     )
-- SELECT game_date_est AS dim_game_date,
--     season AS dim_season,
--     team_id AS dim_team_id,
--     player_id AS dim_player_id,
--     player_name AS dim_player_name,
--     start_position AS dim_start_position,
--     team_id = home_team_id AS dim_is_playing_at_home,
--     COALESCE(POSITION('DNP' in comment), 0) > 0 as dim_did_not_play,
--     COALESCE(POSITION('DND' in comment), 0) > 0 as dim_did_not_dress,
--     COALESCE(POSITION('NWT' in comment), 0) > 0 as dim_not_with_team,
--     CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
--     fgm as m_fgm,
--     fga as m_fga,
--     fg3m as m_fg3m,
--     fg3a as m_fg3a,
--     ftm as m_ftm,
--     fta as m_fta,
--     oreb as m_oreb,
--     dreb as m_dreb,
--     reb as m_reb,
--     ast as m_ast,
--     stl as m_stl,
--     blk as m_blk,
--     "TO" as m_turnovers,
--     pf as m_pf,
--     pts as m_pts,
--     plus_minus as m_plus_minus
-- from deduped
-- where row_num = 1;
/* ---------------------------------------------------------------------------*/
/* SIXTH STEP: Bring in infromation from the teams table by joining it with fct_game_details */
-- select t.*,
--     gd.*
-- from fct_game_details gd
--     join teams t on t.team_id = gd.dim_team_id;
/* ---------------------------------------------------------------------------*/
/* SEVENTH STEP: Let us find the player who bailed out on most games */
-- select dim_player_name,
--     count(1) as num_games,
--     count(
--         case
--             when dim_not_with_team then 1
--         END
--     ) as bailed_num,
--     CAST(
--         COUNT(
--             CASE
--                 when dim_not_with_team then 1
--             END
--         ) as REAL
--     ) / COUNT(1) as bail_pct -- percentage
-- from fct_game_details
-- GROUP BY 1
-- ORDER BY 4 DESC;
/* ---------------------------------------------------------------------------*/
/* EIGHTS STEP: Let us compare the number of points a player makes when they
 play at home and not at home.*/
select dim_player_name,
    dim_is_playing_at_home,
    count(1) as num_games,
    SUM(m_pts) as total_points,
    count(
        case
            when dim_not_with_team then 1
        END
    ) as bailed_num,
    CAST(
        COUNT(
            CASE
                when dim_not_with_team then 1
            END
        ) as REAL
    ) / COUNT(1) as bail_pct -- percentage
from fct_game_details
GROUP BY 1,
    2
ORDER BY 6 DESC;
/* Example player: Elliot Williams bailed out on all games played not at home.*/