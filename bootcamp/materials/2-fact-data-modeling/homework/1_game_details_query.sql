-- SELECT game_id, team_id, player_id, COUNT(1)
-- FROM game_details
-- GROUP BY 1,2,3 
-- HAVING COUNT(1) > 1;

WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details
)
SELECT
    game_id, 
    team_id, 
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    comment,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO",
    pf,
    pts,
    plus_minus) 
FROM ranked
WHERE row_num = 1; 