SELECT * FROM game_details

WITH ranked_games as(
    SELECT
        game_id,
        team_id,
        player_id,
        start_position,
        "comment",
        "min",
        fgm,
        fga,
        fg_pct,
        fg3m,
        fg3a,
        fg3_pct,
        ftm,
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
        plus_minus,
        RANK() OVER (PARTITION BY game_id, team_id, player_id) as rn -- Ranking the games to filter out duplicates on a later step
    FROM game_details
)
-- ONLY selecting the non redundant terms
SELECT
    game_id,
    team_id,
    player_id,
    start_position,
    "comment",
    "min",
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
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
    plus_minus
FROM ranked_games
WHERE rn = 1