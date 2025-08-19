-- SELECT game_id, team_id, player_id, COUNT(1)
-- FROM game_details
-- GROUP BY 1,2,3 
-- HAVING COUNT(1) > 1;

SELECT 
    game_id, 
    team_id, 
    MAX(team_abbreviation),
    MAX(team_city),
    player_id,
    MAX(player_name),
    MAX(nickname),
    MAX(start_position),
    MAX(comment),
    MAX(min),
    MAX(fgm),
    MAX(fga),
    MAX(fg_pct),
    MAX(fg3m),
    MAX(fg3a),
    MAX(fg3_pct),
    MAX(ftm),
    MAX(fta),
    MAX(ft_pct),
    MAX(oreb),
    MAX(dreb),
    MAX(reb),
    MAX(ast),
    MAX(stl),
    MAX(blk),
    MAX("TO"),
    MAX(pf),
    MAX(pts),
    MAX(plus_minus)
FROM game_details
GROUP BY game_id, team_id, player_id; 