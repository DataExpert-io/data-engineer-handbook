 SELECT player_name,
        (season_stats[cardinality(season_stats)]::season_stats).pts/
         CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
             ELSE  (season_stats[1]::season_stats).pts END
            AS ratio_most_recent_to_first
 FROM players
 WHERE current_season = 1998;