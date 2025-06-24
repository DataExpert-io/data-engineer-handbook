SELECT player_name,
    seasons,
    (seasons [cardinality(seasons)]::season_stats).pts / CASE
        WHEN (seasons [1]::season_stats).pts = 0 THEN 1
        ELSE (seasons [1]::season_stats).pts
    END AS ratio_most_recent_to_first
FROM players
WHERE current_season = 1998;
/* Explanation: For each player we compare their points in their
 last year to their first year to see how they developed.
 Apparently, we start counting from 1 and not from 0. */
/* Let us see the stars */
SELECT player_name,
    seasons,
    (seasons [cardinality(seasons)]::season_stats).pts / CASE
        WHEN (seasons [1]::season_stats).pts = 0 THEN 1
        ELSE (seasons [1]::season_stats).pts
    END AS ratio_most_recent_to_first
FROM players
WHERE current_season = 2001
    and scoring_class = 'star';