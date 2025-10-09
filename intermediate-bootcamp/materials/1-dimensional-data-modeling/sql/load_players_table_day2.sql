WITH first_season AS (
    SELECT player_name, MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
),
player_timeline AS (
    SELECT fs.player_name, gs.season
    FROM first_season fs
    JOIN LATERAL generate_series(fs.first_season, 2022) AS gs(season) ON true
),
season_stats_array AS (
    SELECT
        pt.player_name,
        ARRAY_AGG(
            ROW(ps.season, ps.gp, ps.pts, ps.reb, ps.ast)::season_stats
            ORDER BY ps.season  
        ) AS seasons
    FROM player_timeline pt
    LEFT JOIN player_seasons ps
        ON pt.player_name = ps.player_name AND pt.season = ps.season
    GROUP BY pt.player_name
),
latest_season AS (
    SELECT DISTINCT ON (player_name)
        player_name,
        season,
        pts
    FROM player_seasons
    ORDER BY player_name, season DESC
),
static_info AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
INSERT INTO players (
    player_name,
    height,
    college,
    country,
    draft_year,
    draft_round,
    draft_number,
    season_stats,
    scoring_class,
    years_since_last_active,
    is_active,
    season
)
SELECT
    s.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    sa.seasons,
    CASE
        WHEN ls.pts > 20 THEN 'star'
        WHEN ls.pts > 15 THEN 'good'
        WHEN ls.pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class, 
    2022 - ls.season AS years_since_last_active,
    ls.season = 2022 AS is_active,
    2022
FROM static_info s
JOIN season_stats_array sa ON s.player_name = sa.player_name
JOIN latest_season ls ON s.player_name = ls.player_name;
