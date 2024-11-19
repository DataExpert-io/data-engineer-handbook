WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 1997

),

this_season AS (
    SELECT * FROM player_seasons
    WHERE season = 1998
)

INSERT INTO players
SELECT
    COALESCE(ls.player_name, ts.player_name) AS player_name,
    COALESCE(ls.height, ts.height) AS height,
    COALESCE(ls.college, ts.college) AS college,
    COALESCE(ls.country, ts.country) AS country,
    COALESCE(ls.draft_year, ts.draft_year) AS draft_year,
    COALESCE(ls.draft_round, ts.draft_round) AS draft_round,
    COALESCE(ls.draft_number, ts.draft_number)
    AS draft_number,
    COALESCE(
        ls.seasons,
        ARRAY[]::season_stats []
    ) || CASE
        WHEN ts.season IS NOT NULL
            THEN
                ARRAY[ROW(
                    ts.season,
                    ts.pts,
                    ts.ast,
                    ts.reb, ts.weight
                )::season_stats]
        ELSE ARRAY[]::season_stats []
    END
    AS seasons,
    CASE
        WHEN ts.season IS NOT NULL
            THEN
                (CASE
                    WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad'
                END)::scoring_class
        ELSE ls.scorer_class
    END AS scorer_class,
    ts.season IS NOT NULL AS is_active,
    1998 AS current_season,
    (current_season - ts.season) AS years_since_last_active

FROM last_season AS ls
FULL OUTER JOIN this_season AS ts
    ON ls.player_name = ts.player_name;
