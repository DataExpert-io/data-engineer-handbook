-- required for changed_records CTE
CREATE TYPE players_scd_type AS
(
    scoring_class scoring_class,
    is_active     BOOLEAN,
    start_season  INTEGER,
    end_season    INTEGER
);

WITH last_season_scd AS (SELECT *
                         FROM players_scd
                         WHERE current_season = 2021
                           AND end_season = 2021),

     historical_scd AS (SELECT player_name,
                               scoring_class,
                               is_active,
                               start_season,
                               end_season
                        FROM players_scd
                        WHERE current_season = 2021
                          AND end_season < 2021),

     this_season_data AS (SELECT *
                          FROM players
                          WHERE current_season = 2022),

     unchanged_records AS (SELECT ts.player_name,
                                  ts.scoring_class,
                                  ts.is_active,
                                  ls.start_season,
                                  ts.current_season AS end_season
                           FROM this_season_data ts
                                    JOIN last_season_scd ls
                                         ON ts.player_name = ls.player_name
                           WHERE ts.scoring_class = ls.scoring_class
                             AND ts.is_active = ls.is_active),

     changed_records AS (SELECT ts.player_name,
                                UNNEST(ARRAY [
                                    ROW (ls.scoring_class, ls.is_active, ls.start_season, ls.end_season)::players_scd_type,
                                    ROW (ts.scoring_class, ts.is_active, ts.current_season, ts.current_season)::players_scd_type
                                    ]) AS records
                         FROM this_season_data ts
                                  LEFT JOIN last_season_scd ls
                                            ON ts.player_name = ls.player_name
                         WHERE ts.scoring_class <> ls.scoring_class
                            OR ts.is_active <> ls.is_active),

     unnested_changed_records AS (SELECT player_name,
                                         (records::players_scd_type).scoring_class,
                                         (records::players_scd_type).is_active,
                                         (records::players_scd_type).start_season,
                                         (records::players_scd_type).end_season
                                  FROM changed_records),

     new_records AS (SELECT ts.player_name,
                            ts.scoring_class,
                            ts.is_active,
                            ts.current_season AS start_season,
                            ts.current_season AS end_season
                     FROM this_season_data ts
                              LEFT JOIN last_season_scd ls
                                        ON ts.player_name = ls.player_name
                     WHERE ls.player_name IS NULL)

SELECT *
FROM historical_scd

UNION ALL

SELECT *
FROM unchanged_records

UNION ALL

SELECT *
FROM unnested_changed_records

UNION ALL

SELECT *
FROM new_records

ORDER BY player_name, start_season;


/*
This approach used SCD incremental build and is more efficient than backfill approach.
And its querying a lot less data and this will work way faster.
But, it also has a sequential problem, as now we are dependent on yesterday's data like historical_scd and then current.
That, make it a little bit harder to backfill and that part of this is annoying.

This query can have multiple edge case to consider like:
1. New players added in the current season.
2. Players who have retired and are no longer active.
3. Players whose attributes have changed multiple times within the same season.
4. Players who have rejoined after retirement.
Each of these cases needs to be handled appropriately to ensure the SCD table accurately reflects the state of each player across seasons.

also, what if there are NULLs in scoring_class or is_active columns in players table?
in that case, the comparison logic needs to be adjusted to handle NULL values correctly,
for eg- using IS DISTINCT FROM instead of <> for comparison in changed_records CTE.
*/