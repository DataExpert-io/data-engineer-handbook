-- CREATE TYPE scd_type AS (
--     scoring_class scoring_class,
--     is_active boolean,
--     start_season INTEGER,
--     end_season INTEGER
-- );
WITH last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
        AND end_season = 2021
),
/* this table has rows from players_scd where current_season = 2021
 where there are streaks that ended before 2021. We keep those records
 as they are. Unlike the players_scd table, historical_scd doesn't have
 a current_season column. current_season will be set to 2022 later.*/
historical_scd AS (
    SELECT player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2021
        AND end_season < 2021
),
this_season_data AS (
    SELECT *
    FROM players
    WHERE current_season = 2022
),
/* this_season_data might have players that already exist in last_season_scd
 and it might as well have new players. Here, we focus on already existing
 players whose status didn't change. */
unchanged_records AS (
    SELECT ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season as end_season -- which equals 2022
    FROM this_season_data ts
        JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
        AND ts.is_active = ls.is_active
),
/* In this table and in the following table we will do a LEFT JOIN.
 In this table, we focus on already existing players whose status DID change. */
changed_records AS (
    SELECT ts.player_name,
        UNNEST(
            ARRAY [ -- those are the values from the last_season_scd
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW( -- those are the values from this season
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]
        ) as records
    FROM this_season_data ts
        LEFT JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE (
            ts.scoring_class <> ls.scoring_class
            OR ts.is_active <> ls.is_active
        )
        /* The result of the left join does NOT include players who didn't exist in the
         last_season_scd table. The reason is, remembering that ls.scoring_class equals NULL, that
         when comparing any value to NULL using <> (or any other operator), the result is NULL and
         not TRUE or False. The condition inside the WHERE clause should be TRUE in order for the
         rows to be included in the result.
         */
        /* The UNNEST ARRAY operation expands the array into a set of rows.
         The result of this query has two rows for each player; one with their previous
         streak and one for their streak from 2022.
         */
),
unnested_changed_records AS (
    SELECT player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    FROM changed_records
),
/* This table has info on new players that didn't exist in the last_season_scd table.
 Example: Max Christie*/
new_records AS (
    SELECT ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
        LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT *,
    2022 AS current_season
FROM (
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
    ) as final