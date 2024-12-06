WITH last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
      AND end_season = 2021
),
historical_scd AS (
    SELECT
         player_name,
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
unchanged_records AS (
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
        AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT
        ts.player_name,
        unnest(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) as records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
    WHERE (
        ts.scoring_class <> ls.scoring_class
        OR ts.is_active <> ls.is_active
    )
),
unnested_changed_records as (
    select
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
        from changed_records
),
new_records as (
    select
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season as start_season,
        ts.current_season as end_season
    from this_season_data ts
    left join last_season_scd ls
        on ts.player_name=ls.player_name
    where ls.player_name is null
)
SELECT * FROM historical_scd

union all

SELECT * FROM unchanged_records

union all

SELECT * FROM unnested_changed_records

union all

SELECT * FROM new_records;
