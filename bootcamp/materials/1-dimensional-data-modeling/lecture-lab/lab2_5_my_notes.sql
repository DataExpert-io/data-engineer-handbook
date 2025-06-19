/* This is the scd generation query used in the lab*/
WITH with_previous AS (
    select player_name,
        current_season,
        scoring_class,
        is_active,
        LAG(scoring_class, 1) OVER (
            PARTITION BY player_name
            ORDER BY current_season
        ) as previous_scoring_class,
        LAG(is_active, 1) OVER (
            PARTITION BY player_name
            ORDER BY current_season
        ) as previous_is_active
    from players
),
with_indicators AS (
    select *,
        CASE
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    from with_previous
),
with_streaks AS (
    select *,
        SUM(change_indicator) OVER(
            PARTITION BY player_name
            ORDER BY current_season
        ) AS streak_identifier
    from with_indicators
)
select player_name,
    scoring_class,
    is_active,
    --streak_identifier,
    MIN(current_season) as start_season,
    MAX(current_season) as end_season
from with_streaks
group by player_name,
    streak_identifier,
    is_active,
    scoring_class
order by player_name,
    streak_identifier;
/* In order to prepare for the incremental scd table, let us
 filter this table with the condition: current_season <=2021 */
INSERT into players_scd WITH with_previous AS (
        select player_name,
            current_season,
            scoring_class,
            is_active,
            LAG(scoring_class, 1) OVER (
                PARTITION BY player_name
                ORDER BY current_season
            ) as previous_scoring_class,
            LAG(is_active, 1) OVER (
                PARTITION BY player_name
                ORDER BY current_season
            ) as previous_is_active
        from players
        where current_season <= 2021
    ),
    with_indicators AS (
        select *,
            CASE
                WHEN scoring_class <> previous_scoring_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END AS change_indicator
        from with_previous
    ),
    with_streaks AS (
        select *,
            SUM(change_indicator) OVER(
                PARTITION BY player_name
                ORDER BY current_season
            ) AS streak_identifier
        from with_indicators
    )
select player_name,
    scoring_class,
    is_active,
    --streak_identifier,
    MIN(current_season) as start_season,
    MAX(current_season) as end_season,
    2021 AS current_season
from with_streaks
group by player_name,
    streak_identifier,
    is_active,
    scoring_class
order by player_name,
    streak_identifier;