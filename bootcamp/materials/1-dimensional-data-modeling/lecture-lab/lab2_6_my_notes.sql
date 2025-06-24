/* let us understand the scd generation query */
/* STEP1: Understanding the with_previous table */
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
from players;
/* The result is the players table added to it two columns: previous_scoring_class
 and previous_is_active */
/*------------------------------------------------------*/
/* STEP2: the with_indicator table: adding the change indicator columns*/
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
)
select *,
    CASE
        WHEN scoring_class <> previous_scoring_class THEN 1
        ELSE 0
    END AS scoring_class_change_indicator,
    CASE
        WHEN is_active <> previous_is_active THEN 1
        ELSE 0
    END AS is_active_change_indicator
from with_previous;
/* This query adds to the with_previous table two columns which tells us if there has been a
 change in the is_active and scoring_class values. */
/*----------------------------------------------------*/
/* STEP3: simplifying the with_indicator table. It is easier to track the change of
 only one variable so we update the with_indicator table as follows: */
-- 1 in the LAG is offset to get the value from the previous row.
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
)
select *,
    CASE
        WHEN scoring_class <> previous_scoring_class THEN 1
        WHEN is_active <> previous_is_active THEN 1
        ELSE 0
    END AS change_indicator
from with_previous;
/*----------------------------------------------------*/
/*STEP4: Adding a streak_identifier */
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
)
select *,
    SUM(change_indicator) OVER(
        PARTITION BY player_name
        ORDER BY current_season
    ) AS streak_identifier
from with_indicators;
/* Very important note about the SUM() function: it computes
 the sums CUMULATIVELY up untill current_season. So streak_identifier tells us
 how many times the player changes up untill current_season. */
/*----------------------------------------------------*/
/*STEP5: Grouping by the streak_identifier to get start_season
 and end_season*/
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
    streak_identifier,
    is_active,
    scoring_class,
    MIN(current_season) as start_season,
    MAX(current_season) as end_season,
    2021 as current_season
from with_streaks
group by player_name,
    streak_identifier,
    is_active,
    scoring_class;
/* For a player, in each streak_identifier group, the values of is_active and
 scoring_class are the same since no change has been made (all rows had the 
 same streak_identifier). See players A.C. Green or Aaron Brooks as examples.*/
/*STEP6: small remarks:
 In the final query when we group by the streak identifier, we can
 add 2021 as the value for the current_season. It will be a fixed value.
 */