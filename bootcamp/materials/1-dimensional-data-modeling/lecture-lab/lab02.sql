create table players_scd_table
(
    player_name text,
    scorer_class scoring_class,
    is_active boolean,
    current_season integer,
    start_season integer,
    end_season integer,
    primary key (player_name, start_season)

);



insert into players_scd_table
with with_previous as (
    select
        player_name,
        current_season,
        scorer_class,
        is_active,
        lag(scorer_class, 1) over (
            partition by player_name
            order by current_season
        ) as previous_scorer_class,
        lag(is_active, 1) over (
            partition by player_name
            order by current_season
        ) as previous_is_active
    from players where current_season <= 2021
),

with_indicators as (
    select
        *,
        case
            when is_active <> previous_is_active then 1
            when scorer_class <> previous_scorer_class then 1
            else 0
        end as change_indicator
    from with_previous
),

with_streaks as (
    select
        *,
        sum(change_indicator)
            over (partition by player_name order by current_season)
        as streak_identifier
    from with_indicators
)

select
    player_name,
    scorer_class,
    is_active,

    2021 as current_season,

    min(current_season) as start_season,
    max(current_season) as end_season

from with_streaks

group by player_name, streak_identifier, is_active, scorer_class
order by player_name;
