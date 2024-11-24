
insert into players_scd
with with_previous as (
	select 
		player_name,
		scorer_class,
		is_active,
		lag(scorer_class,1) over (partition by player_name order by current_season) as previous_scorer_class,
		lag(is_active,1) over (partition by player_name order by current_season) as previous_is_active,
		current_season
	from players
	where current_season <= 2021
),

with_change_indicator as (
	select 
		*,
		case
			when scorer_class <> previous_scorer_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as change_indicator
	from with_previous
),

with_streak_identifier as (
	select
		*,
		sum(change_indicator) over (partition by player_name order by current_season) as streak_identifier
	from with_change_indicator
)

select 
	player_name, scorer_class, is_active,
	min(current_season) as start_season,
	max(current_season) as end_season,
	2021 as current_season
from with_streak_identifier
group by player_name, scorer_class, is_active,streak_identifier
order by player_name, streak_identifier



