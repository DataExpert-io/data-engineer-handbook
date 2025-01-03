-- Answers who scored the most points playing for one team?
with player_team_agg as (
	select
		*
	from
		grouping_set_table
	where
		agg_level = 'player_name__team_abbreviation'
)

select * from player_team_agg where total_points = (select max(total_points) from player_team_agg)
