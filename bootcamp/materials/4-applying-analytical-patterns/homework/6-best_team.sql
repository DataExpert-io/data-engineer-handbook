-- Answers which team has won the most games?

with team_agg as (
	select
		*
	from
		grouping_set_table
	where
		agg_level = 'team_abbreviation'
)

select * from team_agg where nb_games_won = (select max(nb_games_won) from team_agg)
