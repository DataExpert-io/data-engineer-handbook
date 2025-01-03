
with home_teams as ( -- first I want to have a line per team, game 
	select
		game_date_est as game_date
		, home_team_id as team_id
		, home_team_wins::boolean as has_won
	from
		games
)

, visitors as (
	select
		game_date_est as game_date
		, visitor_team_id as team_id
		, (not home_team_wins::boolean) as has_won
	from
		games
)

, unioned as (
	select * from home_teams
	union
	select * from visitors
)

, final as (
	select
		game_date
		, team_id
		, has_won
		, sum(has_won::integer) over (partition by team_id order by game_date asc rows 89 preceding) as nb_won_last_90 -- 89 last + current match makes 90 game stretch
	from
		unioned
)

select * from final order by nb_won_last_90 desc


