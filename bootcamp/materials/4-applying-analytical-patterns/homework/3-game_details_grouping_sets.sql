create table grouping_set_table as 

with dedups_rn as (
	select
		*
		, row_number() over (partition by game_id, player_name) as row_num
	from
		game_details
	where -- not necessary but ensuring non nullity for grouping under
		player_name is not null
		and game_id is not null
		and team_id is not null
		
)

, dedups as (
	select
		player_name
		, team_id
		, team_abbreviation
		, game_id
		, pts
	from
		dedups_rn
    where
        row_num = 1
)

, add_infos as (
	select
		d.player_name
		, d.team_abbreviation
		, g.season
		, case
			when d.team_id = g.home_team_id and g.home_team_wins::boolean then 1
			when d.team_id = g.visitor_team_id and not g.home_team_wins::boolean then 1
			else 0
		end as has_won
		
		, d.pts
	from
		dedups as d
	inner join -- inner to ensure non nullity again
		games as g
		on d.game_id = g.game_id
	where
		season is not null
)

, final as (
	select
		case
			when grouping(player_name) = 0 
				and grouping(team_abbreviation) = 0
				then 'player_name__team_abbreviation'
			
			when grouping(player_name) = 0 
				and grouping(season) = 0
				then 'player_name__season'
			
			when
				grouping(team_abbreviation) = 0
				then 'team_abbreviation'
		end as agg_level
		
		, coalesce(player_name::varchar, '(overall)') as player_name
		, coalesce(team_abbreviation::varchar, '(overall)') as team_abbreviation
		, coalesce(season::varchar, '(overall)') as season
		, coalesce(count(1), 0) as nb_games
		, sum(has_won) as nb_games_won
		, coalesce(sum(pts), 0) as total_points
	from
		add_infos
	group by grouping sets (
		( player_name, team_abbreviation )
		, ( player_name, season )
		, ( team_abbreviation )
		
	)
)

select * from final