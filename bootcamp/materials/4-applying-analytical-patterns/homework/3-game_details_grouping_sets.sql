with dedups_rn as (
	select
		*
		, row_number() over (partition by game_id, player_id) as row_num
	from
		game_details
	where -- not necessary but ensuring non nullity for grouping under
		player_id is not null
		and game_id is not null
		and team_id is not null
		
)

, dedups as (
	select
		player_id
		, team_id
		, game_id
		, pts
	from
		dedups_rn
    where
        row_num = 1
)

, add_infos as (
	select
		d.player_id
		, d.team_id
		, g.season
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
			when grouping(player_id) = 0 
				and grouping(team_id) = 0
				then 'player_id__team_id__season'
			
			when grouping(player_id) = 0 
				and grouping(season) = 0
				then 'player_id__season'
			
			when
				grouping(team_id) = 0
				then 'team_id'
		end as agg_level
		
		, coalesce(player_id::varchar, '(overall)') as player_id
		, coalesce(team_id::varchar, '(overall)') as team_id
		, coalesce(season::varchar, '(overall)') as season
		, coalesce(sum(pts), 0) as total_points
	from
		add_infos
	group by grouping sets (
		( player_id, team_id )
		, ( player_id, season )
		, ( team_id )
		
	)
)

select * from final 