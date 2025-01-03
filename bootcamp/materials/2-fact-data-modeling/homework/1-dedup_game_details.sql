with dedup_row_num as (
	select 
		*
		, row_number() over (partition by game_id, player_id) as row_num
	from
		game_details
)

, dedup as (
	select
		game_id
		, team_id
		, team_abbreviation
		, team_city
		, player_id
		, player_name
		, nickname
		, start_position
		, comment
		, min
		, fgm
		, fga
		, fg_pct
		, fg3m
		, fg3a
		, fg3_pct
		, ftm
		, fta
		, ft_pct
		, oreb
		, dreb
		, reb
		, ast
		, stl
		, blk
		, "TO"
		, pf
		, pts
		, plus_minus
	from
		dedup_row_num
	where
		row_num = 1
)

select * from dedup;