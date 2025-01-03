with dedups_rn as (
	select
		*
		, row_number() over (partition by game_id, player_id) as row_num
	from
		game_details
	where
		comment is null -- comment is not null only when player did not play
)

, dedups as (
	select
		player_name
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
		, g.game_date_est
		, d.pts
	from
		dedups as d
	inner join -- inner to ensure non nullity again
		games as g
		on d.game_id = g.game_id
)

, streaks as (
	-- contructucting a streak -> as long as the player scores more than 10 pts, it stays in the same group
	select
		*
		, row_number() over (partition by player_name order by game_date_est asc)
			- sum(case when pts > 10 then 1 else 0 end) over (partition by player_name order by game_date_est asc)
			as streak_id
		, pts > 10 as has_scored_above_10 	--need this to filter and avoid counting rows without above 10 points 
											-- -> streaks that finishes as soon as they begin when player does not scores over 10
	from
		add_infos
)

, final as (
	select
		player_name
		, streak_id
		, min(game_date_est) as streak_start
		, max(game_date_est) as streak_end
		, count(*) as nb_games_over_10_pts
	from
		streaks
	where
		has_scored_above_10
	group by
		player_name
		, streak_id
)

, max_streaks as (
	select
		player_name
		, max(nb_games_over_10_pts) as max_streak_over_10
	from
		final
	group by
		player_name
)

select * from max_streaks
