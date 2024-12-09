-- select *
--  from player_seasons
 
 create type season_stats as (
 	season integer,
	 gp integer,
	 pts real,
	 reb real,
	 ast real
 )
 
 
 create table players (
 	player_name text,
	 height  text,
	 college text,
	 country text, 
	 draft_year text,
	 draft_round text,
	 draft_number text,
	 season_stats season_stats[],
	 current_season integer,
	 primary key (player_name, current_season)
 )


/*
	yesterday: holding schema of the cumulated table schema
	today: holding schema of the source table that will be added into the cumulated table.
*/

insert into players
with yesterday as (
	select * from players 
	where current_season = 2000
),
today as (
	select * from player_seasons
	where season = 2001
)

select
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case 
	-- if yesterday table has no season_stats data
		when y.season_stats is null then array[row(t.season,t.gp,t.pts,t.reb,t.ast)::season_stats]
		-- if yesterday table has data, and also today table has season_stats data
		when t.season is not null then y.season_stats || array[row(t.season,t.gp,t.pts,t.reb,t.ast)::season_stats]
		-- if today has no data
		else y.season_stats
	end as season_stats,
	coalesce(t.season,y.current_season + 1) as current_season
	

from today t Full Outer Join yesterday y
on t.player_name = y.player_name 

/*
	unnest array of structs column
*/

-- with cte as (
-- select 
-- 	player_name,
-- 	unnest(season_stats) as season_stats
-- from players
-- where current_season = 2001
-- -- and player_name = 'Michael Jordan'
-- )

-- select player_name,(season_stats::season_stats).*
-- from cte


/*

*/




