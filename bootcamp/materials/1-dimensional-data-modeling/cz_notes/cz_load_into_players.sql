select *
from player_seasons


-- create type season_stats as (
-- 	season integer,
-- 	gp real,
-- 	pts real,
-- 	reb real,
-- 	ast real
-- )


-- create type scorer_class as enum ('star','good','average','bad')


-- drop table if exists players;

--  CREATE TABLE players (
--      player_name TEXT,
--      height TEXT,
--      college TEXT,
--      country TEXT,
--      draft_year TEXT,
--      draft_round TEXT,
--      draft_number TEXT,
--      seasons season_stats[],
--      scorer_class scorer_class,
--      years_since_last_active integer,
--      current_season INTEGER,
--      is_active BOOLEAN,
--      PRIMARY KEY (player_name, current_season)
--  );


---------------------------------

insert into players
with years as (
	select * 
	from generate_series(1996,2022) as season
),
p as (
	select player_name, min(season) as first_season
	from player_seasons
	group by player_name
),
players_and_seasons as (
	select *
	from p 
	left join years 
	on p.first_season <= years.season
),
-- construct table window, which holding 3 columns, player_name, season, and seasons which is an array of struct datetype include season, gp, pts, reb,ast

windowed as (
	select
		pas.player_name,
		pas.season,
		array_remove(
				array_agg(
						case
							when ps.season is not null then
								row(
									ps.season,
									ps.gp,
									ps.pts,
									ps.reb,
									ps.ast	
								)::season_stats 
						end
					) 
					over (partition by pas.player_name order by pas.season), 
			NULL
		)	as seasons
		
	
	from players_and_seasons pas  -- full hisotry table for the player including all seasons since the first season of the player.
	left join player_seasons ps
		on pas.player_name = ps.player_name
		and pas.season = ps.season
	order by pas.player_name, pas.season
),

static as (
	select 
		player_name,
		max(height) as height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
	from player_seasons
	group by player_name
	
)
/* final data model: holding 
	- static data of the player from static table, 
	- plus seasons from windowed table <array of struct>, 
	- scorer_class: based on the last season pts, mapping to an enum data type.
	- years_since_last_active,
	- season,
	- is_active

*/
select 
	w.player_name,
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_round,
	s.draft_number,
	seasons as season_stats,
	case
		when (seasons[cardinality(seasons)]::season_stats).pts > 20 then 'star'
		when (seasons[cardinality(seasons)]::season_stats).pts > 15 then 'good'
		when (seasons[cardinality(seasons)]::season_stats).pts > 10 then 'average'
		else 'bad'
	end::scorer_class as scorer_class,
	w.season - (seasons[cardinality(seasons)]::season_stats).season as years_since_last_active,
	w.season as current_season,
	w.season = (seasons[cardinality(seasons)]::season_stats).season as is_active
	
from windowed w 
left join static s 
on w.player_name = s.player_name





