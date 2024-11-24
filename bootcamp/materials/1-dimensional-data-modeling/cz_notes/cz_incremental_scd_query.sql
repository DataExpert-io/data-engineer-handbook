-- create type scd_type as (
-- 	scorer_class text,
-- 	is_active boolean,
-- 	start_season integer,
-- 	end_season integer	
-- )



with last_season_scd as (

	select *
	from players_scd
	where current_season = 2021
	and end_season = 2021
),

historical_scd as (
	select 
		player_name,
		scorer_class::scorer_class,
		is_active,
		start_season,
		end_season
	from players_scd
	where current_season = 2021
	and end_season < 2021
),

this_season_data as (
	select * from players
	where current_season = 2022
),

unchanged_records as(
	select 
		ts.player_name,
		ts.scorer_class, 
		ts.is_active,
		ls.start_season,
		ts.current_season as end_season
	from this_season_data ts 
		 join last_season_scd ls 
		on ts.player_name = ls.player_name
	where ts.scorer_class = ls.scorer_class
	and ts.is_active = ls.is_active
),

changed_records as (
	select 
		ts.player_name,
		unnest(array[
			row(
				ls.scorer_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			row(
				ts.scorer_class,
				ts.is_active,
				ts.current_season,
				ts.current_season 
			)::scd_type
		]) as records
	from this_season_data ts 
		 left join last_season_scd ls 
		on ts.player_name = ls.player_name
	where (ts.scorer_class <> ls.scorer_class
		or ts.is_active <> ls.is_active)
),
unnested_changed_records as (
	select
		player_name,
		(records::scd_type).scorer_class::scorer_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
	from changed_records
),

new_records as (
	select 
		ts.player_name,
		ts.scorer_class,
		ts.is_active,
		ts.current_season as start_season,
		ts.current_season as end_season
	from this_season_data as ts
	left join last_season_scd ls 
	on ts.player_name = ls.player_name
	where ls.player_name is null 
),

-- all results combined 
results as(
select * from historical_scd
 union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all 
select * from new_records
)

select * from results 
where player_name like '%Michael Jordan%'