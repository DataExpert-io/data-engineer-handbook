
create type actors_scd_type as (
		quality_class quality_class,
		is_active boolean,
		start_year integer,
		end_year integer
)


with last_year_scd as (
select * from actors_history_scd ahs 
where current_year = 1981 and end_year = 1981),

this_year_data as (
select * from actors a 
where current_year = 1982),

historical_scd as (
select actor,
	   actor_id as actorid,
	   quality_class,
	   is_active,
	   start_year,
	   end_year
from actors_history_scd ahs 
where current_year = 1981 and end_year < 1981),

unchanged_records as (
select ty.actor,
ty.actorid,
ty.quality_class,
ty.is_active,
ly.start_year,
ty.current_year as end_year
from this_year_data ty join last_year_scd ly
on ty.actorid = ly.actor_id
where ty.quality_class = ly.quality_class and 
ty.is_active = ly.is_active),

changed_records as (
   SELECT
            ty.actor,
            ty.actorid,
            UNNEST(ARRAY[
                ROW(
                    ly.quality_class,
                    ly.is_active,
                    ly.start_year,
                    ly.end_year
                    )::actors_scd_type,
                ROW(
                    ty.quality_class,
                    ty.is_active,
                    ty.current_year,
                    ty.current_year
                    )::actors_scd_type
            ]) as records
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
    ON ly.actor_id = ty.actorid
     WHERE (ty.quality_class <> ly.quality_class
      OR ty.is_active <> ly.is_active)),

unnested_changed_records as (
select actor,
	   actorid,
	   (records::actors_scd_type).quality_class,
	   (records::actors_scd_type).is_active,
	   (records::actors_scd_type).start_year,
	   (records::actors_scd_type).end_year
	   from changed_records),

new_records as (
select ty.actor,
	ty.actorid,
	ty.quality_class,
	ty.is_active,
	ty.current_year as start_year,
	ty.current_year as end_year
from this_year_data ty left join last_year_scd as ly
on ty.actorid = ly.actor_id
where ly.actor_id is null)

select *, 1982 as current_year from (
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
) a

