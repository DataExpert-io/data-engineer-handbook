-- Creating a temp table should not be necessary
-- but we are working with Postgres 14 and it does not handles merge
-- statements. (Available since Postgres 15)
-- So we need to create a temporary table to make the upsert operation.

create temporary table tmp_upsert as

with last_processed_year as (
	select max(end_year) as last_year from actors_scd
)

, last_year_scd as ( --retrieve all "active" -> in dbt's snapshots it would be nulls
	select * from actors_scd where end_year = (select last_year from last_processed_year)
)

, this_year_data as (
	select
		actorid
		, actor
		, is_active
		, quality_class
		, current_year
	from
		actors
	where
		current_year = (select last_year + 1 from last_processed_year)
)

, unchanged_data as (
	select
		ly.actorid
        , ly.actor
        , ly.is_active
        , ly.quality_class
        , ly.start_year
        , ty.current_year as end_year
	from
		last_year_scd as ly
	inner join
		this_year_data as ty
		on ly.actorid = ty.actorid
	where
		ly.is_active = ty.is_active
		and ly.quality_class = ty.quality_class
)

, new_data as (
	select
		ty.actorid
        , ty.actor
        , ty.is_active
        , ty.quality_class
        , ty.current_year as start_year
        , ty.current_year as end_year
	from
		this_year_data as ty
	left join
		last_year_scd as ly
		on ly.actorid = ty.actorid
	where ly.actorid is null
)

, changed_data as ( --leave old lines because they already have the right end_year
	select
		ty.actorid
        , ty.actor
        , ty.is_active
        , ty.quality_class
        , ty.current_year as start_year
        , ty.current_year as end_year
	from
		this_year_data as ty
	inner join
		last_year_scd as ly
		on ty.actorid = ly.actorid
	where
		ly.is_active != ty.is_active
		or ly.quality_class != ty.quality_class
)

, rows_to_upsert as (
	select * from unchanged_data
	union
	select * from new_data
	union
	select * from changed_data
)

select * from rows_to_upsert;

update actors_scd as target
set end_year = source.end_year
from tmp_upsert as source
where target.actorid = source.actorid
  and target.start_year = source.start_year;

insert into actors_scd (actorid, actor, is_active, quality_class, start_year, end_year)
select
    actorid,
    actor,
    is_active,
    quality_class,
    start_year,
    end_year
from tmp_upsert
where not exists (
    select 1
    from actors_scd as target
    where target.actorid = tmp_upsert.actorid
      and target.start_year = tmp_upsert.start_year
);