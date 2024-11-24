-- select max(year)
-- from actor_films

-- create a datatype for field films which is an array of struct
-- create type films as (
-- 	film text,
-- 	votes integer,
-- 	rating real,
-- 	filmid text
-- )


-- create type quality_class as enum ('star','good','average','bad')

-- create table actors ddl

-- drop table if exists actors;
-- create table actors (
-- 	actor text,
-- 	actorid text,
-- 	films films[],
-- 	quality_class quality_class,
-- 	current_year integer,
-- 	isactive boolean
-- )



DO $$
DECLARE 
	var_year integer;

BEGIN
	FOR var_year in 1969..2020 LOOP

		insert into actors
		with yesterday as (
			select 
				actor,
				actorid,
				films,
				quality_class,
				current_year,
				isactive
			from actors
			where current_year = var_year
		),

		today as (
			select 
				actor,
				actorid,
				array_agg(row(f.film,f.votes,f.rating,f.filmid)::films) as films,
			/*
			execution order of case when is TOP-DOWN. 
			- Each condition in the case statement is evaluated in sequence, starting from the first when clause
			- as soon as a condition evaluates to True, the corresponding result is returned.
			- The remaining conditions are ignored.
			*/
				(case
					when avg(rating) > 8 then 'star'
					when avg(rating) > 7 then 'good'
					when avg(rating) >6 then 'average'
					else 'bad'
				end)::quality_class as quality_class,
				year,
				case
					when sum(case when filmid is not null then 1 else 0 end) > 0 then TRUE
					else FALSE
				end as isactive

			from actor_films f
			where year = var_year + 1
			group by actor,actorid,year
		)

		select
			coalesce(t.actor, y.actor) as actor,
			coalesce(t.actorid, y.actorid) as actorid,
			case
				when y.films is null then t.films
				when t.films is null then y.films
				else y.films || t.films		
			end as films,
			-- quality class is determined by their most recent year. 
			case
				when t.quality_class is null then y.quality_class
				else t.quality_class
			end as quality_class,

			coalesce(t.year,y.current_year+1) as current_year,

			coalesce(t.isactive,FALSE) as isactive

		from today t full outer join yesterday y
		on t.actorid = y.actorid;

	END LOOP;
END $$;


select  current_year, count(*)
from actors 
group by current_year



	
