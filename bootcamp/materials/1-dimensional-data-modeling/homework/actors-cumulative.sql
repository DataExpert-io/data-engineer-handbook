insert into actors
with yesterday as (
select * from actors af 
where current_year = 1972),

today as (
	select actor, actorid, year,
array_agg(row(
	film, 
	votes, 
	rating,
	filmid
)::films_struct) as films,
round(avg(rating)::numeric,0) as avg_rating
from actor_films af 
where year = 1973
group by 1,2,3
)

select 
	coalesce(y.actor, t.actor) as actor,
	coalesce(y.actorid, t.actorid) as actorid,
	case when y.films is null
	then t.films
	when t.films is not null then y.films || t.films
			else y.films
			end as films,
	case when t.films is not null then
	case when t.avg_rating > 8 then 'star'
		when t.avg_rating > 7 and t.avg_rating <= 8 then 'good'
		when t.avg_rating > 6 and t.avg_rating <= 7 then 'average'
		when t.avg_rating <= 6 then 'bad'
		end::quality_class
	else y.quality_class end as quality_class,
	case when t.films is null then y.year_last_active
	else t.year
	end as year_last_active,
	1973 as current_year,
	case when t.films is null then false 
	else true 
	end::boolean as is_active
	from today t full outer join yesterday y
	on t.actorid = y.actorid
