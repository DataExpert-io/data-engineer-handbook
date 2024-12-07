with last_processed_year as (
	select max(current_year) as last_year from actors
)

, last_year as (
	select * from actors where current_year = (select last_year from last_processed_year)
)

, this_year as (
	select * from actor_films where year = (select last_year + 1 from last_processed_year)
)

, this_year_films_in_array as (
	select
		actorid
		, actor --not necessarily needed but easier to read
		, avg(rating) as avg_rating
		, array_agg(
			row(film, votes, rating, filmid)::films
		) as films
	from
		this_year
	group by
		actorid
		, actor --as unique as actorid so it does not change cardinality but I 
				--observed better performances than using an any_value on the field
)

, final as (
	select
		coalesce(ly.actorid, ty.actorid) as actorid
		, coalesce(ly.actor, ty.actor) as actor
		, coalesce(ly.films, array[]::films[])
			|| case when ty.films is not null then ty.films
			else array[]::films[]
			end as films
		, case 
			when ty.avg_rating is not null then
				(
					case
						when ty.avg_rating > 8 then 'star'
						when ty.avg_rating >= 8 and ty.avg_rating > 7 then 'good'
						when ty.avg_rating <= 7 and ty.avg_rating > 6 then 'average'
						when ty.avg_rating <= 6 then 'bad'
					end
				)::quality_class
			else ly.quality_class
		end as quality_class
		, ty.films is not null as is_active
		, (select last_year + 1 from last_processed_year) as current_year
		
	from
		last_year as ly
	full outer join this_year_films_in_array as ty
		on ly.actorid = ty.actorid
)

insert into actors
select * from final