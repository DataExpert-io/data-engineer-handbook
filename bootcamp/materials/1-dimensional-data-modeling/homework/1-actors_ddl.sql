create type films as (
	film text
	, votes integer
	, rating real
	, filmid text
);

create type quality_class as enum ('star', 'good', 'average', 'bad');

create table actors (
    actorid text
    , actor text
    , films films[]
    , quality_class quality_class
    , is_active boolean
    , current_year integer
    , primary key (actorid, current_year)
 );

select min(year) from public.actor_films;