create table actors_history_scd(
	       actor text NOT NULL,
		   quality_class quality_class,
		   is_active BOOLEAN,
		   start_date integer,
		   end_date integer
);

CREATE TYPE scd_type_class AS (
                    quality_class quality_class,
                    is_active BOOLEAN,
                    start_date INTEGER,
                    end_date INTEGER
                        )
                        
                        
with actor_change as (select 
	actor,
	year,
	quality_class,
	case when (LAG(quality_class,1,quality_class) over (partition by actor ORDER BY year ) <> quality_class 
			or LAG(quality_class,1,quality_class) over (partition by actor ORDER BY year ) is null
			OR LAG(is_active) OVER (PARTITION BY actor ORDER BY year) <> is_active)
	then 1 else 0 end as did_change,
	is_active
from actors
WHERE year < 2010)

, streak_identifier as (
	select actor,
		   year,
		   quality_class,
		   COALESCE(is_active, false) AS is_active,
		   sum(did_change) over (partition by actor order by year asc ) as streak_identifier
	from actor_change)
	
	
insert into actors_history_scd  
	select actor,
		   quality_class,
		   is_active,
		   min(year) as min_date,
		   max(year) as max_date
		   
		from streak_identifier
		group by actor,
				 is_active,
				 quality_class,
				 streak_identifier
order by actor,min_date