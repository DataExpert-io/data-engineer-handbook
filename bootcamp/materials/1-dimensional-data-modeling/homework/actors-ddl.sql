 CREATE TYPE films_struct AS (
                         film text,
                         votes integer,
                         rating REAL,
                         filmid text
                       );
                      
 CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');



create table actors (
	actor text,
	actorid text,
	films films_struct[],
	quality_class quality_class,
	year_last_active integer,
	current_year integer,
	is_active boolean,
	primary key (actorid, current_year)
);
