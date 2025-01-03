create table actors_history_scd (
	actorid text
	, actor text
	, quality_class quality_class
	, is_active boolean
	, start_year integer
	, end_year integer
	, primary key (actorid, start_year)
);
