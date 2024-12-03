create table actors_history_scd (
	actor text,
	actor_id text,
	quality_class quality_class,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key (actor_id, start_year)
)


