create table host_activity_reduced (
	host text
	, month_start date
	, hit_array real[]
	, unique_visitors_array real[]
 	, max_computed_date date -- here to track what date was computed 
    -- it allows easily incremental load of the table withouth have to update the query 
    -- and hardcode the date we want to compute
	, primary key (host, month_start)
);
