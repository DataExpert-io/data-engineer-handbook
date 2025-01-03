-- I feel like this is a bit weird to have this full outer join here
-- I am not sure this would work but I will try to add a start and end date
-- to mimic a SCD2 table and not have duplicated rows for each day with no change
-- I may have misunderstood the model though

with filter_date as (
	select
		coalesce(
			-- either the last computed date
			(select max(_current_date)  from user_devices_cumulated)
			-- or if never computed, the day before the dataset begins
			, (select min(date(cast(event_time as timestamp))) - interval '1 day' from events)
		) as filter_date
)

, yesterday as (
	select
		*
	from
		user_devices_cumulated
	where
		_current_date = (select filter_date from filter_date)
)


, today as (
	select
		cast(e.user_id as text) as user_id
		, coalesce(d.browser_type, 'UNKNOWN') as browser_type
		, max(date(cast(event_time as timestamp))) as _current_date
	from
		events as e
	left join
		devices as d
		on e.device_id = d.device_id
	where
		date(cast(event_time as timestamp)) = (select filter_date + interval '1 day' from filter_date)
		and user_id is not null
	group by
		e.user_id
		, d.browser_type
		
)

, final as (
	select
		coalesce(t.user_id, y.user_id) as user_id
		, coalesce(t.browser_type, y.browser_type) as browser_type
		, case
			when y.device_activity_datelist is null then array[t._current_date]
			when t.browser_type is null then y.device_activity_datelist
			else array[t._current_date] || y.device_activity_datelist
		end as device_activity_datelist
		, (select filter_date + interval '1 day' from filter_date) as _current_date
	from
		today as t
	full outer join
		yesterday as y
		on t.user_id = y.user_id
		and t.browser_type = y.browser_type
)

insert into user_devices_cumulated
select * from final