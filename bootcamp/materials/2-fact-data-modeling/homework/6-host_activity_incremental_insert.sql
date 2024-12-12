-- This feels a lot like the previous homework. 
-- I may have misunderstood the requirement
with filter_date as (
	select
		coalesce(
			-- either the last computed date
			(select max(_current_date)  from host_cumulated)
			-- or if never computed, the day before the dataset begins
			, (select min(date(cast(event_time as timestamp))) - interval '1 day' from events)
		) as filter_date
)

, yesterday as (
	select
		*
	from
		host_cumulated
	where
		_current_date = (select filter_date from filter_date)
)


, today as (
	select
		host
		, max(date(cast(event_time as timestamp))) as _current_date
	from
		events
	where
		date(cast(event_time as timestamp)) = (select filter_date + interval '1 day' from filter_date)
	group by
		host
		
)

, final as (
	select
		coalesce(t.host, y.host) as host
		, case
			when y.host_activity_datelist is null then array[t._current_date]
			when t.host is null then y.host_activity_datelist
			else array[t._current_date] || y.host_activity_datelist
		end as host_activity_datelist
		, (select filter_date + interval '1 day' from filter_date) as _current_date
	from
		today as t
	full outer join
		yesterday as y
		on t.host = y.host
)

insert into host_cumulated
select * from final