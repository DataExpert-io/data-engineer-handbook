
insert into host_activity_reduced

with day_to_compute as ( --from that date we can extract the month_start
	select coalesce(
		(select date(max(max_computed_date) + interval '1 day') from host_activity_reduced)
		, (select min(date(event_time)) from events)
		) as day_to_compute
)

 , today_data as (
	select
		host
		, date(event_time) as date
		, count(1) as num_hits
		, count(distinct user_id) as num_users
	from
		events
	where
		date(event_time) = (select day_to_compute from day_to_compute)
	group by
		host
		, date(event_time)
)

, yesterday_data as (
	select
		*
	from
		host_activity_reduced
	where month_start = date_trunc('month', (select day_to_compute from day_to_compute)) 
)

, final as (
	select
		coalesce(td.host, yd.host) as host
		, coalesce(yd.month_start, date_trunc('month', td.date)) as month_start
		, case
			when yd.hit_array is null then array[coalesce(td.num_hits, 0)]
			else yd.hit_array || array[coalesce(td.num_hits, 0)]
		end as hit_array
		, case
			when yd.unique_visitors_array is null
				then array_fill(0, array[td.date - coalesce(yd.month_start, date(date_trunc('month', td.date)))]) 
					|| array[coalesce(td.num_users, 0)] --in case new hosts comes in the middle of the month and get consistent number of digits in array
			else yd.unique_visitors_array || array[coalesce(td.num_users, 0)]
		end as unique_visitors_array
		, coalesce(td.date, yd.max_computed_date + interval '1 day') as max_computed_date
		
	from
		today_data td
	full outer join
		yesterday_data as yd
		on td.host = yd.host -- no need to put month start since we already filtered the right data
)

select * from final

on conflict (host, month_start)
do
	update set hit_array = excluded.hit_array, unique_visitors_array = excluded.unique_visitors_array, max_computed_date = excluded.max_computed_date
;