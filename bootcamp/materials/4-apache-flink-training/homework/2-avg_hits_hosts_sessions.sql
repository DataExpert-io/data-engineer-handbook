select
	host
	, avg(num_hits) as avg_num_hits
from
	processed_events_aggregated_ip_host_session
group by
	host
order by
	avg_num_hits desc