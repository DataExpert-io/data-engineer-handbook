-- at url level
with deduped_events as (
    SELECT user_id,
        url,
        event_time,
        DATE(event_time) as event_date
    from events
    where user_id is not null -- and url in ('/signup', '/api/v1/login')
    group by user_id,
        url,
        event_time,
        DATE(event_time)
),
selfjoined as (
    SELECT d1.user_id,
        d1.url,
        d2.url as destination_url,
        d1.event_time,
        d2.event_time
    from deduped_events d1
        join deduped_events d2 ON d1.user_id = d2.user_id
        and d1.event_date = d2.event_date
        and d1.event_time < d2.event_time -- where d1.url = '/signup'
),
userlevel as(
    select user_id,
        url,
        count(1) as num_of_hits,
        sum(
            case
                when destination_url = '/api/v1/login' then 1
                else 0
            end
        ) as converted
    from selfjoined
    GROUP BY user_id,
        url
)
select url,
    sum(num_of_hits) as num_hits,
    sum(converted) as num_converted,
    cast(sum(converted) as real) / sum(num_of_hits) as pct_converted
from userlevel
GROUP BY url;