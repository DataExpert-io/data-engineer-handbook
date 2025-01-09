-- here we care only about users that come from the signup page.
-- here the query is at the USER LEVEL. Here we study the PER USER CONVERSION.
with deduped_events as (
    SELECT user_id,
        url,
        event_time,
        DATE(event_time) as event_date
    from events
    where user_id is not null
        and url in ('/signup', '/api/v1/login')
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
        and d1.event_time < d2.event_time
    where d1.url = '/signup'
),
-- the selfjoined table has 358 records and 6 distinct users only 
-- one user '14434123505499000000' has 353 records
-- select user_id,
--     count(1)
-- from selfjoined
-- group by user_id;
userlevel as(
    select user_id,
        max(
            case
                when destination_url = '/api/v1/login' then 1
                else 0
            end
        ) as converted
    from selfjoined
    GROUP BY user_id -- so we don't count the same user who converted more than once.
)
select count(1) as total_users,
    sum(converted) as total_converted,
    cast(sum(converted) as real) / count(1) as conversion_rate
from userlevel;