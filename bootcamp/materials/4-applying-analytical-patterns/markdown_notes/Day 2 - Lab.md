# Day 2 - Lab

For this lab, we will be working almost exclusively with the `events` table.

```sql
select * from events;
```

The first thing we’ll do is to figure out for every person that goes to sign up page, how many of them actually sign up.

There’s two URLs that will determine this.

- `/signup`
- `/api/v1/users` (or `/api/v1/login`)

[The data we have is different than Zach’s, so we will use `/api/v1/login` for the sake of the exercise — Ed.]

```sql
select * from events
where url in ('/signup', '/api/v1/login');
```

We want to understand what % of people who reached the signup page, eventually signed up. We’re gonna do this without window functions, we’re going to do with with self joins.

Let’s get started by deduping and filtering the dataset.

```sql
with deduped_events as (
  select
   user_id,
   url,
   event_time,
   date(event_time) as event_date
  from events
  where user_id is not null
  and url in ('/signup', '/api/v1/login')
  group by user_id, url, event_time, date(event_time)
)

select *
from deduped_events;
```

What we want to do now is say *“did this user, who visited a sign up page, did they ever sign up after they visited the sign up page”?*

We join the table on itself, so the visiting of the sign up page and the sign up event are on the same row.

```sql
-- [..]

select
 d1.user_id,
  d1.url,
  d2.url as destination_url,
  d1.event_time,
  d2.event_time
from deduped_events d1
 join deduped_events d2
  on d1.user_id = d2.user_id
  and d1.event_date = d2.event_date
  and d2.event_time > d1.event_time
where d1.url = '/signup'
and d2.url = '/api/v1/login';
```

[The results of the queries are janky because we don’t have the same data as Zach — Ed.]

This shows all users who did signup before, and later logged in, in one single row.

Let’s now expand the above query

```sql
-- [..]

selfjoined as (
  select
    d1.user_id,
    d1.url,
    d2.url as destination_url,
    d1.event_time,
    d2.event_time
  from deduped_events d1
    join deduped_events d2
    on d1.user_id = d2.user_id
    and d1.event_date = d2.event_date
    and d2.event_time > d1.event_time
  where d1.url = '/signup'
)

select
 user_id,
  MAX(case when destination_url = '/api/v1/login' then 1 else 0 end) as converted
from selfjoined
group by user_id;
```

This one will now show the users who visited the signup page and converted, as well as the users who visited the signup page **but didn’t convert.**

Aggregating once more, we can see the totals of this, and the global conversion rate:

```sql
-- [..]

userlevel as (
  select
    user_id,
    MAX(case when destination_url = '/api/v1/login' then 1 else 0 end) as converted
  from selfjoined
  group by user_id
)

select
 count(1) as total_users,
  sum(converted) total_converted,
  round(sum(converted) * 1.0 / count(1), 2) as conversion_rate
from userlevel;
```

Let’s now finish the query by adding extra information, including all pages above a certain number of hits (to prevent nonsense), and removing the very specific filters on just `signup` and `api/v1/login` (note the commented out rows).

```sql
with deduped_events as (
  select
   user_id,
   url,
   event_time,
   date(event_time) as event_date
  from events
  where user_id is not null
--   and url in ('/signup', '/api/v1/login')
  group by user_id, url, event_time, date(event_time)
),

selfjoined as (
  select
    d1.user_id,
    d1.url,
    d2.url as destination_url,
    d1.event_time,
    d2.event_time
  from deduped_events d1
    join deduped_events d2
    on d1.user_id = d2.user_id
    and d1.event_date = d2.event_date
    and d2.event_time > d1.event_time
--   where d1.url = '/signup'
),

userlevel as (
  select
   url,
   count(1) as number_of_hits,
    user_id,
    MAX(case when destination_url = '/api/v1/login' then 1 else 0 end) as converted
  from selfjoined
  group by user_id, url
)

select
 url,
  sum(number_of_hits) as num_hits,
 sum(converted) as num_converted,
  round(sum(converted) * 1.0 / sum(number_of_hits), 2) as conversion_rate
from userlevel
group by url
having sum(number_of_hits) > 500;
```

One issue with this query, w.r.t. the previous one, is that whereas before the hits on a certain page per user where counted only once, now they’re double counted all the time.

[The way Zach fixes this issue is dubious to me imho, but he was rushing a bit so I didn’t want to include it in the notes in detail. It’s not that much relevant anyway, as the goal here is to understand the analytical patterns rather than specific SQL gymnastics — Ed.]

Anyway, this is the idea behind funnel analysis. You have two events and you want to see “this happens after that”.

---

Let’s now work with another query to work with `grouping sets`:

```sql
with events_augmented as (
 select
   coalesce(d.os_type, 'unknown') as os_type,
   coalesce(d.device_type, 'unknown') as device_type,
   coalesce(d.browser_type, 'unknown') as browser_type,
   url,
   user_id
  from events e
   join devices d on e.device_id = d.device_id
)

select * from events_augmented;
```

What we’re going to do now is looking at website events, and see what type of device is the one that’s most common.

The naive way to do this, is to just run:

```sql
-- [..]

select
 os_type,
  device_type,
  browser_type,
 count(1)
from events_augmented
group by os_type, device_type, browser_type;
```

But what if we’re interested in different kind of slices of this data?

This is where grouping sets come in.

> Remember that you have to put all grouping columns in the grouping sets at least once, otherwise the query will fail.
>

```sql
-- [..]

select
 os_type,
  device_type,
  browser_type,
 count(1)
from events_augmented
group by grouping sets (
  (browser_type, device_type, os_type),
  (browser_type),
  (os_type),
  (device_type)
);
```

Run the above query and sort by `count` descending. You see the nulls? That’s because of grouping sets, or in other words, it’s because they have been ignored in that specific grouping, for that particular row.

To give it more meaning, you can do something like this:

```sql
-- [..]

select
 coalesce(os_type, '(overall)') as os_type,
  coalesce(device_type, '(overall)') as device_type,
  coalesce(browser_type, '(overall)') as browser_type,
 count(1)
-- [..]
```

So this will give you the indication that when the value says `(overall)`, it means that column for that row has all values included in that group.

E.g.

| os_type | device_type | browser_type | count |
| --- | --- | --- | --- |
| Mac OS X | (overall) | (overall) | 3304 |

means that all Mac OS X users, regardless of `device_type` and `browser_type`, in total accumulated 3304 events.

Now try adding these lines

```sql
select
 grouping(os_type),
  grouping(device_type),
  grouping(browser_type),
-- [..]
```

You will see that the 1st 3 columns now are a bunch of 0s and 1s. What it means is that if the value is 0, the column is being grouped upon, and if the value is 1, it means the column is not being grouped by.

Let’s now build a new column for this table, that uses these groupings, to understand which grouping set is being applied for each row.

```sql
-- [..]
  case
   when grouping(os_type) = 0
     and grouping(device_type) = 0
      and grouping(browser_type) = 0
     then 'os_type__device_type__browser_type'
   when grouping(os_type) = 0 then 'os_type'
    when grouping(device_type) = 0 then 'device_type'
    when grouping(browser_type) = 0 then 'browser_type'
  end as aggregation_level,
-- [..]
```

A probably smarter way to do the above, especially in the case of several combinations, is to write it like this:

```sql
  array_to_string(array[
    case when grouping(os_type) = 0 then 'os_type' end,
  case when grouping(device_type) = 0 then 'device_type' end,
    case when grouping(browser_type) = 0 then 'browser_type' end
 ], '__') as aggregation_level
```

This way, you can get any kind of `aggregation_level` automatically, without having to write them all out explicitly.

Let’s now create a table out of all this query

```sql
create table device_hits_dashboard as (
-- [..]
);
```

Now you can run queries like

```sql
select *
from device_hits_dashboard
where aggregation_level = 'device_type';
```

which means in your dashboards you don’t have to query event data, as it’s pre-aggregated, and **it can be powered based on `WHERE` conditions rather than `GROUP BY` conditions**. You even have the largest aggregation level like `os_type__device_type__browser_type`, which shows the results as if we were grouping for all 3 columns at once.
