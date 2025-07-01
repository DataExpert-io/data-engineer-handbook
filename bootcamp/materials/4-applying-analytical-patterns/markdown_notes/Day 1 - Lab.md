# Day 1 - Lab

This lab works with the same setup of the first week (the postgres one).

Let’s start by creating a new table, which we’re going to use also for the survivor analysis.

```sql
create table users_growth_accounting (
  user_id text,
  first_active_date date,
  last_active_date date,
  daily_active_state text, -- values that are gonna be like "churned, resurrected" etc.
  weekly_active_state text,
  dates_active date[], -- this is similar to week 2 cumulation
  date date,
  primary key (user_id, date)
);
```

Let’s now start building the cumulative table. The data available is not the same as what Zach has in the video somehow, so the dates we use are different. We start from ‘2023-01-01’.

```sql
with yesterday as (
  select *
  from users_growth_accounting
  where date = date('2022-12-31')
),

today as (
  select
   user_id::text,
  date_trunc('day', event_time::timestamp) as today_date,
   count(1)
  from events
  where date_trunc('day', event_time::timestamp) = date('2023-01-01')
 group by user_id, date_trunc('day', event_time::timestamp)
)

select
 coalesce(t.user_id, y.user_id) as user_id,
  coalesce(y.first_active_date, t.today_date) as first_active_date,
  -- this one below is intentionally the opposite of the one above:
  -- if they were active today, that's the last active date
  coalesce(t.today_date, y.last_active_date) as last_active_date,
  case
   -- active today, not yesterday
   when y.user_id is null and t.user_id is not null then 'New'
   when y.last_active_date = t.today_date - interval '1 day' then 'Retained'
    when y.last_active_date < t.today_date - interval '1 day' then 'Resurrected'
    -- y.date is the partition date
    when t.today_date is null and y.last_active_date = y.date then 'Churned'
    else 'Stale'
 end as daily_active_state,

  case when 1 = 1 then 1 end as weekly_active_state,
  coalesce(y.dates_active, array[]::date[])
   || case when
      t.user_id is not null
        then array[t.today_date]
        else array[]::date[]
        end as date_list,
 coalesce(t.today_date, y.date + Interval '1 day') as date
from today t
 full outer join yesterday y
  on t.user_id = y.user_id;
```

Run this query, and since this is the first iteration, you will see that everyone under `daily_active_state` is gonna be “New”, as expected, because this is the first exact day, everyone must be new. Keep in mind that the **starting date** matters a lot in determine new users.

Now add this extra piece right under `daily_active_state`, and try to understand the various case statements.

```sql
   case
   when y.user_id is null then 'New'
    -- here, since it's weekly, the user needs to be at any point in the last 7 days
    -- basically any day between yesterday and 7 days before
    when y.last_active_date > y.date - interval '7 day' then 'Retained'
    -- resurrected means they came back after a larger period of time than window
    when y.last_active_date < t.today_date - interval '7 day' then 'Resurrected'
    -- not active today, and last time active is exactly 7 days ago
    -- (cause they churn on this specific day), else they're stale
    when t.today_date is null and y.last_active_date = y.date - interval '7 day'
     then 'Churned'
    else 'Stale'
  end as weekly_active_state,
```

Now add `insert into users_growth_accounting` to the top of the query, and run it a couple times, bumping the dates every time. Query the new table and take a look at the results. You will see that some will be “**weekly active**”, but they have churned for “**daily active**”.

Now run it a few more times until you reach at least 9 total cumulations, so until ‘2023-01-09’.

Then run a query on `users_growth_accounting` filtering on the last date. You will see that some users have “churned” on `weekly_active_state`. These are those people that were last active longer than 1 week before.

Now let’s run for analysis. Run this query:

```sql
select
 date,
 count(1)
from users_growth_accounting
where first_active_date = '2023-01-01'
group by date;
```

You will see a result like this:

![image.png](images/d1la_image.png)

Basically this is a cohort of 84 users that were active the first time on the 1st of January.

Now, for the next step, run this:

```sql
select
  date,
  count(1),
  count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end) as number_active,
  round(1.0 * count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end)
   / count(1), 2) as pct_active
from users_growth_accounting
where first_active_date = '2023-01-01'
group by date
order by date;
```

You will basically see that **J-curve** mentioned in the the lecture. The 2 columns we added represent the (remaining) active users on each given day, as well as their percentage over the initial total.

However, this query works just for one cohort, and is not great for multiple ones. So we need to switch it up a bit.

```sql
select
  date - first_active_date as days_since_first_active,
  count(1),
  count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end) as number_active,
  round(1.0 * count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end)
   / count(1), 2) as pct_active
from users_growth_accounting
group by date - first_active_date
order by date - first_active_date;
```

Now, with this one we can find all cohorts, regardless of **when** they started. You see the 1st one obviously has 100% active percentage, with 508 people. Then it progressively reduces slowly throughout each **“day since first active”.** And now this is actually the J-curve mentioned in the lecture, for all people, not just a single cohort.

Obviously, you can take the above example and add more columns and group on them, to gain more information about the different cohorts.
