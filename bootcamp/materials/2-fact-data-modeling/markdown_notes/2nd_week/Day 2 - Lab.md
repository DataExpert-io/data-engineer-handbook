# Day 2 - Lab

Let’s take a look at `events` table.

```sql
SELECT * FROM events;
```

This is network requests going to Zach’s website.

What we wanna do is to cumulate this up, and find all days where different users were active.

For starters, let’s create a table

```sql
CREATE TABLE users_cumulated (
  user_id TEXT, -- text because ids are bigger than BIGINT allows
  dates_active DATE[], -- list of past dates where user was active
  date DATE, -- current date for the user
  PRIMARY KEY (user_id, date)
);
```

Let’s now get started with cumulative table design, like we did the previous week.

```sql
INSERT INTO users_cumulated
WITH
yesterday AS (
  SELECT
    *
  FROM users_cumulated
  WHERE date = DATE('2022-12-31') -- last date before beginning of dataset
),

-- from this, we need all users who were active "today"
-- this is where you need to come up with a definition of "active".
today AS (
  SELECT
    CAST(user_id AS TEXT),
    DATE(CAST(event_time AS TIMESTAMP)) AS date_active
  FROM events
  WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01') -- last date before beginning of dataset
  AND user_id IS NOT NULL
  GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

-- Let's match the schema of the table we just created
SELECT
  COALESCE(t.user_id, y.user_id) AS user_id,
  CASE
    WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
    WHEN t.date_active IS NULL THEN y.dates_active
    ELSE ARRAY[t.date_active] || y.dates_active
  END AS dates_active,
   COALESCE(t.date_active, y.date + INTERVAL  '1 day') AS date
FROM today t
  FULL OUTER JOIN yesterday y
  ON t.user_id = y.user_id;


SELECT * FROM users_cumulated;
```

Now build this up by bumping both dates by 1 day for the whole month of January, and running the query each time.

To make sure, after the 2nd cumulation check:

```sql
SELECT * FROM user_cumulated
WHERE date = '2023-01-02';
```

You see that the last date, is the first element in the array `dates_active`.

Let’s now generate the “date list” for a month, with the BIT MASK as described in the lecture.

```sql
-- first we need to generate the series of the days we will consider, so all January
SELECT *
FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day');

WITH
users AS (
  SELECT *
  FROM users_cumulated
  WHERE date = ('2023-01-31')
),

series AS (
  SELECT *
  FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
  AS series_date
),

place_holder_ints AS (
  SELECT
    CASE
      -- when the specific date is in the array of active dates
      WHEN dates_active @> ARRAY[DATE(series_date)]
        THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
        ELSE 0
      END AS placeholder_int_value,
    *
  FROM users CROSS JOIN series
)

SELECT
  user_id,
  CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS bit_mask
FROM place_holder_ints
GROUP BY user_id;
```

The procedure to generate the bit mask works as such:

- Join each user with the list of dates of the month → We get 31 rows per user.
- Wherever the date from the list of dates **is in** the user’s `dates_active` array, it means the user was **active on that date**
  - Find how many days passed since the start of the period (which is 32 days long, staring yesterday)
    → `x = 32 - (today - date_active)`
    - So imagine, if today is Jan 31st, the period is from **Jan 30th back until Dec 31**.
  - So if `date_active == today`, there’s 32 days since beginning of period (counting today as well)
    *Note: This (today’s activity) won’t appear in the bit mask, as it overflows the BIT(32). It can however show up if instead of `32` , you subtract from `31`. Depends on the situation.*
- Calculate 2 to the power of the number above
- Transform this power into binary number

That’s cause $2^x$ generates a number $n$ with `len(n) = x` , with only the leftmost digit = 1, and everything else equal to 0. By summing all powers of 2, and then getting the `BIT(32)` value of it, we get the **BIT MASK** for the full month, where 1s are active days, and 0s are inactive days, for any given user. Given that we use `BIT(32)`, the total length of the bit mask is 32, so we can look back to **at most** 32 days of activity (where the 1st is “today”, so last day is 31 days “ago”).

In other words, a person active ONLY yesterday looks like `10000...`, one active ONLY two days ago looks like `01000...`. Someone active the 1st and 30th of January (again assuming today is 31st) looks like `1000......001`. The `len` of this number is always **32.**

This cumulative table design is a very powerful way to record user activity in minimal space and also saves a lot of time for analytics as the queries won’t require any aggregation, since you just look at the bit mask of the activity.

Let’s now see how we can calculate if a user is `monthly_active`.

```sql
-- [..]
BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)))
```

Counting “active” bits shows how many days throughout the month the user was active.

So we can do

```sql
BIT_COUNT(...) > 0 AS dim_is_monthly_active
```

What if we want to do is `weekly_active` instead?

This is not super elegant, but basically we **mask** the original bit mask with just the last 7 days, with a bitwise operation: `1111111000... & bit_mask`. This is called a **bitwise and.**

In Postgresql it’s like this

```sql
CAST('11111110000000000000000000000000' AS BIT(32)) &
CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
```

Now, we can do the `BIT_COUNT` again and you’d get `dim_weekly_active`.

The same for other values, like `daily_active` (e.g. active last day).

```sql
-- [..]

SELECT
  user_id,
  CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS bit_mask,
  BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
    AS dim_is_monthly_active,
  BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
    AS dim_is_weekly_active,
  BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) &
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
    AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id;
```

Also, while this operation looks ugly in code, it’s extremely efficient from a computing perspective.
