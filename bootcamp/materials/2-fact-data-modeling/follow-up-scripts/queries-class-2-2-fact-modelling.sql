

select * from events limit 100;

select min(event_time), max(event_time) from events;

DROP TABLE IF EXISTS users_cumulated;
CREATE TABLE users_cumulated (
  user_id NUMERIC,
  -- the list of dates in the past where the user was active
  dates_active DATE[],
  -- the current date for the user
  date DATE,
  PRIMARY KEY (user_id, date)
);
comment on column users_cumulated.dates_active is 'the list of dates in the past where the user was active';


INSERT INTO users_cumulated
WITH yesterday AS (
  SELECT * FROM users_cumulated WHERE date = DATE('2023-01-01')
),
today AS (
  SELECT user_id, CAST(event_time AS DATE) AS date_active
  FROM events
  WHERE CAST(event_time AS DATE) = DATE('2023-01-02')
  AND user_id IS NOT NULL
  GROUP BY user_id, CAST(event_time AS DATE)
)
SELECT
  COALESCE(t.user_id, y.user_id) AS user_id,
  CASE WHEN y.dates_active IS NULL THEN ARRAY [t.date_active]
    WHEN t.date_active IS NULL THEN y.dates_active
    ELSE ARRAY[t.date_active] || y.dates_active END AS dates_active,
  COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t FULL JOIN yesterday y ON y.user_id = t.user_id;

SELECT * FROM users_cumulated;


DO $$
  DECLARE
    min_date DATE := '2023-01-02';
    max_date DATE := '2023-01-30';
    cd DATE;
  BEGIN
    FOR cd IN SELECT generate_series(min_date, max_date, INTERVAL '1 day')  LOOP
        INSERT INTO users_cumulated
        WITH yesterday AS (
          SELECT * FROM users_cumulated WHERE date = cd
        ),
             today AS (
               SELECT user_id, CAST(event_time AS DATE) AS date_active
               FROM events
               WHERE CAST(event_time AS DATE) = cd + INTERVAL '1 day'
                 AND user_id IS NOT NULL
               GROUP BY user_id, CAST(event_time AS DATE)
             )
        SELECT
          COALESCE(t.user_id, y.user_id) AS user_id,
          CASE WHEN y.dates_active IS NULL THEN ARRAY [t.date_active]
               WHEN t.date_active IS NULL THEN y.dates_active
               ELSE ARRAY[t.date_active] || y.dates_active END AS dates_active,
          COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
        FROM today t FULL JOIN yesterday y ON y.user_id = t.user_id;
    END LOOP;
END $$

SELECT * FROM users_cumulated WHERE CARDINALITY(dates_active) > 10;


-- create date list for 30 days

WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
),
series AS (
SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
)
SELECT *
FROM series
CROSS JOIN users WHERE user_id = '439578290726747300';


-- check if series date is inside dates active array, and if so, creates a bit value

WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
),
     series AS (
       SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
     )
SELECT dates_active @> ARRAY [series_date::DATE] is_active,
       date - series_date::DATE,
       *
FROM series CROSS JOIN users WHERE user_id = '439578290726747300';


WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
),
     series AS (
       SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
     )
SELECT CASE WHEN dates_active @> ARRAY [series_date::DATE]
  THEN POW(2, 32 - (date - series_date::DATE)) ELSE 0 END int_value,
       *
FROM series CROSS JOIN users WHERE user_id = '439578290726747300';


WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
), series AS (
 SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
SELECT
  CAST(CASE WHEN dates_active @> ARRAY [series_date::DATE]
    THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT)
    ELSE 0 END AS BIT(32)) bit_value, *
FROM series CROSS JOIN users WHERE user_id = '439578290726747300'
)
SELECT * FROM placeholder_ints;



WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
),
series AS (
 SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
 SELECT
   CASE WHEN dates_active @> ARRAY [series_date::DATE]
     THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT)
   ELSE 0 END bit_value, *
 FROM series CROSS JOIN users WHERE user_id = '439578290726747300'
)
SELECT
user_id,
CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) as bit_summed
FROM placeholder_ints
GROUP BY user_id ;


-- get how many days the users were active

WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
),
     series AS (
       SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
     ),
     placeholder_ints AS (
       SELECT
         CASE WHEN dates_active @> ARRAY [series_date::DATE]
                THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT)
              ELSE 0 END bit_value, *
       FROM series CROSS JOIN users
     )
SELECT
  user_id,
  CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) as bit_summed
FROM placeholder_ints
GROUP BY user_id ;


-- analysis: how many monthly active

WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
), series AS (
 SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
), placeholder_ints AS (
 SELECT CASE WHEN dates_active @> ARRAY [series_date::DATE]
   THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT) ELSE 0 END bit_value, *
 FROM series CROSS JOIN users
)
SELECT
  user_id,
  CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) AS bit_summed,
  BIT_COUNT(CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32))) AS dim_number_active_days,
  BIT_COUNT(CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active
FROM placeholder_ints
GROUP BY user_id ;


-- WEEKLY ACTIVE ANALYSIS

WITH users AS (
  SELECT * FROM users_cumulated WHERE date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
), placeholder_ints AS (
  SELECT CASE WHEN dates_active @> ARRAY [series_date::DATE]
                THEN CAST(POW(2, 32 - (date - series_date::DATE)) AS BIGINT) ELSE 0 END bit_value, *
  FROM series CROSS JOIN users
)
SELECT
  user_id,
  CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) AS bit_summed,
  BIT_COUNT(CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32))) AS dim_number_active_days,
  BIT_COUNT(CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
  BIT_COUNT( CAST('11111110000000000000000000000000' AS BIT(32)) &
  CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) ) > 0 AS dim_weekly_active,
  BIT_COUNT( CAST('10000000000000000000000000000000' AS BIT(32)) &
             CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(32)) ) > 0 AS dim_daily_active
FROM placeholder_ints
GROUP BY user_id ;

