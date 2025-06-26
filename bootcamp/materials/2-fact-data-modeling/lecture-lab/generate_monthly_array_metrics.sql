WITH yesterday AS (
    SELECT *
    FROM monthly_user_site_hits
    WHERE date_partition = '2023-03-02'
),

today AS (
    SELECT
        user_id,
        DATE_TRUNC('day', event_time) AS today_date,
        COUNT(*) AS num_hits
    FROM events
    WHERE
        DATE_TRUNC('day', event_time) = DATE('2023-03-03')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time)
)

INSERT INTO monthly_user_site_hits
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(
        y.hit_array,
        ARRAY_FILL(NULL::BIGINT, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')])
    )
    || ARRAY[t.num_hits] AS hits_array,
    DATE('2023-03-01') AS month_start,
    CASE
        WHEN y.first_found_date < t.today_date
            THEN y.first_found_date
        ELSE t.today_date
    END AS first_found_date,
    DATE('2023-03-03') AS date_partition
FROM yesterday AS y
FULL OUTER JOIN today AS t
    ON y.user_id = t.user_id
