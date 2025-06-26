WITH starter AS (
    SELECT
        uc.user_id,
        uc.dates_active @> ARRAY[DATE(d.valid_date)] AS is_active,
        EXTRACT(
            DAY FROM DATE('2023-03-31') - d.valid_date
        ) AS days_since
    FROM users_cumulated AS uc
    CROSS JOIN
        (
            SELECT
                GENERATE_SERIES(
                    '2023-02-28', '2023-03-31', INTERVAL '1 day'
                ) AS valid_date
        ) AS d
    WHERE uc.date = DATE('2023-03-31')
),

bits AS (
    SELECT
        user_id,
        SUM(CASE
            WHEN is_active THEN POW(2, 32 - days_since)
            ELSE 0
        END)::BIGINT::BIT(32) AS datelist_int,
        DATE('2023-03-31') AS date
    FROM starter
    GROUP BY user_id
)

INSERT INTO user_datelist_int
SELECT * FROM bits
