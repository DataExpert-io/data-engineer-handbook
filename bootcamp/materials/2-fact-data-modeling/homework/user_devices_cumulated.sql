-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE USER_DEVICES_CUMULATED(
                                       USER_ID NUMERIC,
                                       DEVICE_ID NUMERIC,
                                       BROWSER_TYPE TEXT,
                                       DEVICE_ACTIVITY DATE[],
                                       DATE DATE,
                                       PRIMARY KEY(USER_ID, DEVICE_ID, BROWSER_TYPE, DATE)
)


INSERT INTO USER_DEVICES_CUMULATED
WITH YESTERDAY AS(
    SELECT * FROM USER_DEVICES_CUMULATED
    -- WHERE DATE = DATE('2022-12-31')
    WHERE DATE = DATE('2023-01-30')
    ),
    TODAY AS(
SELECT
    E.USER_ID,
    E.DEVICE_ID,
    D.BROWSER_TYPE,
    DATE(E.EVENT_TIME) AS DATE
FROM EVENTS E
    LEFT JOIN DEVICES D
ON E.DEVICE_ID = D.DEVICE_ID
WHERE
    DATE(EVENT_TIME) = DATE('2023-01-31')
  AND E.USER_ID IS NOT NULL
  AND E.DEVICE_ID IS NOT NULL
GROUP BY 1,2,3,4
    )
SELECT
    COALESCE(T.USER_ID, Y.USER_ID) AS USER_ID,
    COALESCE(T.DEVICE_ID, Y.DEVICE_ID) AS DEVICE_ID,
    COALESCE(T.BROWSER_TYPE, Y.BROWSER_TYPE) AS BROWSER_TYPE,
    CASE WHEN Y.DEVICE_ACTIVITY IS NULL
             THEN ARRAY[T.DATE]
         WHEN T.DATE IS NULL THEN Y.DEVICE_ACTIVITY
         ELSE ARRAY[T.DATE] || Y.DEVICE_ACTIVITY
        END AS DEVICE_ACTIVITY,
    COALESCE(T.DATE, Y.DATE + INTERVAL '1 DAY') AS DATE
FROM TODAY T FULL OUTER JOIN YESTERDAY Y
ON T.USER_ID = Y.USER_ID
GROUP BY 1,2,3,4,5


WITH USER_DEVICES AS (
    SELECT * FROM USER_DEVICES_CUMULATED
    WHERE DATE = DATE('2023-01-31')
    ), SERIES AS (
    SELECT * FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS SERIES_DATE

    ), PLACEHOLDER_INT AS (
    SELECT
    CASE WHEN
    DEVICE_ACTIVITY @> ARRAY[DATE(SERIES_DATE)]
    THEN	POW(2, 32- (DATE - DATE(SERIES_DATE)))
    ELSE 0
    END AS INT_VALUE,
    *
    FROM USER_DEVICES CROSS JOIN SERIES
    )
SELECT USER_ID,
       BROWSER_TYPE,
       CAST(CAST(SUM(INT_VALUE)AS BIGINT) AS BIT(32)) AS DATELIST_INT ,
       BIT_COUNT(CAST(CAST(SUM(INT_VALUE)AS BIGINT) AS BIT(32))) > 0 AS DIM_IS_MONTHLY_ACTIVE,
       BIT_COUNT( CAST('11111110000000000000000000000000' AS BIT(32))& CAST(CAST(SUM(INT_VALUE)AS BIGINT) AS BIT(32))) > 0 AS DIM_IS_WEEKLY_ACTIVE,
       BIT_COUNT( CAST('10000000000000000000000000000000' AS BIT(32))& CAST(CAST(SUM(INT_VALUE)AS BIGINT) AS BIT(32))) > 0 AS DIM_IS_DAILY_ACTIVE

FROM PLACEHOLDER_INT
GROUP BY USER_ID, BROWSER_TYPE
