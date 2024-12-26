-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)


-- SELECT * FROM EVENTS

CREATE TABLE HOSTS_CUMULATED(
                                       HOST TEXT,
                                       HOST_ACTIVITY_DATELIST DATE[],
                                       DATE DATE,
                                       PRIMARY KEY(HOST,DATE)
)


INSERT INTO HOSTS_CUMULATED
WITH YESTERDAY AS(
    SELECT * FROM HOSTS_CUMULATED
    -- WHERE DATE = DATE('2022-12-31')
    WHERE DATE = DATE('2023-01-13')
    ),
    TODAY AS(
SELECT
    HOST,
    DATE(EVENT_TIME) AS DATE
FROM EVENTS
WHERE
    DATE(EVENT_TIME) = DATE('2023-01-14')
GROUP BY 1,2
    )
SELECT
    COALESCE(T.HOST, Y.HOST) AS HOST,
    CASE WHEN Y.HOST_ACTIVITY_DATELIST IS NULL
             THEN ARRAY[T.DATE]
         WHEN T.DATE IS NULL THEN Y.HOST_ACTIVITY_DATELIST
         ELSE ARRAY[T.DATE] || Y.HOST_ACTIVITY_DATELIST
        END AS HOST_ACTIVITY_DATELIST,
    COALESCE(T.DATE, Y.DATE + INTERVAL '1 DAY') AS DATE
FROM TODAY T FULL OUTER JOIN YESTERDAY Y
ON T.HOST = Y.HOST




CREATE TABLE HOST_ACTIVITY_REDUCED (
                                       MONTH DATE,
                                       HOST TEXT,
                                       HIT_ARRAY REAL[],
                                       UNIQUE_VISITORS_ARRAY INT[],
                                       PRIMARY KEY(MONTH, HOST, HIT_ARRAY, UNIQUE_VISITORS_ARRAY)
)

    INSERT INTO HOST_ACTIVITY_REDUCED
WITH DAILY_AGGREGATE AS (
	SELECT
		HOST,
		DATE(EVENT_TIME) AS DATE,
		COUNT(1) AS HITS,
		COUNT(DISTINCT USER_ID) AS UNIQUE_VISITORS

	FROM EVENTS
	WHERE DATE(EVENT_TIME) = DATE('2023-01-3')
	AND HOST IS NOT NULL
	GROUP BY HOST, DATE(EVENT_TIME)
),
	YESTERDAY_ARRAY AS (

	SELECT * FROM HOST_ACTIVITY_REDUCED
	WHERE MONTH = DATE('2023-01-01')

)
SELECT
    DA.DATE AS MONTH,
		COALESCE(DA.HOST, YA.HOST) AS HOST,
		CASE WHEN YA.HIT_ARRAY IS NOT NULL
			THEN YA.HIT_ARRAY || ARRAY[COALESCE(DA.HITS, 0)]
		WHEN YA.HIT_ARRAY IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(DATE - DATE(DATE_TRUNC('MONTH', DATE)),0)]) || ARRAY[COALESCE(DA.HITS, 0)]
END	AS HIT_ARRAY,

		CASE WHEN YA.UNIQUE_VISITORS_ARRAY IS NOT NULL
			THEN YA.UNIQUE_VISITORS_ARRAY || ARRAY[COALESCE(DA.UNIQUE_VISITORS, 0)]
		WHEN YA.UNIQUE_VISITORS_ARRAY IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(DATE - DATE(DATE_TRUNC('MONTH', DATE)),0)]) || ARRAY[COALESCE(DA.UNIQUE_VISITORS, 0)]
END	AS UNIQUE_VISITORS_ARRAY

	FROM DAILY_AGGREGATE DA
		FULL OUTER JOIN YESTERDAY_ARRAY YA
		ON DA.HOST = YA.HOST
