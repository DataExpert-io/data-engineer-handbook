WITH deduped_events AS (
    SELECT
        url, host, user_id,event_time
    FROM events
    GROUP BY 1,2,3,4
),
     clean_events AS (
         SELECT *, DATE(event_time) as event_date FROM deduped_events
WHERE user_id IS NOT NULL
ORDER BY user_id, event_time
     ),
     converted AS (
         SELECT ce1.user_id,
                ce1.event_time,
                ce1.url,
                COUNT(DISTINCT CASE WHEN ce2.url = '/api/v1/user' THEN ce2.url END) as converted
         FROM clean_events ce1
                  JOIN clean_events ce2
                       ON ce2.user_id = ce1.user_id
                           AND ce2.event_date = ce1.event_date
                           AND ce2.event_time > ce1.event_time

         GROUP BY 1, 2,3
     )

SELECT url, COUNT(1), CAST(SUM(converted) AS REAL)/COUNT(1)
FROM converted
GROUP BY 1
HAVING CAST(SUM(converted) AS REAL)/COUNT(1) > 0
AND COUNT(1) > 100
