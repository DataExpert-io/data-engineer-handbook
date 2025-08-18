CREATE TABLE device_hits_dashboard AS

WITH events_augmented AS (
    SELECT COALESCE(d.os_type, 'unknown')      AS os_type,
           COALESCE(d.device_type, 'unknown')  AS device_type,
           COALESCE(d.browser_type, 'unknown') AS browser_type,
           url,
           user_id
    FROM events e
             JOIN devices d on e.device_id = d.device_id
)

SELECT
       CASE
           WHEN GROUPING(os_type) = 0
               AND GROUPING(device_type) = 0
               AND GROUPING(browser_type) = 0
               THEN 'os_type__device_type__browser'
           WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
           WHEN GROUPING(device_type) = 0 THEN 'device_type'
           WHEN GROUPING(os_type) = 0 THEN 'os_type'
       END as aggregation_level,
       COALESCE(os_type, '(overall)') as os_type,
       COALESCE(device_type, '(overall)') as device_type,
       COALESCE(browser_type, '(overall)') as browser_type,
       COUNT(1) as number_of_hits
FROM events_augmented
GROUP BY GROUPING SETS (
        (browser_type, device_type, os_type),
        (browser_type),
        (os_type),
        (device_type)
    )
ORDER BY COUNT(1) DESC