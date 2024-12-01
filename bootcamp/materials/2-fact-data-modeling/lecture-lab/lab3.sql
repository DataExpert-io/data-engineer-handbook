   
   DROP TABLE array_metrics
   CREATE TABLE array_metrics(
   user_id NUMERIC,
   month_start date,
   metric_name text,
   metric_array real[],
   PRIMARY KEY (user_id, month_start, metric_name)
  
   )
   
  
   
   INSERT INTO array_metrics
   WITH daily_aggregate AS(
   SELECT 
	   user_id,
	   event_time ::date AS date,
	   count(1) AS num_site_hits
   FROM events
   WHERE event_time ::date = '2023-01-03'::date
   AND user_id IS NOT NULL 
   GROUP BY user_id, event_time::date 
   ),
   yesterday_array AS (
   
   SELECT * FROM array_metrics
   WHERE month_start = '2023-01-01'::date
   
   )
   SELECT
   COALESCE (da.user_id, ya.user_id) AS user_id,
   COALESCE ( ya.month_start, date_trunc('month', da.date)) AS month_start,
   'site_hits' AS metric_name,
   CASE 
   		WHEN ya.metric_array IS NOT NULL THEN ya.metric_array||array[COALESCE (da.num_site_hits,0)]
   		
   		WHEN ya.metric_array IS NULL THEN array_fill(0,ARRAY[COALESCE(date - month_start,0)]) || array[COALESCE(da.num_site_hits, 0)]
   END  AS metric_array
   FROM daily_aggregate da
   FULL OUTER JOIN yesterday_array ya ON 
   da.user_id=ya.user_id
ON conflict(user_id, month_start, metric_name)
DO 
UPDATE SET metric_array = EXCLUDED.metric_array
--------
WITH agg AS (

 SELECT 
 metric_name, 
 month_start, 
array[sum(metric_array[1]), sum(metric_array[2]), sum(metric_array[1])] AS summed_array
FROM array_metrics 
GROUP BY 1,2
)
SELECT 
metric_name,
month_start + CAST(cast(INDEX-1 AS text) || 'day' AS INTERVAL),
elem AS value
FROM agg CROSS JOIN unnest(agg.summed_array) WITH ORDINALITY AS a(elem, index)


SELECT * FROM array_metrics am 
