--------- REDUSE 

--DROP TABLE array_metrics

-- CREATE TABLE array_metrics
-- (
--     user_id          TEXT,
--     month_start      DATE,
--     metric_name        TEXT,   
--     metric_array REAL[],   
--     PRIMARY KEY (user_id, month_start,metric_name)
-- );

--DELETE FROM  array_metrics
-- WITH daily_aggregate AS (
    
--     SELECT
--          CAST(user_id AS TEXT) AS user_id  ,
--          DATE(event_time) as date,
--          COUNT(1) as num_site_hits
--     FROM events
--     WHERE DATE(event_time)=DATE('2023-01-03')
--     AND user_id IS NOT NULL
--     GROUP BY user_id,DATE(event_time)
-- ),

-- yesterday_array AS (

--     SELECT * FROM array_metrics
--     WHERE month_start=DATE('2023-01-01')
-- )

-- INSERT INTO array_metrics(user_id, month_start, metric_name, metric_array)

-- SELECT  
--       COALESCE(da.user_id,ya.user_id) as user_id,
--       COALESCE(ya.month_start, DATE_TRUNC('month',da.date)) AS month_start,
--       'site_hits' AS metric_name,
--       CASE
--        -- WHEN yesterdays metric is not null then keep that  
--         WHEN ya.metric_array IS NOT NULL 
--         -- if yesterday is not null and today is also not null then concat both
--         THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
--         -- if yesterday is null 
--         WHEN ya.metric_array IS NULL 
--         -- then keep todays data

--         --THEN ARRAY[COALESCE(da.num_site_hits,0)] END 

--         /*problem with this is that if user dose not exist prior then it will take data for 
--         that date when user showed up but we need to backfull all days in the month
--         for this case we use  */

--         THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) 
--         || ARRAY[COALESCE(da.num_site_hits, 0)]
--     ELSE 
--         ARRAY[COALESCE(da.num_site_hits, 0)]  END AS metric_array

        
        
-- FROM daily_aggregate da 
-- FULL OUTER JOIN yesterday_array ya
-- ON da.user_id=ya.user_id

-- ON CONFLICT (user_id, month_start, metric_name) 
-- DO UPDATE 
-- SET metric_array = EXCLUDED.metric_array;

-- checking how many day each user has 

-- SELECT cardinality(metric_array),count(1) 
-- FROM array_metrics
-- GROUP BY 1


--------- doing dim analysis for 3 days 
WITH agg AS(
SELECT  
     metric_name,
     month_start,
     ARRAY[SUM(metric_array[1]),
           SUM(metric_array[2]),
           SUM(metric_array[3])] as summed_array
 FROM array_metrics
 GROUP BY 1,2 ) 

 SELECT  *,
       DATE(month_start+ CAST(CAST(index -1  AS TEXT) || 'day' AS Interval)) AS date
  FROM agg
   CROSS JOIN UNNEST(agg.summed_array)
   WITH ordinality AS a(elem,index)