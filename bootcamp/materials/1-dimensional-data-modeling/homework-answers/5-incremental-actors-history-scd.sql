CREATE TYPE actor_scd_type AS (
   quality_class quality_class_enum,
   is_active boolean,
   start_year INTEGER,
   end_year INTEGER
);

WITH latest_scd AS (
 SELECT * FROM actors_history_scd 
 WHERE EXTRACT(YEAR FROM end_date) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
 OR end_date IS NULL
),
historical_scd AS (
 SELECT 
   actor_id,
   quality_class,
   is_active,
   EXTRACT(YEAR FROM start_date)::INTEGER as start_year,
   EXTRACT(YEAR FROM end_date)::INTEGER as end_year
 FROM actors_history_scd
 WHERE EXTRACT(YEAR FROM end_date) < EXTRACT(YEAR FROM CURRENT_DATE) - 1
),
current_actors AS (
 SELECT 
   id as actor_id,
   quality_class,
   is_active,
   EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER as curr_year
 FROM actors
),
unchanged_records AS (
 SELECT
   ca.actor_id,
   ca.quality_class,
   ca.is_active,
   EXTRACT(YEAR FROM ls.start_date)::INTEGER as start_year,
   ca.curr_year as end_year
 FROM current_actors ca
 JOIN latest_scd ls ON ls.actor_id = ca.actor_id
 WHERE ca.quality_class = ls.quality_class 
 AND ca.is_active = ls.is_active
),
changed_records AS (
 SELECT
   ca.actor_id,
   UNNEST(ARRAY[
     ROW(
       ls.quality_class,
       ls.is_active,
       EXTRACT(YEAR FROM ls.start_date)::INTEGER,
       EXTRACT(YEAR FROM ls.end_date)::INTEGER
     )::actor_scd_type,
     ROW(
       ca.quality_class,
       ca.is_active,
       ca.curr_year,
       ca.curr_year
     )::actor_scd_type
   ]) as records
 FROM current_actors ca
 LEFT JOIN latest_scd ls ON ls.actor_id = ca.actor_id
 WHERE ca.quality_class != ls.quality_class
 OR ca.is_active != ls.is_active
),
unnested_changed_records AS (
 SELECT 
   actor_id,
   (records::actor_scd_type).quality_class,
   (records::actor_scd_type).is_active,
   (records::actor_scd_type).start_year,
   (records::actor_scd_type).end_year
 FROM changed_records
),
new_records AS (
 SELECT
   ca.actor_id,
   ca.quality_class,
   ca.is_active,
   ca.curr_year as start_year,
   ca.curr_year as end_year
 FROM current_actors ca
 LEFT JOIN latest_scd ls ON ca.actor_id = ls.actor_id
 WHERE ls.actor_id IS NULL
)
INSERT INTO actors_history_scd (
 actor_id,
 quality_class,
 is_active,
 start_date,
 end_date
)
SELECT 
 actor_id,
 quality_class,
 is_active,
 make_timestamp(start_year, 1, 1, 0, 0, 0)::timestamptz as start_date,
 CASE 
   WHEN end_year IS NOT NULL THEN make_timestamp(end_year, 12, 31, 23, 59, 59)::timestamptz
   ELSE NULL
 END as end_date
FROM (
 SELECT * FROM historical_scd
 UNION ALL
 SELECT * FROM unchanged_records
 UNION ALL 
 SELECT * FROM unnested_changed_records
 UNION ALL
 SELECT * FROM new_records
) combined;