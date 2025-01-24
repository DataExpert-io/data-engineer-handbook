TRUNCATE actors_history_scd;

WITH actor_class_changes AS (
 SELECT 
   actorId,
   actor,
   year,
   AVG(rating)::DECIMAL(3,1) as avg_rating,
   CASE 
     WHEN AVG(rating) > 8 THEN 'star'::quality_class_enum
     WHEN AVG(rating) > 7 THEN 'good'::quality_class_enum
     WHEN AVG(rating) > 6 THEN 'average'::quality_class_enum 
     ELSE 'bad'::quality_class_enum
   END as curr_quality_class,
   year = (SELECT MAX(year) FROM actor_films) as curr_is_active,
   ROW_NUMBER() OVER (PARTITION BY actorId ORDER BY year) as version_num,
   LAG(CASE 
     WHEN AVG(rating) > 8 THEN 'star'::quality_class_enum
     WHEN AVG(rating) > 7 THEN 'good'::quality_class_enum
     WHEN AVG(rating) > 6 THEN 'average'::quality_class_enum 
     ELSE 'bad'::quality_class_enum
   END) OVER (PARTITION BY actorId ORDER BY year) as prev_quality_class,
   LAG(year = (SELECT MAX(year) FROM actor_films)) OVER (PARTITION BY actorId ORDER BY year) as prev_is_active
 FROM actor_films
 GROUP BY actorId, actor, year
)
INSERT INTO actors_history_scd (
 actor_id,
 quality_class,
 is_active,
 start_date,
 end_date
)
SELECT 
 a.id as actor_id,
 c.curr_quality_class,
 c.curr_is_active,
 make_timestamp(c.year, 1, 1, 0, 0, 0)::timestamptz as start_date,
 CASE 
   WHEN LEAD(c.year) OVER (PARTITION BY c.actorId ORDER BY c.year) IS NOT NULL 
   THEN make_timestamp(LEAD(c.year) OVER (PARTITION BY c.actorId ORDER BY c.year), 1, 1, 0, 0, 0)::timestamptz
   ELSE NULL
 END as end_date
FROM actor_class_changes c
JOIN actors a ON a.name = c.actor
WHERE version_num = 1 
  OR curr_quality_class != prev_quality_class 
  OR curr_is_active != prev_is_active
ORDER BY c.actorId, c.year;