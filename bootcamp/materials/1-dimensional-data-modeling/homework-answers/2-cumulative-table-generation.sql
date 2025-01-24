TRUNCATE actors;

WITH RECURSIVE years_desc AS (
 SELECT MAX(year) as year
 FROM actor_films
 UNION ALL
 SELECT year - 1
 FROM years_desc 
 WHERE year > (SELECT MIN(year) FROM actor_films)
),
yearly_films AS (
 SELECT 
   actorId,
   actor,
   year,
   ARRAY_AGG(
     ROW(film, votes, rating, gen_random_uuid())::film_struct  
     ORDER BY rating DESC
   ) as films,
   AVG(rating)::DECIMAL(3,1) as avg_rating,
   ROW_NUMBER() OVER (PARTITION BY actorId ORDER BY year DESC) as row_num
 FROM actor_films 
 GROUP BY actorId, actor, year
)
INSERT INTO actors (id, name, films, quality_class, is_active)
SELECT 
 gen_random_uuid(),
 y.actor,
 y.films,
 (CASE 
   WHEN y.avg_rating > 8 THEN 'star'::quality_class_enum
   WHEN y.avg_rating > 7 THEN 'good'::quality_class_enum
   WHEN y.avg_rating > 6 THEN 'average'::quality_class_enum
   ELSE 'bad'::quality_class_enum
 END),
 row_num = 1
FROM yearly_films y
JOIN years_desc yd ON y.year = yd.year
ORDER BY yd.year DESC;