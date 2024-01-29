INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
--CROSS JOIN UNNEST(films) As t(film, votes, rating, film_id)
where current_year = 1913),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1914)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor



INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
where current_year = 1914),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1915)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor


INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
where current_year = 1915),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1916)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor


INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
where current_year = 1916),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1917)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor


INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
where current_year = 1917),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1918)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor


INSERT into Jaswanthv.actors
With last_year As (
Select * from Jaswanthv.actors
where current_year = 1918),
current_year As
(Select
*,
 CASE WHEN rating <= 6 THEN 'bad'
           WHEN rating > 6 And rating <= 7 Then 'average'
           WHEN rating > 7 And rating <= 8 Then 'good'
           Else 'star' End As quality_class
from bootcamp.actor_films
where year = 1919)
Select
COALESCE(cy.actor, ly.actor) As actor,
COALESCE(cy.actor_id, ly.actor_id) As actor_id,
CASE
    WHEN cy.film IS NULL THEN ly.films
    WHEN cy.film IS NOT NULL AND ly.films IS NULL THEN
ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)]
    WHEN cy.film IS NOT NULL AND ly.films IS NOT NULL THEN ARRAY[ROW(cy.film,cy.votes,cy.rating,cy.film_id)] || ly.films END
 As films,
CASE WHEN cy.film IS NOT NULL then cy.quality_class
     ELSE ly.quality_class END As quality_class,
cy.film IS NOT NULL as is_active,
COALESCE(cy.year, ly.current_year+1) As current_year
FROM last_year ly FULL OUTER JOIN current_year cy on ly.actor = cy.actor
