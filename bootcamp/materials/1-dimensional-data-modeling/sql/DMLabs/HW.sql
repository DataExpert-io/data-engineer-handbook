-- SELECT * FROM actor_films

/*DDL for actors table: 
Create a DDL for an actors table with the following fields:
*/

-- CREATE TYPE films AS (
--     film TEXT,
--     votes INTEGER,
--     rating REAL,
--     filmid TEXT    
-- );



-- CREATE TYPE quality_class AS
--     ENUM ('star','good','avg','bad')


-- CREATE TABLE actors (
--     actor_id TEXT ,
--     actor TEXT ,    
--     films films[] , 
--     current_year INTEGER ,
--     last_release_year INTEGER,
--     quality_class quality_class ,
--     is_active BOOLEAN ,   
--     PRIMARY KEY (actor_id) 
-- );

-- SELECT MIN(year),Max(year)
-- FROM actor_films   --min year 1970 max year is 2021


/*Cumulative table generation query:
 Write a query that populates the actors table one year at a time.
 */


WITH 
last_year AS (
    SELECT  * 
    FROM actor_films 
    WHERE year = 2010
),
current_year AS (
    SELECT * FROM actor_films
    WHERE year = 2011
)
SELECT 
    COALESCE(cy.actor, ly.actor) AS actor_name,
    COALESCE(cy.actorid, ly.actorid) AS actor_id,
    
    COALESCE(ly.year,
            ARRAY[]::films[]
            ) || CASE WHEN cy.year IS NOT NULL THEN
                ARRAY[ROW(
                cy.film, 
                cy.votes,
                cy.rating,
                cy.filmid )::films]
                ELSE ARRAY[]::films[] END
            as films,         

    -- Scoring class based on average rating
    CASE
        WHEN cy.year IS NOT NULL THEN
            CASE 
                WHEN AVG(cy.rating) OVER (PARTITION BY cy.actor) > 8 THEN 'star'
                WHEN AVG(cy.rating) OVER (PARTITION BY cy.actor) > 7 THEN 'good'
                WHEN AVG(cy.rating) OVER (PARTITION BY cy.actor) > 6 THEN 'avg'
                ELSE 'bad'
            END::quality_class
        ELSE 
            ly.quality_class
    END AS quality_class, 

    -- Update the last release year
    CASE 
        WHEN cy.year IS NOT NULL THEN 0 
        ELSE ly.last_release_year + 1 
    END AS last_release_year,

    -- Check if the actor is active
    cy.year IS NOT NULL AS is_active,

    -- Determine the current year
    COALESCE(cy.year, ly.current_year + 1) AS current_year

FROM current_year cy
FULL OUTER JOIN last_year ly
ON cy.actor = ly.actor;
