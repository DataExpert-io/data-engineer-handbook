--------------- TOPIC :Slowly Changing Dim  ----------------

-- SELECT player_name,scoring_class,is_active 
-- FROM players
-- WHERE current_season=2001


--- SDC TABLE
-- DROP TABLE players_scd_table
-- create table players_scd_table
-- (
-- 	player_name text,
-- 	scoring_class scoring_class,
-- 	is_active boolean,
-- 	start_season integer,
-- 	end_season integer,
-- 	current_season INTEGER,
--     PRIMARY KEY (player_name,start_season,end_season,current_season)
    
-- );



--- Creating table with no filters 
INSERT INTO players_scd_table (
    player_name,
    scoring_class,
    is_active,
    start_season,
    end_season,
    current_season
)

-- 1st window function is to calculate previous_sc and previous_sc status 

WITH with_previous as (
SELECT 
      player_name,
      current_season,
      scoring_class,      
      is_active,
      LAG(scoring_class,1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_sc,
      LAG(is_active,1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active

 FROM players 
 WHERE current_season <=2022),

--  SELECT * ,
--        CASE 
--           WHEN scoring_class <> previous_sc 
--           THEN 1 
--        ELSE 0 END as  scoring_class_indicator,

--        CASE 
--           WHEN is_active <> previous_is_active 
--           THEN 1 
--        ELSE 0 END as  is_active_indicator

--  FROM with_previous

--- instead of 2 separate indictor creating 1 common indicator 

--- 2nd window function is to see the how often change occurred in previous_sc and previous_is_active
with_indicator AS (
    SELECT * ,
       CASE 
          WHEN scoring_class <> previous_sc  THEN 1 
          WHEN is_active <> previous_is_active THEN 1 
       ELSE 0 END as  change_indicator       
 FROM with_previous),

--- 3rd window function is to flatten table more based on change_indicator
 with_streaks AS (SELECT * ,
       SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
 FROM with_indicator)

--- Now we see the change activity across years and store this values in player_scd_table
 SELECT player_name,
        --streak_identifier,
        scoring_class,
        is_active,        
        MIN(current_season) as start_season,
        Max(current_season) as end_season,
        2022 as current_season
 FROM with_streaks
 GROUP BY player_name,streak_identifier,is_active,scoring_class
 ORDER BY player_name


------- Problem with this form too many windows 
------- we have to do to many window function without crunching data

-- this query is Prun of out-of memory problem and 
-- but if we have over streak changes too often then rows will increment drastically


