-- SELECT DISTINCT(current_season) FROM players_scd_table
-- CREATE TYPE scd_type AS (
--                     scoring_class scoring_class,
--                     is_active boolean,
--                     start_season INTEGER,
--                     end_season INTEGER
--                         )


/* using the SCD table we create all the possible groups that can 
be occurred 

*/

-- 1st player with same current season same as end season 
WITH last_season_scd AS (
      SELECT * FROM players_scd_table
      WHERE current_season=2021
      AND end_season = 2021
),

--- 2nd player with who ended their career before the current season
historical_season_scd AS (
      SELECT 
      player_name,
      scoring_class,
      is_active,
      start_season,
      end_season
      
       FROM players_scd_table
      WHERE current_season=2021
      AND end_season < 2021
),

--- current season 
this_season_scd AS (
      SELECT * FROM players_scd_table
      WHERE current_season=2022
),

-- recodes which are unchanged 
 unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_scd ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
/* calculating the changes for this we have to created a nested arrays
 that will keep hold of all changes 
*/ 
changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_scd ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),

-- unnesting the nested changed record 
unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),

new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_scd     ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )

-- uninon of all sets 
SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_season_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              )  AS t;

