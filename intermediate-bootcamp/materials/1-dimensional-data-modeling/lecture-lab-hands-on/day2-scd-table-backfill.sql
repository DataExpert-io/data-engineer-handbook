-- Re-Create players table using players.sql
-- and backfill data using load_players_table_day2.sql script in players table.

-- Create a Slowly Changing Dimension (SCD) table to track changes in scoring_class and is_active status
-- for each player across seasons
CREATE TABLE players_scd
(
    player_name    TEXT,
    scoring_class  scoring_class,
    is_active      BOOLEAN,
    start_season   INTEGER,
    end_season     INTEGER,
    current_season INTEGER, -- in production, this column can be data partitioning column
    PRIMARY KEY (player_name, start_season)
);

-- Identify changes in scoring_class and is_active status for each player across seasons
INSERT INTO players_scd
WITH with_previous AS (SELECT player_name,
                              current_season,
                              scoring_class,
                              is_active,
                              LAG(scoring_class, 1)
                              OVER (PARTITION BY player_name ORDER BY current_season)                   AS prev_scoring_class,
                              LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active
                       FROM players
                       WHERE current_season <= 2021), -- increment this value like from airflow for every new season to backfill scd table
     with_indicators AS (SELECT *,
                                CASE
                                    WHEN scoring_class <> prev_scoring_class THEN 1
                                    WHEN is_active <> prev_is_active THEN 1
                                    ELSE 0
                                    END AS change_indicator
                         FROM with_previous),
     with_streaks AS (SELECT *,
                             SUM(change_indicator)
                             OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
                      FROM with_indicators)


SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season,
       2021                AS current_season -- increment this value like from airflow for every new season to backfill scd table
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier;


/*
This query gonna be a lot more prone to out of memory exception and Skew,
and other interesting problems like that.
for eg- suppose few players are not slowly changing dimension,
they are changing every season, like their scoring_class is changing every season or other dimensions,
then while doing the aggregation in streak_identifier with window function or group by,
this blows up the cardinality for those players,
and all the data for that player is going to one node and that node might run out of memory.

This query works fine for millions of users like in Airbnb,
compared to billions of users in facebook where this might not work.

There is a line of scale when adding 1 or 2 zeros to millions of users,
where you cant throw everything into a window function or group by and expect it to work.
*/