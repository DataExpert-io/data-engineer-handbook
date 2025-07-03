
DROP TABLE IF EXISTS players_scd;
CREATE TABLE players_scd (
  player_name TEXT,
  scoring_class scoring_class,
  is_active BOOLEAN,
  start_season INTEGER,
  end_season INTEGER,
  current_season INTEGER,
  PRIMARY KEY (player_name, start_season)
);

WITH cte_previous AS (
  SELECT
    player_name,
    current_season,
    scoring_class,
    is_active,
    LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
  FROM players
)
SELECT *,
      CASE WHEN scoring_class != previous_scoring_class THEN 1 ELSE 0
        END AS scoring_class_change_indicator,
      CASE WHEN is_active != previous_is_active THEN 1 ELSE 0
        END AS is_active_change_indicator
FROM cte_previous;


WITH cte_previous AS (
SELECT
  player_name,
  current_season,
  scoring_class,
  is_active,
  LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
  LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
FROM players
),
  cte_indicator AS (
    SELECT *,
           CASE
             WHEN scoring_class != previous_scoring_class THEN 1
             WHEN is_active != previous_is_active THEN 1
             ELSE 0
             END AS change_indicator
    FROM cte_previous
)
SELECT *,
       SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM cte_indicator
;


WITH cte_previous AS (
  SELECT
    player_name,
    current_season,
    scoring_class,
    is_active,
    LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
  FROM players
),
cte_indicator AS (
 SELECT *,
        CASE
          WHEN scoring_class != previous_scoring_class THEN 1
          WHEN is_active != previous_is_active THEN 1
          ELSE 0
          END AS change_indicator
 FROM cte_previous
),
  cte_streaks AS (
SELECT *,
       SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM cte_indicator
)
SELECT player_name,
       streak_identifier,
       is_active,
       scoring_class,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season
FROM cte_streaks
WHERE player_name = 'Michael Jordan'
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name
;

INSERT INTO players_scd
WITH cte_previous AS (
  SELECT
    player_name,
    current_season,
    scoring_class,
    is_active,
    LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
  FROM players
  WHERE current_season <= 2021
),
     cte_indicator AS (
       SELECT *,
              CASE
                WHEN scoring_class != previous_scoring_class THEN 1
                WHEN is_active != previous_is_active THEN 1
                ELSE 0
                END AS change_indicator
       FROM cte_previous
     ),
     cte_streaks AS (
       SELECT *,
              SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
       FROM cte_indicator
     )
SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season,
       2021 AS current_season
FROM cte_streaks
GROUP BY player_name, is_active, scoring_class
;

SELECT * FROM players_scd WHERE player_name = 'Michael Jordan';

WITH last_season_scd AS (
  SELECT * FROM players_scd
  WHERE current_season = 2021
  AND end_season = 2021
),
this_season_data AS (
  SELECT * FROM players
  WHERE current_season = 2022
)
SELECT ts.player_name,
       ts.scoring_class, ts.is_active,
       ls.scoring_class, ls.is_active
FROM this_season_data ts
LEFT JOIN last_season_scd ls ON ls.player_name = ts.player_name;

WITH historical_scd AS (
  SELECT * FROM players_scd
  WHERE current_season = 2021
  AND end_season < 2021
) SELECT * FROM historical_scd;

CREATE TYPE scd_type AS (
  scoring_class scoring_class,
  is_active BOOLEAN,
  start_season INTEGER,
  end_season INTEGER
);

WITH last_season_scd AS (
  SELECT * FROM players_scd
  WHERE current_season = 2021
    AND end_season = 2021
),
historical_scd AS (
 SELECT
  player_name,
  scoring_class,
  is_active,
  start_season,
  end_season
 FROM players_scd
 WHERE current_season = 2021
   AND end_season < 2021
),
this_season_data AS (
 SELECT * FROM players
 WHERE current_season = 2022
),
unchanged_scd AS (
  SELECT ts.player_name,
         ts.scoring_class,
         ts.is_active,
         ls.start_season,
         ls.current_season as end_season
  FROM this_season_data ts
  JOIN last_season_scd ls ON ls.player_name = ts.player_name
  WHERE ts.scoring_class = ls.scoring_class
  AND ts.is_active = ls.is_active
),
changed_records AS (
  SELECT ts.player_name,
         unnest(
           ARRAY [
             ROW (
               ls.scoring_class,
               ls.is_active,
               ls.start_season,
               ls.end_season
               )::scd_type,
             ROW (
               ts.scoring_class,
               ts.is_active,
               ts.current_season,
               ts.current_season
               )::scd_type
             ]
         ) AS records
  FROM this_season_data ts
  JOIN last_season_scd ls ON ls.player_name = ts.player_name
  WHERE (ts.scoring_class != ls.scoring_class
    OR ts.is_active != ls.is_active)
),
  unnested_changed_records AS (
    select player_name,
           (records).scoring_class,
           (records).is_active,
           (records).start_season,
           (records).end_season
    from changed_records
  ),
  new_records AS (
    SELECT
      ts.player_name,
      ts.scoring_class,
      ts.is_active,
      ts.current_season AS start_season,
      ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE ls.player_name IS NULL
  )
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_scd
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;

