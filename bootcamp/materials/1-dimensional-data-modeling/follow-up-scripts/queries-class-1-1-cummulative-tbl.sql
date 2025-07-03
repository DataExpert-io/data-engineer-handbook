SELECT *
FROM player_seasons;

SELECT season, COUNT(*)
FROM player_seasons
GROUP BY season;

SELECT season, COUNT(*)
FROM player_seasons
WHERE player_name = 'Michael Jordan'
GROUP BY season;

-- CREATE TYPE SEASON_STATS AS (
--   SEASON INTEGER,
--   gp INTEGER,
--   pts REAL,
--   reb REAL,
--   ast REAL
-- );

-- create table players
CREATE TABLE players
(
  player_name    TEXT,
  height         TEXT,
  college        TEXT,
  country        TEXT,
  draft_year     TEXT,
  draft_round    TEXT,
  draft_number   TEXT,
  season_stats   season_stats[],
  current_season INTEGER,
  PRIMARY KEY (player_name, current_season)
);

-- commulative structure
WITH last_season AS (SELECT * FROM players WHERE current_season = 1995),
     this_season AS (SELECT * FROM player_seasons WHERE season = 1996)
SELECT
-- *
COALESCE(t.player_name, l.player_name)   AS player_name,
COALESCE(t.height, l.height)             AS height,
COALESCE(t.college, l.college)           AS college,
COALESCE(t.country, l.country)           AS country,
COALESCE(t.draft_year, l.draft_year)     AS draft_year,
COALESCE(t.draft_round, l.draft_round)   AS draft_round,
COALESCE(t.draft_number, l.draft_number) AS draft_number,
CASE
  WHEN l.season_stats IS NULL THEN
    ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
  WHEN t.season IS NOT NULL THEN l.season_stats ||
                                 ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
  ELSE l.season_stats
  END                                    AS season_stats,
COALESCE(t.season, l.current_season + 1) AS current_season
FROM this_season t
       FULL OUTER JOIN last_season l
                       ON t.player_name = l.player_name;

-- INSERT INTO
INSERT INTO players
WITH last_season AS (SELECT * FROM players WHERE current_season = 2000),
     this_season AS (SELECT * FROM player_seasons WHERE season = 2001)
SELECT COALESCE(t.player_name, l.player_name)   AS player_name,
       COALESCE(t.height, l.height)             AS height,
       COALESCE(t.college, l.college)           AS college,
       COALESCE(t.country, l.country)           AS country,
       COALESCE(t.draft_year, l.draft_year)     AS draft_year,
       COALESCE(t.draft_round, l.draft_round)   AS draft_round,
       COALESCE(t.draft_number, l.draft_number) AS draft_number,
       CASE
         WHEN l.season_stats IS NULL THEN
           ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
         WHEN t.season IS NOT NULL THEN
           l.season_stats || ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
         ELSE l.season_stats
         END                                    AS season_stats,
       COALESCE(t.season, l.current_season + 1) AS current_season
FROM this_season t
  FULL OUTER JOIN last_season l ON t.player_name = l.player_name;

SELECT * FROM players where current_season = (select max(current_season) from players);
select count(*) from players;
-- delete from players where current_season != (select max(current_season) from players);
-- michael's analysis
select player_name, unnest(season_stats) as season_stats
from players where current_season = 2001 and player_name = 'Michael Jordan';

WITH unnested AS (
  SELECT player_name,
         unnest(season_stats)::season_stats as season_stats
  FROM players where current_season = 2001 and player_name = 'Michael Jordan'
)
SELECT player_name, (season_stats).* FROM unnested;

DROP TABLE players;

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

-- create table players v2
CREATE TABLE players
(
  player_name    TEXT,
  height         TEXT,
  college        TEXT,
  country        TEXT,
  draft_year     TEXT,
  draft_round    TEXT,
  draft_number   TEXT,
  season_stats   season_stats[],
  scoring_class  scoring_class,
  years_since_last_season INTEGER,
  current_season INTEGER,
  PRIMARY KEY (player_name, current_season)
);

-- INSERT INTO v2
INSERT INTO players
WITH last_season AS (SELECT * FROM players WHERE current_season = 2000),
     this_season AS (SELECT * FROM player_seasons WHERE season = 2001)
SELECT
  COALESCE(t.player_name, l.player_name)   AS player_name,
  COALESCE(t.height, l.height)             AS height,
  COALESCE(t.college, l.college)           AS college,
  COALESCE(t.country, l.country)           AS country,
  COALESCE(t.draft_year, l.draft_year)     AS draft_year,
  COALESCE(t.draft_round, l.draft_round)   AS draft_round,
  COALESCE(t.draft_number, l.draft_number) AS draft_number,
  CASE
    WHEN l.season_stats IS NULL THEN
      ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
    WHEN t.season IS NOT NULL THEN
      l.season_stats || ARRAY [ROW (t.season, t.gp, t.pts, t.ast, t.reb)::season_stats]
  ELSE l.season_stats END                  AS season_stats,
  CASE WHEN t.season IS NOT NULL THEN
    CASE WHEN t.pts > 20 THEN 'star'
      WHEN t.pts > 15 THEN 'good'
      WHEN t.pts > 10 THEN 'average'
    ELSE 'bad' END::scoring_class
  ELSE l.scoring_class END AS scoring_class,
  CASE WHEN t.season IS NOT NULL THEN 0
    ELSE l.years_since_last_season + 1 END AS years_since_last_season,
  COALESCE(t.season, l.current_season + 1) AS current_season
FROM this_season t
  FULL OUTER JOIN last_season l ON t.player_name = l.player_name;

SELECT * FROM players WHERE current_season = (SELECT MAX(current_season) FROM players);
SELECT * FROM players WHERE current_season = 2001 and player_name = 'Michael Jordan';
SELECT * FROM players WHERE current_season = 2000 and player_name = 'Michael Jordan';

SELECT player_name,
       season_stats[1] AS first_sesason,
       season_stats[CARDINALITY(season_stats)] AS latest_sesason
       FROM players
-- optional
WHERE season_stats[CARDINALITY(season_stats)].season = 2001
;

SELECT player_name,
       season_stats[1].pts AS first_sesason,
       season_stats[CARDINALITY(season_stats)].pts AS latest_sesason
FROM players;


SELECT player_name,
       season_stats[CARDINALITY(season_stats)].pts /
       CASE WHEN season_stats[1].pts = 0 THEN 1 ELSE season_stats[1].pts END AS pts_rate
FROM players
WHERE current_season = 2001
-- optional
ORDER BY 2 DESC
;

select * from players where player_name = 'Don MacLean';

SELECT player_name,
       season_stats[CARDINALITY(season_stats)].pts /
       CASE WHEN season_stats[1].pts = 0 THEN 1 ELSE season_stats[1].pts END AS pts_rate
FROM players
WHERE current_season = 2001 AND scoring_class = 'star'
;
