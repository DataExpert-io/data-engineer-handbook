
CREATE TYPE vertex_type AS ENUM ('player', 'team', 'game');

CREATE TABLE vertices (
  identifier TEXT,
  type vertex_type,
  properties JSON,
  PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type AS ENUM ('plays_against',
  'shares_team', 'plays_in', 'plays_on');

CREATE TABLE edges (
  subject_identifier TEXT,
  subject_type vertex_type,
  object_identifier TEXT,
  object_type vertex_type,
  edge_type edge_type,
  properties JSON,
  PRIMARY KEY (subject_identifier,
              subject_type,
              object_identifier,
              object_type,
              edge_type)
);


SELECT * FROM game_details LIMIT 100;
SELECT * FROM games LIMIT 100;

INSERT INTO vertices
SELECT
  game_id AS identifier,
  'game'::vertex_type AS type,
  json_build_object(
  'pts_home', pts_home,
  'pts_away', pts_away,
  'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
  )
FROM games;


INSERT INTO vertices
WITH players_agg AS (
SELECT player_id AS identifier,
       MAX(player_name) AS player_name,
       COUNT(1) AS number_of_games,
       SUM(pts) AS total_points,
       ARRAY_AGG(DISTINCT team_id) AS teams
FROM game_details
GROUP BY player_id
)
SELECT identifier,
        'player'::vertex_type,
        json_build_object('player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams)
FROM players_agg;


SELECT * FROM teams LIMIT 100;

INSERT INTO vertices
WITH teams_dedup AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id) AS rn
  FROM teams
)

SELECT
  team_id AS identifier,
  'team'::vertex_type AS type,
  json_build_object(
  'abbreviation', abbreviation,
  'nickname', nickname,
  'city', city,
  'arena', arena,
  'year_founded', yearfounded
  ) AS properties
FROM teams_dedup
WHERE rn = 1;


SELECT type, count(*) FROM vertices GROUP BY type ;


SELECT * FROM game_details LIMIT 100;

SELECT player_id, game_id, count(*) FROM game_details GROUP BY player_id, game_id HAVING count(*) > 1;


INSERT INTO edges
WITH game_details_deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) rn
  FROM game_details
)
SELECT
  player_id AS subject_identifier,
  'player'::vertex_type AS subject_type,
  game_id AS object_identifier,
  'game'::vertex_type AS object_type,
  'plays_in'::edge_type AS edge_type,
  json_build_object(
  'start_position', start_position,
  'pts', pts,
  'team_id', team_id,
  'team_abbreviation', team_abbreviation
  ) AS properties
FROM game_details_deduped
WHERE rn = 1;

SELECT * FROM vertices v
  JOIN edges e ON v.identifier = e.subject_identifier
                    AND e.subject_type = v.type;


SELECT
  v.properties->>'player_name' AS player_name,
  MAX(CAST(e.properties->>'pts' AS INTEGER)) AS max_pts
FROM vertices v
  JOIN edges e ON v.identifier = e.subject_identifier
  AND e.subject_type = v.type
GROUP BY 1 ORDER BY 2 DESC ;


-- DOUBLE EDGED QUERY (A PLAYS B AND B PLAYS A)
WITH game_details_deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) rn
  FROM game_details
),
 game_details_filtered AS (
  SELECT * FROM game_details_deduped WHERE rn = 1
 )
SELECT
  f1.player_id AS subject_identifier,
  f1.player_name,
  'player'::vertex_type AS subject_type,
  f2.player_id AS object_identifier,
  f2.player_name,
  'player'::vertex_type AS object_type,
  CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
    ELSE 'plays_against'::edge_type END AS edge_type,
  COUNT(*) AS num_games,
  SUM(f1.pts) AS subject_pts,
  SUM(f2.pts) AS object_pts
FROM game_details_filtered f1
JOIN game_details_filtered f2 ON f1.game_id = f2.game_id
  AND f1.player_name != f2.player_name
GROUP BY 1,2,4,5,
 CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
      ELSE 'plays_against'::edge_type END;


-- SINGLE EDGED QUERY
WITH game_details_deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) rn
  FROM game_details
),
     game_details_filtered AS (
       SELECT * FROM game_details_deduped WHERE rn = 1
     )
SELECT
  f1.player_id AS subject_identifier,
  f1.player_name AS subject_name,
  'player'::vertex_type AS subject_type,
  f2.player_id AS object_identifier,
  f2.player_name AS object_name,
  'player'::vertex_type AS object_type,
  CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
       ELSE 'plays_against'::edge_type END AS edge_type,
  COUNT(*) AS num_games,
  SUM(f1.pts) AS subject_pts,
  SUM(f2.pts) AS object_pts
FROM game_details_filtered f1
       JOIN game_details_filtered f2 ON f1.game_id = f2.game_id
  AND f1.player_name != f2.player_name
WHERE f1.player_id > f2.player_id
GROUP BY 1,2,4,5,
         CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
              ELSE 'plays_against'::edge_type END;



INSERT INTO edges
WITH game_details_deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) rn
  FROM game_details
),
 game_details_filtered AS (
   SELECT * FROM game_details_deduped WHERE rn = 1
 ),
  game_details_aggregated AS (
SELECT
  f1.player_id AS subject_identifier,
  'player'::vertex_type AS subject_type,
  f2.player_id AS object_identifier,
  'player'::vertex_type AS object_type,
  CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
       ELSE 'plays_against'::edge_type END AS edge_type,
  COUNT(*) AS num_games,
  SUM(f1.pts) AS subject_pts,
  SUM(f2.pts) AS object_pts
FROM game_details_filtered f1
       JOIN game_details_filtered f2 ON f1.game_id = f2.game_id
  AND f1.player_name != f2.player_name
WHERE f1.player_id > f2.player_id
GROUP BY 1,3,5
)
select
  subject_identifier,
  subject_type,
  object_identifier,
  object_type,
  edge_type,
  json_build_object(
    'num_games', num_games,
    'subject_pts', subject_pts,
    'subject_pts', object_pts
  )
from game_details_aggregated;


select * from edges where subject_type = 'player' and object_type = 'player';

select * from edges e
  join vertices v ON e.subject_identifier = v.identifier
                       AND e.subject_type = v.type
where e.object_type = 'player'::vertex_type;

select
  v.properties ->> 'player_name' as player_name,
  e.object_identifier,
  cast(v.properties ->> 'number_of_games' as real) /
  case when cast(v.properties ->> 'total_points' as real) = 0
    then 1 else cast(v.properties ->> 'total_points' as real)
  end games_per_points,
  e.properties ->> 'subject_pts' as subject_pts,
  e.properties ->> 'num_games' as num_games
from edges e
join vertices v ON e.subject_identifier = v.identifier
  AND e.subject_type = v.type
where e.object_type = 'player'::vertex_type