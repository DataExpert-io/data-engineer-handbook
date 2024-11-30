-- checking for duplicates 
-- SELECT player_id,game_id,count(1) FROM game_details GROUP BY 1,2

--DELETE FROM edges WHERE TRUE

------------- Player veterx
INSERT INTO edges

--- deduplicating the game data 
WITH game_dup AS ( 
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),

-- filtering only de-dup values 
filtered AS (
    SELECT * 
    FROM game_dup
    WHERE row_num = 1
),

--- self join to find player to player relations 
aggregated AS (
SELECT
    f1.player_id as subject_playerid,
    MAX(f1.player_name) as subject_player_name, --using Max to de-dup names
    f2.player_id as object_playerid,
    MAX(f2.player_name) as object_player_name,
    CASE 
        WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'share_team'::edge_type
        ELSE 'plays_against'::edge_type  -- case to identify plays share team or not 
    END AS edge_type,
    COUNT(*) AS num_game,
    SUM(f1.pts) as subject_point,
    SUM(f2.pts) as object_points
FROM filtered f1 
JOIN filtered f2 
ON f1.game_id = f2.game_id
AND f1.player_name <> f2.player_name  --player names are !=
WHERE f1.player_id>f2.player_id -- making sure each pair comes only 1 time eg: a-b or b-a same only
GROUP BY 
f1.player_id,
f2.player_id,
CASE 
    WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'share_team'::edge_type
    ELSE 'plays_against'::edge_type
END)


SELECT 
      subject_playerid as subject_identifier,
      'player' :: vertex_type as subject_type,
      object_playerid as object_identifier,
      'player' :: vertex_type as object_type,
      edge_type as edge_type,
      json_build_object(
        'num_game', num_game,
        'subject_point',subject_point,
        'object_point' ,object_points
      )
FROM aggregated

 

-- DELETE  FROM edges WHERE TRUE
-------- Game vertex ------------
------    player_game_edges --------
-- WITH game_dup AS ( 
--     SELECT *, 
--            ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
--     FROM game_details
-- )

-- INSERT INTO edges
-- SELECT 
--       player_id AS subject_identifier,
--       'player':: vertex_type AS subject_type,
--       game_id AS object_identifier,
--       'game' :: vertex_type AS object_identifier,
--       'plays_in' :: edge_type AS edge_type,
--       json_build_object (
--         'start_position', start_position,
--         'pts' ,pts,
--         'team_id',team_id,
--         'team_abbreviation', team_abbreviation
--       ) as properties
-- FROM game_dup WHERE row_num=1

-- SELECT * FROM edges