# SQL scripts for performing queries on the graph
SELECT
    v.properties ->> 'player_name' AS player_name,
    e.object_identifier AS opponent_identifier,
    e.properties ->> 'num_games' AS games_against_opponent
FROM vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
