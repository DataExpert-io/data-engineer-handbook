SELECT
    v.properties->>'player_name',
    MAX(CAST(e.properties->>'pts' AS INTEGER))

FROM vertices v JOIN edges e
    ON e.subject_identifier = v.identifier
        AND e.subject_type = v.type
GROUP BY 1
order by 2 DESC

SELECT
    v.properties->>'player_name',
    e.object_identifier,
    CAST(v.properties->>'number_of_games' AS real)/CASE WHEN CAST(v.properties->>'total_points' AS real) = 0 THEN 1 ELSE CAST(v.properties->>'total_points' AS real) END,
    e.properties->>'subject_points',
    e.properties->>'num_games'
FROM vertices v JOIN edges e
    ON v.identifier = e.subject_identifier
        AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type