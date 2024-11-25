INSERT INTO edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered AS (
 SELECT * FROM deduped
 WHERE row_num = 1
), aggregated AS (
    SELECT
        f1.player_id subject_player_id,
        f2.player_id object_player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        COUNT(1) AS num_games,
        MAX(f1.player_name) subject_player_name,
        MAX(f2.player_name) object_player_name,
        sum(f1.pts) AS subject_points,
        sum(f2.pts) as object_points
    FROM filtered f1
        JOIN filtered f2
            ON f1.game_id = f2.game_id
                AND f1.player_name <> f2.player_name
    WHERE f1.player_id > f2.player_id
    GROUP BY
        f1.player_id,
        f2.player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
SELECT
    subject_player_id AS subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type as edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    )
FROm aggregated