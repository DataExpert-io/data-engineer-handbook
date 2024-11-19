WITH teams_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams
)

SELECT
    team_id AS identifier,
    'team'::vertex_type AS type,
    JSON_BUILD_OBJECT(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    )
FROM teams_deduped
WHERE row_num = 1
