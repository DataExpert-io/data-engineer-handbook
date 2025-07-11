WITH change_indicators AS (
    SELECT
        actor_id, 
        actor_name, 
        quality_class, 
        is_active,
        current_year,
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) 
            <> quality_class AS quality_class_changed,
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) 
            <> is_active AS is_active_changed
    FROM actors
),
streak_identified AS (
    SELECT
        actor_id, 
        actor_name, 
        quality_class, 
        is_active,
        current_year,
        SUM(CASE 
                WHEN quality_class_changed THEN 1 
                WHEN is_active_changed THEN 1 
                ELSE 0
            END) OVER (PARTITION BY actor_id ORDER BY current_year) 
            AS streak_identifier
    FROM change_indicators
)
-- INSERT INTO actors_history_scd
SELECT
    actor_id, 
    actor_name, 
    quality_class, 
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date
FROM streak_identified
GROUP BY actor_id, actor_name, quality_class, is_active, streak_identifier
ORDER BY actor_id, actor_name, start_date;
