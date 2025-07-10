WITH previous_values AS (
    SELECT
        actor_id, 
        actor_name, 
        quality_class, 
        is_active,
        current_year,
        LAG(quality_class, 1) OVER 
            (PARTITION BY actor_id ORDER BY current_year)
    FROM actors
),