SELECT
       date - first_active_date AS days_since_first_active,
       CAST(COUNT(CASE
           WHEN daily_active_state
                    IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1) FROM users_growth_accounting
GROUP BY date - first_active_date;
