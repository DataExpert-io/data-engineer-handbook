SELECT *
FROM clean_events ce1
    JOIN clean_events ce2 ON ce2.user_id = ce1.user_id
    AND ce2.event_date = ce1.event_date
    AND ce2.event_time > ce1.event_time
ORDER BY ce1.user_id,
    ce1.event_time;