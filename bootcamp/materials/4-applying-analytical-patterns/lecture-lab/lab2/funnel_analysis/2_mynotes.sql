-- Save the clean_events table 
-- CREATE TABLE clean_events AS
-- WITH deduped_events AS (
--     SELECT url,
--         host,
--         user_id,
--         event_time
--     FROM events
--     GROUP BY 1, 2, 3, 4
-- ),
-- clean_events AS (
--     SELECT *,
--         DATE(event_time) AS event_date
--     FROM deduped_events
--     WHERE user_id IS NOT NULL
--     ORDER BY user_id, event_time
-- )
-- SELECT * FROM clean_events;
-- ======================================================= 
-- to check the output of the self join
SELECT *
FROM clean_events ce1
    JOIN clean_events ce2 ON ce2.user_id = ce1.user_id
    AND ce2.event_date = ce1.event_date
    AND ce2.event_time > ce1.event_time
ORDER BY ce1.user_id,
    ce1.event_time;