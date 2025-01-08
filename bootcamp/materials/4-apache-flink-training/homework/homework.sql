-- Query 1: Retrieve average web events for each user-host pair on Tech Creator
SELECT
    ip,
    host,
    AVG(num_logs) AS avg_events
FROM
    sessionized_events
WHERE
    host LIKE '%techcreator%'
GROUP BY
    ip, host;

-- Query 2: Compare average web events for specific hosts
SELECT
    host,
    AVG(num_logs) AS avg_events
FROM
    sessionized_events
WHERE
    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY
    host;

-- Query 3: Session summary by IP and host
SELECT
    ip,
    host,
    session_start,
    session_end,
    num_logs
FROM
    sessionized_events
ORDER BY
    session_start;
