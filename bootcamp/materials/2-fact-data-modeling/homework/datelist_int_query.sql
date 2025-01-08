-- Step 1: Generate a starter dataset with activity status for each day in the specified range
WITH starter AS (
    SELECT uc.device_activity_datelist @> ARRAY [DATE(d.valid_date)]   AS is_active,
        -- Check if the user was active on a given date by comparing the date list with the generated date
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           uc.user_id,
           uc.browser_type
    FROM user_devices_cumulated uc
             CROSS JOIN
        -- Create a series of dates for the month (e.g., from '2022-12-31' to '2023-01-31')
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
),
-- Step 2: Convert the daily activity status into a compact bitwise representation
bits AS (
    SELECT user_id,
        browser_type,
        SUM(
            CASE
                -- If the user was active on a given day, add the corresponding power of 2
                WHEN is_active THEN POW(2, 32 - days_since)
                ELSE 0 END)::bigint::bit(32) AS datelist_int
    FROM starter
    GROUP BY user_id, browser_type
)
-- Step 3: Query the compact representation to calculate monthly statistics
SELECT
        user_id,
        datelist_int,
        browser_type,
        -- Check if the user was active at least once during the month
        -- BIT_COUNT checks in a number of 0 and 1, which element is set to 1
        BIT_COUNT(datelist_int)>0 as monthly_active,
        -- Count the total number of active days based on the bit representation
        BIT_COUNT(datelist_int) as num_days_active
FROM bits