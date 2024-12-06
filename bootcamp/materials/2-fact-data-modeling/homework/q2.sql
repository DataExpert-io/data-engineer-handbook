-- A DDL for user_devices_cumulated table that has:
--
-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
        -- Current date for the user
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
)