CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    browser_type TEXT,
    -- List of active days by browser type
    device_activity_datelist DATE[],
    -- Current date for the user
    record_date DATE,
    PRIMARY KEY(user_id, browser_type, record_date)
);