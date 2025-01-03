
create table user_devices_cumulated (
	-- I prefer having a row per user per browser type
	user_id text
	, browser_type text
	, device_activity_datelist date[]
	-- We are building this table as a cumulated (as if it was day per day)
	, _current_date date
	, primary key (user_id, browser_type, _current_date)
);