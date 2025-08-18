# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

- A query to deduplicate `game_details` from Day 1 so there's no duplicates

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

- A cumulative query to generate `device_activity_datelist` from `events`

- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
  
- The incremental query to generate `host_activity_datelist`

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

- An incremental query that loads `host_activity_reduced`
  - day-by-day

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)