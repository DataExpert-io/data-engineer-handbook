# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

1. A query to deduplicate `game_details` from Day 1 so there's no duplicates

2. A DDL for an `user_devices_cumulated` table that has:  
  2.1. a `device_activity_datelist` which tracks a users active days by `browser_type`  
  2.2. data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

3. A cumulative query to generate `device_activity_datelist` from `events`

4. A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

5. A DDL for `hosts_cumulated` table   
  5.1. a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
  
6. The incremental query to generate `host_activity_datelist`

7. A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

8. An incremental query that loads `host_activity_reduced`
  - day-by-day

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)