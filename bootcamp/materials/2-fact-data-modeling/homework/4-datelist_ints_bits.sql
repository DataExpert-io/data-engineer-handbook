-- we want to check for each day if the user was active on that day
-- and how long has it been since he last came.
-- I am not sure why we do this tbh, I feel it would have been
-- easier to do it directly in the *user_devices_cumulated* table


with month_to_generate as (-- Lets generate month per month
	-- This could be more dynamic if we wanted
	select
		date('2023-01-01') as first_day_month
		, date('2023-01-31') as last_day_month
)

, user_devices as (
	select * from user_devices_cumulated where _current_date = (select last_day_month from month_to_generate)
)

, date_serie as (
	select generate_series((select first_day_month from month_to_generate), ((select last_day_month from month_to_generate)), interval '1 day') as valid_dates
)

, cross_join as (
	select
		ud.device_activity_datelist @> array [date(ds.valid_dates)] as is_active
		, ud._current_date - date(ds.valid_dates) as days_since
		, *
	from
		user_devices as ud
	cross join
		date_serie as ds
)

, final as (
	-- use the power of 2 to turn the dates into array of bits of 0s when not active and 1s when active
	-- then sum them up to get all the times in the month where they where active
	-- in the end, if the first digit is a 1, it means the user was active on the current date
	-- if the second is a 1 then the user was active yesterday, etc
	select
		user_id
		, browser_type
		, _current_date
		, cast(
			sum(case
					when is_active
						then pow(2, 31 - days_since)
					else 0
				end)::bigint
		as bit(32)) as datelist_int
	from
		cross_join
	group by
		user_id
		, browser_type
		, _current_date
)

select * from final