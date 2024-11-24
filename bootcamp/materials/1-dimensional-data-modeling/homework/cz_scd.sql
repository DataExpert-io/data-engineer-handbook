insert into actors_history_scd
with with_previous as (
	select *,
		lag(quality_class,1) over (partition by actorid order by current_year) as previous_quality_class,
		lag(isactive, 1) over (partition by actorid order by current_year) as previous_isactive
	from actors
	where current_year <= 2020
),
with_change_indicator as (
	select *,
		case
			when quality_class <> previous_quality_class then 1
			when isactive <> previous_isactive then 1
			else 0
		end as change_indicator
	from with_previous
),
with_streak_identifier as (
	select *,
		sum(change_indicator) over (partition by actorid order by current_year) as streak_identifier
	from with_change_indicator
)

select
    actor,
    actorid,
    quality_class,
    isactive,
    min(current_year) as start_date,
    max(current_year) as end_date
from with_streak_identifier
group by actor, actorid, streak_identifier,quality_class,isactive
order by actor,streak_identifier



------------------


-- incremental SCD, combine last year's 2020 SCD data with new incoming date from actors in 2021

-- create type actor_scd_type as (
--     quality_class quality_class,
--     isactive boolean,
--     start_date integer,
--     end_date integer
--     )



with last_year_scd as (
    select *
    from actors_history_scd
    where end_date = 2020
),
this_year_data as (
    select *
    from actors
    where current_year = 2021
),
historical_scd as (
    select *
    from actors_history_scd
    where end_date <2020
),
unchanged_records as (
    select
            ls.actor,
            ls.actorid,
            ls.quality_class,
            ls.isactive,
            ls.start_date,
            ts.current_year as end_date
        from last_year_scd ls
        join this_year_data ts
        on ls.actorid = ts.actorid
        where ls.quality_class = ts.quality_class
            and ls.isactive = ts.isactive
),
changed_records as (
    select
        ts.actor,
        ts.actorid,
        unnest(array[
                row (
                    ls.quality_class,
                    ls.isactive,
                    ls.start_date,
                    ls.end_date
                    )::actor_scd_type,
                row (
                    ts.quality_class,
                    ts.isactive,
                    ts.current_year,
                    ts.current_year
                    )::actor_scd_type
            ]) as records
        from this_year_data ts
        join last_year_scd ls
        on ts.actorid = ls.actorid
        where (
            ts.quality_class <> ls.quality_class
            or ts.isactive <> ls.isactive
                  )
),
unnest_changed_records as (
    select
            actor,
            actorid,
            (records::actor_scd_type).quality_class,
            (records::actor_scd_type).isactive,
            (records::actor_scd_type).start_date,
            (records::actor_scd_type).end_date
        from changed_records
),
new_records as (
    select
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.isactive,
            ts.current_year as start_date,
            ts.current_year as end_date
        from this_year_data ts
        left join last_year_scd ls
        on ts.actorid = ls.actorid
    where ls.actorid is null
),

this_year_scd as (select *
                  from new_records
                  union all
                  select *
                  from historical_scd
                  union all
                  select *
                  from unchanged_records
                  union all
                  select *
                  from unnest_changed_records)

select count(*) from this_year_scd


