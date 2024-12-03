insert into actors_history_scd  
with previous as (
select actor,
actorid,
films,
current_year,
quality_class,
lag(quality_class, 1) over (partition by actor, actorid order by current_year) as previous_quality_class,
is_active,
lag(is_active, 1) over (partition by actor, actorid order by current_year) as previous_is_active
from actors a),

indicators as (
select *,
case when quality_class <> previous_quality_class then 1
when is_active <> previous_is_active then 1 else 0 end as change_indicator
from previous),

streaks as (
select *,
sum(change_indicator) over (partition by actor, actorid order by current_year) as streak_identifier
from indicators)

select actor, actorid, quality_class, is_active,
min(current_year) as start_year,
max(current_year) as end_year,
1981 as current_year
from streaks
group by actor, actorid, quality_class, is_active, streak_identifier
order by actor, streak_identifier asc