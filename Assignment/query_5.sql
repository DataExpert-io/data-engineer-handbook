-- Inserts for delta
Insert into Jaswanthv.actors_history_scd
With last_snapshot As
(
Select * from Jaswanthv.actors_history_scd where end_date is null
),
latest_snapshot As
(
Select
actor,
MAX(quality_class) As quality_class,
CAST((CASE When MAX(is_active) Then 1
     Else 0 END) AS boolean) As is_active,
CAST(CAST(MIN(current_year) AS VARCHAR)|| '-01-01 00:00:00' As timestamp) As start_date
 from Jaswanthv.actors Where
current_year = 1919
Group by actor
),
delta As (
select
COALESCE(lsd.actor, ls.actor) As actor,
COALESCE(lsd.quality_class, ls.quality_class) As quality_class,
COALESCE(lsd.is_active, ls.is_active) As is_active,
COALESCE(lsd.start_date, lsd.start_date) As start_date,
CASE WHEN (ls.quality_class IS NULL Or ls.is_active IS NULL) Then 1
WHEN
((lsd.quality_class <> ls.quality_class) or (lsd.is_active <> ls.is_active)) Then 2
Else 0 End As did_change
from last_snapshot ls FULL OUTER JOIN latest_snapshot lsd on ls.actor = lsd.actor
)
select
actor,
quality_class,
is_active,
start_date,
null As end_date
from delta where did_change in (1,2)

-- Expiring old records
Insert into Jaswanthv.actors_history_scd
With Upd As
(
Select actor, count(*) As cnt from Jaswanthv.actors_history_scd
where end_date is null group by actor
having count(*) > 1
),
scd As (
select
ahs.actor,
ahs.quality_class,
ahs.is_active,
ahs.start_date,
LAG(ahs.start_date,1 ) OVER(PARTITION BY ahs.actor ORDER BY start_date Desc) As end_date
from Jaswanthv.actors_history_scd
ahs join Upd u
on ahs.actor = u.actor
where end_date is null
)
Select
actor,
quality_class,
is_active,
start_date,
end_date from scd where end_date is not null

/*
Delete  Jaswanthv.actors_history_scd f join del
on f.actor = del.actor and f.start_date = del.start_date
and f.end_date is null
With Upd As
(
Select actor, count(*) As cnt from Jaswanthv.actors_history_scd
where end_date is null group by actor
having count(*) > 1
),
scd As (
select
ahs.actor,
MAX(ahs.start_date) As start_date
from Jaswanthv.actors_history_scd
ahs join Upd u
on ahs.actor = u.actor
where end_date is null
Group by ahs.actor
),
del As (
Select
ahs.*
 from scd s join Jaswanthv.actors_history_scd ahs
 on s.actor = ahs.actor
where ahs.end_date is null and s.start_date <> ahs.start_date
)
*/
