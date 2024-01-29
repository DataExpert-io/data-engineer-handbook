Insert into Jaswanthv.actors_history_scd
WITH LAGGED AS(
Select
actor,
CASE
 WHEN is_active THEN 1
 ELSE 0 END As is_active,
CASE
  WHEN
LAG(is_active,1) OVER(PARTITION BY actor ORDER BY current_year)  THEN 1
  ELSE 0 END As last_is_active,
quality_class,
CASE
  WHEN
LAG(quality_class,1) OVER(PARTITION BY actor ORDER BY current_year) IS NULL THEN quality_class
ELSE LAG(quality_class,1) OVER(PARTITION BY actor ORDER BY current_year) END As last_quality_class,
current_year
From Jaswanthv.actors where current_year <= 1918),
streaked As (
Select
actor,
is_active,
last_is_active,
quality_class,
last_quality_class,
current_year,
SUM (CASE WHEN ((is_active <> last_is_active) or (quality_class <> last_quality_class)) THEN 1 ELSE 0 END) OVER(PARTITION BY actor ORDER BY current_year) AS STREAK_IDENTIFIER
from LAGGED
),
Lead_Pre As
(
Select
actor,
STREAK_IDENTIFIER As streak_ident,
MAX(is_active) As is_active,
MAX(quality_class) As quality_class,
CAST(MIN(current_year) AS VARCHAR)|| '-01-01 00:00:00' As current_year
FROM streaked
Group by actor,STREAK_IDENTIFIER--,current_year
)
Select
actor,
--streak_ident,
quality_class,
cast(is_active As boolean) As is_active,
cast(current_year As timestamp) As start_date,
LEAD(cast(current_year As timestamp),1) Over (Partition by actor order by streak_ident) As end_date
from Lead_pre
