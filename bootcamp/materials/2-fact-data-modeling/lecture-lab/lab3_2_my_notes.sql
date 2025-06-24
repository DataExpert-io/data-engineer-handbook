-- after creating the array_metrics table for 3 days, let us do 
-- aggregation on it
with agg as (
    SELECT metric_name,
        month_start,
        ARRAY [SUM(metric_array[1]
),
SUM(metric_array [2]),
SUM(metric_array [3]) ] as summed_array
from array_metrics
GROUP BY metric_name,
    month_start
)
SELECT metric_name,
    month_start + CAST(CAST(index -1 AS TEXT) || 'day' as interval),
    elem as value
from agg
    cross join unnest(agg.summed_array) with ordinality as a(elem, index)