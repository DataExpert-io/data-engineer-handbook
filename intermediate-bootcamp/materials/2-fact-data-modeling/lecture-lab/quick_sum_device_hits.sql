SELECT
       month_start,
       SUM(hit_array[1]) as num_hits_mar_1,
       SUM(hit_array[2]) AS num_hits_mar_2
FROM monthly_user_site_hits
WHERE date_partition = DATE('2023-03-03')
GROUP BY 1
