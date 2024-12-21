from pyspark.sql import SparkSession


def do_host_activity_reduced_transformation(spark, dataframe, date_to_compute):
      query = f"""
	WITH today_data AS (
    SELECT
        host,
        DATE(event_time) AS date,
        COUNT(1) AS num_hits,
        COUNT(DISTINCT user_id) AS num_users
    FROM
        events
    WHERE
        DATE(event_time) = '{date_to_compute}'
    GROUP BY
        host,
        DATE(event_time)
),

yesterday_data AS (
    SELECT
        *
    FROM
        host_activity_reduced
    WHERE 
        month_start = TRUNC(DATE('{date_to_compute}'), 'MONTH')
),

final AS (
    SELECT
        COALESCE(td.host, yd.host) AS host,
        COALESCE(yd.month_start, TRUNC(td.date, 'MONTH')) AS month_start,
        CASE
            WHEN yd.hit_array IS NULL THEN 
				SEQUENCE(
                    0, 
                    DATEDIFF(td.date, COALESCE(yd.month_start, TRUNC(td.date, 'MONTH'))) - 1
                ) || ARRAY(COALESCE(td.num_hits, 0))
            ELSE CONCAT(yd.hit_array, ARRAY(COALESCE(td.num_hits, 0)))
        END AS hit_array,
        CASE
            WHEN yd.unique_visitors_array IS NULL THEN
                SEQUENCE(
                    0, 
                    DATEDIFF(td.date, COALESCE(yd.month_start, TRUNC(td.date, 'MONTH'))) - 1
                ) || ARRAY(COALESCE(td.num_users, 0))
            ELSE CONCAT(yd.unique_visitors_array, ARRAY(COALESCE(td.num_users, 0)))
        END AS unique_visitors_array,
        COALESCE(td.date, DATE_ADD(yd.max_computed_date, 1)) AS max_computed_date
    FROM
        today_data td
    FULL OUTER JOIN
        yesterday_data yd
        ON td.host = yd.host
)

SELECT * FROM final

	"""
      dataframe.createOrReplaceTempView("events")
      return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("host_activity_reduced") \
        .getOrCreate()
    output_df = do_host_activity_reduced_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("host_activity_reduced")