from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, array_union

def do_hosts_cumulated_transformation(
    spark: SparkSession,
    dataframe,
    yesterday_date: str,
    today_date: str
):
    """
    Performs the hosts cumulated transformation by comparing yesterday's and today's events.

    Args:
        spark (SparkSession): The active Spark session.
        dataframe: The input DataFrame containing the `hosts_cumulated` table.
        yesterday_date (str): The date string for yesterday in 'YYYY-MM-DD' format.
        today_date (str): The date string for today in 'YYYY-MM-DD' format.

    Returns:
        DataFrame: Transformed hosts cumulated DataFrame.
    """
    dataframe.createOrReplaceTempView("hosts_cumulated")

    # Load yesterday's data
    yesterday_df = spark.sql(f"""
        SELECT *
        FROM hosts_cumulated
        WHERE date = DATE('{yesterday_date}')
    """)

    # Load today's events
    today_df = spark.sql(f"""
        SELECT
            host,
            DATE(event_time) AS date_active,
            ROW_NUMBER() OVER (PARTITION BY host ORDER BY event_time) AS counter
        FROM events
        WHERE DATE(event_time) = DATE('{today_date}')
    """).filter(col("counter") == 1)

    # Perform the FULL OUTER JOIN and transformation logic
    result_df = yesterday_df.alias("y").join(
        today_df.alias("t"),
        col("y.host") == col("t.host"),
        "full_outer"
    ).selectExpr(
        "COALESCE(y.host, t.host) AS host",
        """
        CASE
            WHEN y.dates_active IS NULL THEN ARRAY(t.date_active)
            WHEN t.date_active IS NULL THEN y.dates_active
            ELSE array_union(y.dates_active, ARRAY(t.date_active))
        END AS dates_active
        """,
        "COALESCE(t.date_active, y.date + INTERVAL 1 DAY) AS date"
    )

    return result_df

def main():
    """
    Main function to execute the hosts cumulated transformation.
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("hosts_cumulated") \
        .getOrCreate()

    # Retrieve the input DataFrame
    input_df = spark.table("hosts_cumulated")

    # Perform the transformation
    output_df = do_hosts_cumulated_transformation(spark, input_df, '2023-01-30', '2023-01-31')

    # Write the result back to the table
    output_df.write.mode("overwrite").saveAsTable("hosts_cumulated")

if __name__ == "__main__":
    main()
