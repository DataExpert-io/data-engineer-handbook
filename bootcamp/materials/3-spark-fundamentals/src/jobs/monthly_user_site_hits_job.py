from pyspark.sql import SparkSession





def do_monthly_user_site_hits_transformation(spark, dataframe, ds):
    query = f"""
    SELECT
           month_start,
           SUM(COALESCE(hit_array[0], 0)) as num_hits_first_day,
           SUM(COALESCE(hit_array[1], 0)) AS num_hits_second_day,
           SUM(COALESCE(hit_array[2], 0)) as num_hits_third_day
    FROM monthly_user_site_hits
    WHERE date_partition = '{ds}'
    GROUP BY month_start
    """
    dataframe.createOrReplaceTempView("monthly_user_site_hits")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("players_scd") \
      .getOrCreate()
    output_df = do_monthly_user_site_hits_transformation(spark, spark.table("monthly_user_site_hits"), ds)
    output_df.write.mode("overwrite").insertInto("monthly_user_site_hits_agg")