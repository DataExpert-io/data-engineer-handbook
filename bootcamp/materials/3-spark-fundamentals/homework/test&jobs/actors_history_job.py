# jobs/actors_history_scd_job.py

from pyspark.sql import SparkSession

query = """
WITH actor_change AS (
    SELECT 
        actor,
        year,
        quality_class,
        is_active,
        CASE 
            WHEN LAG(quality_class, 1, quality_class) OVER (PARTITION BY actor ORDER BY year) <> quality_class
              OR LAG(quality_class, 1, quality_class) OVER (PARTITION BY actor ORDER BY year) IS NULL
              OR LAG(is_active) OVER (PARTITION BY actor ORDER BY year) <> is_active
            THEN 1 
            ELSE 0 
        END AS did_change
    FROM actors
    WHERE year < 2010
),
streak_identifier AS (
    SELECT 
        actor,
        year,
        quality_class,
        COALESCE(is_active, false) AS is_active,
        SUM(did_change) OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
    FROM actor_change
),
final AS (
    SELECT 
        actor,
        quality_class,
        is_active,
        MIN(year) AS start_date,
        MAX(year) AS end_date
    FROM streak_identifier
    GROUP BY actor, quality_class, is_active, streak_identifier
)
SELECT * FROM final
ORDER BY actor, start_date
"""

def do_actor_scd_transformation(spark, actors_df, cutoff_year=2010):
    actors_df.createOrReplaceTempView("actors")
    rendered_query = query.format(cutoff_year=cutoff_year)
    return spark.sql(rendered_query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_history_scd") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read from Hive table
    actors_df = spark.table("actors")
    
    # Apply transformation
    output_df = do_actor_scd_transformation(spark, actors_df)

    # Write result to SCD table
    output_df.write.mode("overwrite").insertInto("actors_history_scd")
