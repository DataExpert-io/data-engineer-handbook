from pyspark.sql import SparkSession

query = """

WITH streak_started AS (
    SELECT player_name,
           current_season,
           scoring_class,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class       
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS did_change
    FROM players
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
                current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3
     )

     SELECT player_name, scoring_class, start_date, end_date
     FROM aggregated

"""


def do_player_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("players")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("players_scd") \
      .getOrCreate()
    output_df = do_player_scd_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("players_scd")

