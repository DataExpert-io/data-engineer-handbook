from pyspark.sql import SparkSession

# SQL query version of the progress calculation
query = """
SELECT
    player_name,
    seasons[1].pts AS first_season,
    seasons[size(seasons)].pts AS last_season,
    CASE 
        WHEN seasons[1].pts <> 0 THEN seasons[size(seasons)].pts / seasons[1].pts
        ELSE 1 
    END AS progress
FROM players
WHERE current_season = 2016
  AND seasons[size(seasons)].pts IS NOT NULL
"""

# Job function using SQL
def do_player_progress_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("players")
    return spark.sql(query)

# Main entrypoint
def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("player_progress") \
        .enableHiveSupport() \
        .getOrCreate()

    players_df = spark.table("players")
    output_df = do_player_progress_transformation(spark, players_df)

    # Write to Hive table or destination
    output_df.write.mode("overwrite").insertInto("player_progress")
