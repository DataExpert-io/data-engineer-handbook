from pyspark.sql import SparkSession

def do_game_details_transformation(spark, dataframe, row_num):
    query = f"""
        WITH deduped AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) AS row_num
            FROM game_details
        )
        SELECT * 
        FROM deduped
        WHERE row_num = '{row_num}'
    """
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

def main():
    row_num = 1
    spark = SparkSession.builder \
        .master("local") \
        .appName("game_details_dedupe") \
        .getOrCreate()
    output_df = do_game_details_transformation(spark, spark.table("game_details"), row_num)
    output_df.write.mode("overwrite").insertInto("game_details_dedupe")