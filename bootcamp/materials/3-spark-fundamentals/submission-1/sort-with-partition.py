from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from bucket_join_by_match_id import bucket_join

# SQL Queries
sp_average_most_kills_per_game_query = '''
    WITH cte_player_stats AS (
        SELECT
            player_gamertag,
            COUNT(match_id) AS num_of_matches,
            SUM(player_total_kills) AS total_kills
        FROM SP_average_most_kills_per_game
        GROUP BY player_gamertag
    )
    SELECT *,
           total_kills / num_of_matches AS kills_per_match
    FROM cte_player_stats
    ORDER BY kills_per_match DESC
    LIMIT 1
'''

sp_most_played_playlist_query = '''
    WITH cte_count_duplicates AS (
        SELECT
            playlist_id,
            match_id,
            ROW_NUMBER() OVER (PARTITION BY match_id, playlist_id ORDER BY playlist_id) AS COUNTER
        FROM SP_most_played_playlist_query
    )
    SELECT playlist_id AS reproductions
    FROM cte_count_duplicates
    WHERE COUNTER = 1
    GROUP BY playlist_id
    ORDER BY COUNT(match_id) DESC
    LIMIT 1
'''

sp_most_played_map_query = '''
    WITH cte_count_duplicates AS (
        SELECT
            map_variant_id,
            match_id,
            ROW_NUMBER() OVER (PARTITION BY match_id, map_variant_id ORDER BY map_variant_id) AS COUNTER
        FROM SP_most_played_map_query
    )
    SELECT map_variant_id AS most_played
    FROM cte_count_duplicates
    WHERE COUNTER = 1
    GROUP BY map_variant_id
    ORDER BY COUNT(map_variant_id) DESC
    LIMIT 1
'''


def sort_partitions(spark_instance: SparkSession, subsection: str):
    """
    Applies partitioning and sorting to a specified subsection of the data.

    Args:
        spark_instance (SparkSession): The active Spark session.
        subsection (str): The subsection to process ('a', 'b', or 'c').

    Returns:
        DataFrame: The sorted and partitioned DataFrame based on the subsection.
    """
    bucket_join_df = spark_instance.sql("SELECT * FROM bucket_join_df")

    if subsection == 'a':
        # Sort partitions for average_most_kills_per_game
        bucket_join_df.repartition(16, col('player_gamertag'))\
            .sortWithinPartitions('player_gamertag')\
            .createOrReplaceTempView("SP_average_most_kills_per_game")

        # Execute the optimized query
        df1 = spark_instance.sql(sp_average_most_kills_per_game_query)
        return df1

    elif subsection == 'b':
        # Sort partitions for most_played_playlist_query
        bucket_join_df.repartition(16, col('playlist_id'))\
            .sortWithinPartitions('playlist_id')\
            .createOrReplaceTempView("SP_most_played_playlist_query")

        # Execute the optimized query
        df2 = spark_instance.sql(sp_most_played_playlist_query)
        return df2

    elif subsection == 'c':
        # Sort partitions for most_played_map_query
        bucket_join_df.repartition(16, col('map_variant_id'))\
            .sortWithinPartitions('map_variant_id')\
            .createOrReplaceTempView("SP_most_played_map_query")

        # Execute the optimized query
        df3 = spark_instance.sql(sp_most_played_map_query)
        return df3

    else:
        raise ValueError("Invalid subsection. Choose from 'a', 'b', or 'c'.")


def main():
    """
    Main function to execute partitioning, sorting, and query optimizations.
    """
    spark = SparkSession.builder\
        .master("local")\
        .appName("bucket_join_and_aggregations")\
        .getOrCreate()

    # Load bucketed data
    bucket_join(spark)

    # Process each subsection
    result_a = sort_partitions(spark, 'a')
    result_b = sort_partitions(spark, 'b')
    result_c = sort_partitions(spark, 'c')

    # Display results
    print("Average Most Kills Per Game:")
    result_a.show(truncate=False)

    print("Most Played Playlist:")
    result_b.show(truncate=False)

    print("Most Played Map:")
    result_c.show(truncate=False)


if __name__ == "__main__":
    main()
