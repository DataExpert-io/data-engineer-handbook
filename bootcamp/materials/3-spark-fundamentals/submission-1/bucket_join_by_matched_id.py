from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# SQL queries
query = '''select
        a.player_gamertag as player_gamertag_medal,
        a.medal_id,
        a.count,
        b.*,
        c.is_team_game,
        c.playlist_id,
        c.game_variant_id,
        c.is_match_over,
        c.completion_date,
        c.match_duration,
        c.game_mode,
        c.map_variant_id
    from
    bucketed.medals_matches_players a
    left join bucketed.match_details b
    on a.match_id = b.match_id
    left join bucketed.matches c
    on b.match_id = c.match_id'''

average_most_kills_per_game_query = '''
    WITH cte_player_stats AS (SELECT
        player_gamertag,
        count(match_id) as num_of_matches,
        sum(player_total_kills) as total_kills
    FROM bucket_join_df
    GROUP BY player_gamertag)
    SELECT *, total_kills / num_of_matches as kills_per_match FROM cte_player_stats
    ORDER BY kills_per_match DESC LIMIT 1'''

most_played_playlist_query = '''
    WITH cte_count_duplicates AS (SELECT
    playlist_id,
    match_id,
    ROW_NUMBER() OVER (PARTITION BY match_id, playlist_id ORDER BY playlist_id) AS COUNTER
    FROM bucket_join_df)
    SELECT playlist_id as reproductions FROM cte_count_duplicates
    WHERE COUNTER = 1
    GROUP BY playlist_id
    ORDER BY count(match_id) desc
    LIMIT 1'''

most_played_map_query = '''
    WITH cte_count_duplicates AS (SELECT
    map_variant_id,
    match_id,
    ROW_NUMBER() OVER (PARTITION BY match_id, map_variant_id ORDER BY map_variant_id) AS COUNTER
    FROM bucket_join_df)
    SELECT map_variant_id as most_played FROM cte_count_duplicates
    WHERE COUNTER = 1
    GROUP BY map_variant_id
    ORDER BY COUNT(map_variant_id) DESC
    LIMIT 1'''

def bucket_join(spark_instance: SparkSession) -> None:
    """
    Prepares bucketed tables and executes a join using SQL.

    Args:
        spark_instance (SparkSession): Active Spark session.
    """
    # Load and sort data before bucketing
    match_details = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv").sort("match_id")
    matches = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv").sort("match_id")
    medals_matches_players = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv").sort("match_id")

    # Write bucketed tables
    match_details.write.bucketBy(16, "match_id").saveAsTable("bucketed.match_details")
    matches.write.bucketBy(16, "match_id").saveAsTable("bucketed.matches")
    medals_matches_players.write.bucketBy(16, "match_id").saveAsTable("bucketed.medals_matches_players")

    # Execute join
    bucket_join_df = spark_instance.sql(query)
    bucket_join_df.createOrReplaceTempView("bucket_join_df")

def kills_per_game(spark_instance: SparkSession) -> str:
    average_most_kills_per_game = spark_instance.sql(average_most_kills_per_game_query)
    return average_most_kills_per_game.collect()[0]['player_gamertag']

def played_playlist(spark_instance: SparkSession) -> str:
    most_played_playlist = spark_instance.sql(most_played_playlist_query)
    return most_played_playlist.collect()[0]['playlist_id']

def the_most_played_map(spark_instance: SparkSession) -> str:
    most_played_map = spark_instance.sql(most_played_map_query)
    return most_played_map.collect()[0]['map_variant_id']

def map_with_more_killing_spree_medals(spark_instance: SparkSession) -> str:
    medals = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
    medal_id_killing_spree = medals.filter(col("name") == 'Killing Spree').select('medal_id').collect()[0]['medal_id']

    most_killing_spree_medals_map_query = f'''
            WITH cte_medals_per_map AS (SELECT
           map_variant_id,
           medal_id,
           sum(count) as cumulative_sum
           FROM bucket_join_df
           WHERE map_variant_id IS NOT NULL
           GROUP BY map_variant_id, medal_id)
           SELECT map_variant_id FROM cte_medals_per_map
           WHERE medal_id = {medal_id_killing_spree}
           ORDER BY cumulative_sum DESC LIMIT 1'''

    most_killing_spree_medals_map = spark_instance.sql(most_killing_spree_medals_map_query)
    return most_killing_spree_medals_map.collect()[0]['map_variant_id']

def run() -> tuple[str, str, str, str]:
    spark = SparkSession.builder\
        .master("local")\
        .appName("bucket_join_and_aggregations")\
        .getOrCreate()

    bucket_join(spark)

    result1 = kills_per_game(spark)
    result2 = played_playlist(spark)
    result3 = the_most_played_map(spark)
    result4 = map_with_more_killing_spree_medals(spark)

    return (result1, result2, result3, result4)

if __name__ == "__main__":
    results = run()
    print(f"Results: {results}")
