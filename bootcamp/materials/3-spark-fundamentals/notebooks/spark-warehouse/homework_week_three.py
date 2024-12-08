from pyspark.sql.functions import col, avg, count, desc, broadcast
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Jupyter").getOrCreate()

    # Disable automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Load datasets (assuming they are in CSV format)
    match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
    matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
    medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
    medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
    maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

    # Convert appropriate columns to integers where necessary
    match_details = match_details.withColumn("match_id", col("match_id").cast("int")) \
                                .withColumn("player_total_kills", col("player_total_kills").cast("int"))

    matches = matches.withColumn("match_id", col("match_id").cast("int"))

    medals_matches_players = medals_matches_players.withColumn("match_id", col("match_id").cast("int")) \
                                                .withColumn("medal_id", col("medal_id").cast("int")) \
                                                .withColumn("count", col("count").cast("int"))

    medals = medals.withColumn("medal_id", col("medal_id").cast("int"))

    # Rename the 'name' column in the 'medals' DataFrame to 'medal_name' to avoid ambiguity
    medals_renamed = medals.withColumnRenamed("name", "medal_name")

    # Q1 - Explicitly broadcast the 'medals' and 'maps' DataFrames
    medals_broadcasted = broadcast(medals_renamed)
    maps_broadcasted = broadcast(maps)


   # Q2 - Save DataFrames with bucketBy (bucketing on match_id with 16 buckets)
    match_details.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_match_details")
    matches.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_matches")
    medals_matches_players.write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_medals_matches_players")

    # Read the bucketed DataFrames
    match_details = spark.table("bucketed_match_details")
    matches = spark.table("bucketed_matches")
    medals_matches_players = spark.table("bucketed_medals_matches_players")

    # Q3 - Perform bucket join with broadcasted DataFrames
    joined_df = match_details.join(matches, "match_id") \
                            .join(medals_matches_players, ["match_id", "player_gamertag"]) \
                            .join(medals_broadcasted, "medal_id") \
                            .join(maps_broadcasted, "mapid", "left")

    # Aggregation: Which player averages the most kills per game?
    kills_per_game = joined_df.groupBy("player_gamertag") \
                            .agg(avg("player_total_kills").alias("avg_kills")) \
                            .orderBy(desc("avg_kills"))


    # Aggregation: Which playlist gets played the most?
    playlist_count = joined_df.groupBy("playlist_id") \
                            .agg(count("match_id").alias("match_count")) \
                            .orderBy(desc("match_count"))

    # Aggregation: Which map gets played the most?
    map_count = joined_df.groupBy("mapid") \
                        .agg(count("match_id").alias("match_count")) \
                        .orderBy(desc("match_count"))

    # Aggregation: Which map do players get the most Killing Spree medals on?
    killing_spree_maps = joined_df.filter(col("medal_name") == "Killing Spree") \
                                .groupBy("mapid") \
                                .agg(count("medal_id").alias("killing_spree_count")) \
                                .orderBy(desc("killing_spree_count"))

     # Function to measure data size after sorting within partitions
    def measure_data_size(df, sort_column, description):
        print(f"\n--- {description} ---")
        sorted_df = df.sortWithinPartitions(sort_column)
        partition_sizes = sorted_df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        total_size = sum(partition_sizes)
        print(f"Total Data Size: {total_size} rows")
        print(f"Partition Sizes: {partition_sizes}")

    # Experiment with different sortWithinPartitions configurations for all aggregations

    # Kills Per Game
    measure_data_size(kills_per_game, "player_gamertag", "Kills Per Game Sorted by 'player_gamertag'")
    measure_data_size(kills_per_game, "avg_kills", "Kills Per Game Sorted by 'avg_kills'")

    # Playlist Count
    measure_data_size(playlist_count, "playlist_id", "Playlist Count Sorted by 'playlist_id'")
    measure_data_size(playlist_count, "match_count", "Playlist Count Sorted by 'match_count'")

    # Map Count
    measure_data_size(map_count, "mapid", "Map Count Sorted by 'mapid'")
    measure_data_size(map_count, "match_count", "Map Count Sorted by 'match_count'")

    # Killing Spree Maps
    measure_data_size(killing_spree_maps, "mapid", "Killing Spree Maps Sorted by 'mapid'")
    measure_data_size(killing_spree_maps, "killing_spree_count", "Killing Spree Maps Sorted by 'killing_spree_count'")


if __name__ == "__main__":
    main()