# - match_details
#   - a row for every players performance in a match

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder.appName("Assignment 3").getOrCreate()

# Task 1 - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load assets from csv
df_medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
df_medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
df_matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
df_maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# Task 2 - Explicitly broadcast JOINs `medals` and `maps`
df_medals_join_match_id = df_medals_matches_players.join(df_medals, on="medal_id", how="inner")
df_match_join_map_id = df_medals_join_match_id.join(df_matches, on="match_id", how="inner")
df_medals_maps = df_match_join_map_id.join(broadcast(df_maps), on="mapid", how="inner")
df_medals_maps.show()

# Task 3 - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
spark.sql("""CREATE DATABASE IF NOT EXISTS bootcamp""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")

# Load match details from csv
df_match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
# DDL for match details
match_details_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    total_xp INT,
    player_rank_on_team INT,
    player_finished BOOLEAN,
    player_total_kills INT,
    player_total_deaths INT,
    player_total_assists INT,
    did_win INT,
    team_id INT
) USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(match_details_ddl)
df_match_details.select(
    "match_id",
    "player_gamertag",
    "total_xp",
    "player_rank_on_team",
    "player_finished",
    "player_total_kills",
    "player_total_deaths",
    "player_total_assists",
    "did_win",
    "team_id"
).write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")

# DDL for matches
matches_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    game_mode STRING,
    map_variant_id STRING
) USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(matches_ddl)
df_matches.select(
    "match_id",
    "mapid",
    "is_team_game",
    "playlist_id",
    "game_variant_id",
    "game_mode",
    "map_variant_id"
).write.mode("overwrite").bucketBy(16, "match_id").saveAsTable(
    "bootcamp.matches_bucketed"
)

# DDL for medals, matches and players
medal_matches_players_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    count INT
) USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(medal_matches_players_ddl)
df_medals_matches_players.select(
    "match_id",
    "player_gamertag",
    "medal_id",
    "count"
).write.mode("overwrite").bucketBy(16, "match_id").saveAsTable(
    "bootcamp.medal_matches_players_bucketed"
)

bucket_result = spark.sql("""
    SELECT
        mdb.match_id AS match_id,
        mdb.player_gamertag AS player_gamertag,
        mdb.total_xp AS total_xp,
        mdb.player_rank_on_team AS player_rank_on_team,
        mdb.player_finished AS player_finished,
        mdb.player_total_kills AS player_total_kills,
        mdb.player_total_deaths AS player_total_deaths,
        mdb.player_total_assists AS player_total_assists,
        mdb.did_win AS did_win,
        mdb.team_id AS team_id,
        md.mapid AS mapid,
        md.is_team_game AS is_team_game,
        md.playlist_id AS playlist_id,
        md.game_variant_id AS game_variant_id,
        md.game_mode AS game_mode,
        md.map_variant_id AS map_variant_id,
        mmp.medal_id AS medal_id,
        mmp.count AS count
    FROM bootcamp.match_details_bucketed mdb 
    JOIN bootcamp.matches_bucketed md ON mdb.match_id = md.match_id
    JOIN bootcamp.medal_matches_players_bucketed mmp ON mdb.match_id = mmp.match_id
""")

# Task 4 - Aggregate the joined data frame to figure out questions like:
# - Which player averages the most kills per game?
bucket_result.createOrReplaceTempView("bucket_result")
spark.sql("""
    SELECT
        player_gamertag,
        AVG(player_total_kills) AS avg_kills_per_game
    FROM bucket_result
    GROUP BY player_gamertag
    ORDER BY avg_kills_per_game DESC
""").show()

# - Which playlist gets played the most?
spark.sql("""
    SELECT
        playlist_id,
        COUNT(1) AS count
    FROM bucket_result
    GROUP BY playlist_id
    ORDER BY count DESC
""").show()

# - Which map gets played the most?
spark.sql("""
    SELECT
        mapid,
        COUNT(1) AS plays_count
    FROM bucket_result
    GROUP BY mapid
    ORDER BY plays_count DESC
""").show()

# - Which map do players get the most Killing Spree medals on?
spark.sql("""
    SELECT
        mapid,
        SUM(count) AS spree_count
    FROM bucket_result
    WHERE medal_id = 2430242797
    GROUP BY mapid
    ORDER BY spree_count DESC
""").show()

# Task 5 - With the aggregated data set; try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
sorted_bucket_result = bucket_result.repartition(10, col("match_id")).sortWithinPartitions(col("playlist_id"), col("mapid"), col("game_variant_id"), col("team_id"))
sorted_bucket_result.write.mode("overwrite").saveAsTable("bootcamp.sorted_bucket_result")
spark.sql("""
    SELECT
        SUM(file_size_in_bytes) AS size,
        COUNT(1) AS num_files
    FROM bootcamp.sorted_bucket_result.files
""").show()

# combination results
# 15561528 - col("playlist_id"), col("mapid"), col("game_variant_id"), col("team_id")
# 15636964 - col("playlist_id"), col("mapid"), col("team_id"), col("game_variant_id")
# 15688851 - col("playlist_id"), col("mapid")
# 15817563 - col("mapid"), col("playlist_id"), col("game_variant_id")
# 15876151 - col("mapid"), col("game_variant_id"), col("playlist_id")
# 15881785 - col("game_variant_id"), col("playlist_id"), col("mapid")
# 16042944 - col("team_id"), col("game_variant_id"), col("playlist_id"), col("mapid")

# cardinality count
# spark.sql("""
#     SELECT
#         COUNT(DISTINCT team_id) AS team_id,
#         COUNT(DISTINCT mapid) AS mapid,
#         COUNT(DISTINCT playlist_id) AS playlist_id,
#         COUNT(DISTINCT game_variant_id) AS game_variant_id,
#         COUNT(DISTINCT map_variant_id) AS map_variant_id,
#         COUNT(DISTINCT medal_id) AS medal_id
#     FROM bucket_result
# """).show()

spark.stop()