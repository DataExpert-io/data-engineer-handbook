from pyspark.sql.functions import broadcast, split, lit
from pyspark.sql.functions import col
from pyspark import StorageLevel
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("IcebergTableManagement") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .getOrCreate()
df_medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
# maps data frame reading
df_maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
df_1 = df_medals.join(broadcast(df_maps), on="name", how="outer")
df_1.show(5)
match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
medals_matches_players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw3_matches""")
matches_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.hw3_matches(
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
"""
spark.sql(matches_ddl)
matches_df.select("*") \
    .write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.hw3_matches")
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw3_medals_matches_players""")
medals_matches_players_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.hw3_medals_matches_players(
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    count INTEGER
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
"""
spark.sql(medals_matches_players_ddl)
medals_matches_players_df.select("*") \
    .write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.hw3_medals_matches_players")
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw3_match_details""")
match_details_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.hw3_match_details(
    match_id STRING,
    player_gamertag STRING,
    spartan_rank INTEGER,
    player_total_kills INTEGER
)
USING iceberg
CLUSTERED BY (match_id) INTO 16 BUCKETS
"""
spark.sql(match_details_ddl)
match_details_df.select("match_id", "player_gamertag", "spartan_rank", "player_total_kills") \
    .write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.hw3_match_details")
def get_columns_without_match_id(table_name, alias):
    all_columns = spark.table(table_name).columns
    return [f"{alias}.{col}" for col in all_columns if col != "match_id"]
matches_columns = get_columns_without_match_id("bootcamp.hw3_matches", "m")
match_details_columns = get_columns_without_match_id("bootcamp.hw3_match_details", "md")
medals_columns = get_columns_without_match_id("bootcamp.hw3_medals_matches_players", "mp")    
joining_query = f"""
SELECT
    COALESCE(m.match_id, md.match_id) AS match_id,
    {', '.join(matches_columns)}, 
    {', '.join(match_details_columns)}, 
    mp.player_gamertag AS medal_player_gamertag,
    mp.medal_id,
    mp.count
FROM bootcamp.hw3_matches AS m
FULL OUTER JOIN bootcamp.hw3_match_details AS md
    ON m.match_id = md.match_id
FULL OUTER JOIN bootcamp.hw3_medals_matches_players AS mp
    ON COALESCE(m.match_id, md.match_id) = mp.match_id
"""
joined_table_df = spark.sql(joining_query)
joined_table_df.createOrReplaceTempView("joined_table")
q1 = """ 
WITH deduplicated_table AS (
    SELECT DISTINCT 
        match_id, 
        player_gamertag, 
        player_total_kills
    FROM 
        joined_table
),
player_match_kills AS (
    SELECT 
        player_gamertag,
        match_id,
        SUM(player_total_kills) AS total_kills_per_match
    FROM 
        deduplicated_table
    GROUP BY 
        player_gamertag, match_id
),
player_avg_kills AS (
    SELECT 
        player_gamertag,
        AVG(total_kills_per_match) AS avg_kills_per_match
    FROM 
        player_match_kills
    GROUP BY 
        player_gamertag
)
SELECT 
    player_gamertag,
    avg_kills_per_match
FROM 
    player_avg_kills
ORDER BY 
    avg_kills_per_match DESC
LIMIT 1;
"""
spark.sql(q1).show()
q2 = """
WITH deduplicated_table AS (
    SELECT DISTINCT 
        match_id, 
        playlist_id
    FROM 
        joined_table
)
SELECT 
    playlist_id,
    COUNT(*) AS times_played
FROM 
    deduplicated_table
GROUP BY 
    playlist_id
ORDER BY 
    times_played DESC
LIMIT 1;
"""
spark.sql(q2).show(truncate=False)
q3 = """
WITH deduplicated_table AS (
    SELECT DISTINCT 
        match_id, 
        mapid
    FROM 
        joined_table
)
SELECT 
    mapid,
    COUNT(*) AS times_played
FROM 
    deduplicated_table
GROUP BY 
    mapid
ORDER BY 
    times_played DESC
LIMIT 1;
"""
spark.sql(q3).show(truncate=False)
df_medals.createOrReplaceTempView("medals")
q4="""
    WITH deduplicated_table AS (
        SELECT DISTINCT 
            jt.match_id, 
            jt.mapid,
            jt.medal_player_gamertag,
            jt.medal_id,
            jt.count
        FROM 
            joined_table jt
        JOIN
            medals m
        ON 
            jt.medal_id = m.medal_id
        WHERE 
            m.classification = 'KillingSpree'
    )
        SELECT 
        mapid, 
        SUM(count) AS total_medals
    FROM 
        deduplicated_table
    GROUP BY 
        mapid
    ORDER BY 
        total_medals DESC
    LIMIT 1
"""
spark.sql(q4).show(truncate=False)
spark.sql(
"""CREATE TABLE IF NOT EXISTS bootcamp.hw3_joined_table (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    player_gamertag STRING,
    spartan_rank INTEGER,
    player_total_kills INTEGER,
    medal_player_gamertag STRING,
    medal_id BIGINT,
    count INTEGER
)
USING iceberg
    PARTITIONED BY (match_id);
    """
)
spark.sql(
"""CREATE TABLE IF NOT EXISTS bootcamp.hw3_sorted_1 (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    player_gamertag STRING,
    spartan_rank INTEGER,
    player_total_kills INTEGER,
    medal_player_gamertag STRING,
    medal_id BIGINT,
    count INTEGER
)
USING iceberg
    PARTITIONED BY (match_id);
    """
)
start_df = joined_table_df.repartition(4, col("match_id"))
sorted_df_1 = start_df.sortWithinPartitions(col("match_id"), col("player_gamertag"), col("mapid")) 
sorted_df_2 = start_df.sortWithinPartitions(col("match_id"), col("playlist_id"), col("mapid")) 
sorted_df_3 = start_df.sortWithinPartitions(col("match_id"), col("mapid"), col("medal_id")) 
start_df.write.mode("overwrite").saveAsTable("bootcamp.hw3_joined_table")
sorted_df_1.write.mode("overwrite").saveAsTable("bootcamp.hw3_sorted_1")
sorted_df_2.write.mode("overwrite").saveAsTable("bootcamp.hw3_sorted_2")
sorted_df_3.write.mode("overwrite").saveAsTable("bootcamp.hw3_sorted_3")

%%sql
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted'  
FROM bootcamp.hw3_joined_table.files # size 8696598	

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_1'
FROM bootcamp.hw3_sorted_1.files # size 8706875

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_2'
FROM bootcamp.hw3_sorted_2.files # size 8711281

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_3'
FROM bootcamp.hw3_sorted_3.files # size 16967862	
