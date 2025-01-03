# Changing the notebook to a python script since it is asked in the homework
# However, I put everything in a single file since there would be so many duplicates if I split it in multiple files
# I could have splitted it in a real project but I feel like it is not the goal of the homework

# # First part of Zack Wilson's bootcamp [spark homework](https://github.com/DataExpert-io/data-engineer-handbook/blob/main/bootcamp/materials/3-spark-fundamentals/homework/homework.md).

# [Pyspark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.alias.html)

# ### Make imports and create spark session if necessary

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Jupyter").getOrCreate()

### Disable autoBroadcast joins. We will explicitely do it
# spark.sparkContext.getConf().getAll()
print(f"Originally, autoBroadcastJoinThreshold is {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(f"Now is {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

### Add a few helpers

def create_df_from_csv(csv_name):
    '''
        All csvs are in /home/iceberg/data/
        Needs spark
    '''
    return spark \
        .read \
        .option("header","true") \
        .option("inferSchema", "true") \
        .csv(f"/home/iceberg/data/{csv_name}.csv")

def drop_table(table_name, schema_name="bootcamp"):
    spark.sql(f"drop table if exists {schema_name}.{table_name}")

def print_table_size(table_name, schema_name="bootcamp"):
    file_sizes_df = spark.sql(f"""
        select 
            file_path, 
            file_size_in_bytes 
        from 
            {schema_name}.{table_name}.files
    """)

    # file_sizes_df.show(truncate=False)
    
    total_size = file_sizes_df.agg({"file_size_in_bytes": "sum"}).collect()[0][0]
    print(f"Total table size: {total_size/1000000} Mb")

### Read the necessary datasets from csvs

medals = create_df_from_csv("medals")

maps = create_df_from_csv("maps")

match_details = create_df_from_csv("match_details")

matches = create_df_from_csv("matches")

medals_matches_players = create_df_from_csv("medals_matches_players")

for df in [medals, maps, match_details, matches, medals_matches_players]:
    print(f"{df.count()}")

# ### Handle joins directly
# * medals and maps are considerably smaller than the others => broadcast them when join
# * create iceberg tables upon them in order to leverage bucket joins 

# ### Lets create tables and populate with their respective csvs

spark.sql("show tables in bootcamp").show()


drop_table("match_details_bucketed")

match_details_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""

spark.sql(match_details_bucketed_ddl)

match_details \
    .select(
        "match_id", "player_gamertag", "player_total_kills", "player_total_deaths"
    ) \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.match_details_bucketed")


drop_table("matches_bucketed")

matches_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
     match_id STRING,
     mapid STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """

spark.sql(matches_bucketed_ddl)

matches \
    .select(
        "match_id", "mapid", "is_team_game", "playlist_id", "completion_date"
    ) \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.matches_bucketed")


drop_table("medals_matches_players_bucketed")

medals_matches_players_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    medal_id STRING,
    player_gamertag STRING,
    count INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""

spark.sql(medals_matches_players_bucketed_ddl)

medals_matches_players \
    .select(
        "match_id", "medal_id", "player_gamertag", "count"
    ) \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")

### Reselect them as tables
match_details_table = spark.sql("select * from bootcamp.match_details_bucketed")
matches_table = spark.sql("select * from bootcamp.matches_bucketed")
medals_matches_players_table = spark.sql("select * from bootcamp.medals_matches_players_bucketed")

spark.sql("select * from bootcamp.medals_matches_players_bucketed").count()

# ### Answering questions from the homework
# 1. Which player averages the most kills per game?
# 2. Which playlist gets played the most?
# 3. Which map gets played the most?
# 4. Which map do players get the most Killing Spree medals on?

# In the assignement, we are asked first to join all the dataframes and then answer questions but I feel like this would not be the more efficient. I do the join in order to comply but I'll answer questions differently.

# WARNING : in the _medals_ dataset, some _medal\_id_ does not have a _name_ and their are duplicates in _name_ => the joined dataframe will have a row per _match\_id_, _player\_gamertag_, _medal\_id_ (we have to keep the last one to ensure uniqueness).


joined_df = match_details_table \
    .join(
        matches_table
        , matches_table.match_id == match_details_table.match_id
        , how="inner"
    ) \
    .join(
        medals_matches_players_table
        , [medals_matches_players_table.match_id == match_details_table.match_id
            , medals_matches_players_table.player_gamertag == match_details_table.player_gamertag]
        , how="inner"
    ) \
    .join(
        broadcast(maps)
        , maps.mapid == matches_table.mapid
        , how="inner"
    ) \
    .join(
        broadcast(medals)
        , medals.medal_id == medals_matches_players_table.medal_id
        , how="inner"
    ) \
    .select(
        match_details_table.match_id
        , match_details_table.player_gamertag
        , medals_matches_players_table.medal_id
        , maps.name.alias("map_name")
        , matches_table.playlist_id
        , match_details_table.player_total_kills
        , match_details_table.player_total_deaths
        , medals.name.alias("medal_name")
        , F.col("count").alias("medal_count")
    )


# Top 5 avg kills player, limit should be deterministic
match_details_table \
    .groupBy("player_gamertag") \
    .agg(F.avg("player_total_kills").alias("avg_kills")) \
    .sort(F.desc("avg_kills")) \
    .limit(5) \
    .show()
    
# Top 5 most played playlists. Did not found were to look for playlist_name, limit should be deterministic
matches_table \
    .groupBy("playlist_id") \
    .agg(F.count("match_id").alias("nb_played")) \
    .sort(F.desc("nb_played")) \
    .limit(5) \
    .show()

# Top 5 most played maps, limit should be deterministic
matches_table \
    .join(broadcast(maps)
          , matches_table.mapid == maps.mapid
    ) \
    .select(
        matches_table.mapid
        , maps.name.alias("map_name")
    ) \
    .groupBy("map_name") \
    .agg(F.count("mapid").alias("nb_played")) \
    .sort(F.desc("nb_played")) \
    .limit(5) \
    .show()

# Top 5 maps having the most Killing Spree medal, limit should be deterministic
joined_df \
    .where("medal_name='Killing Spree'") \
    .groupBy("map_name") \
    .agg(F.sum("medal_count").alias("sum_medals")) \
    .sort(F.desc("sum_medals")) \
    .limit(5) \
    .show()


# ### Find best keys to sort partitions in order to find the smallest total file size

# In the homework, it is written to sort the aggregated dataset but I do it on the joined one because I feel like there was no single aggregated dataset

def print_file_size_from_sort_keys(df, sorting_keys=[], base_table_name="joined", schema_name="bootcamp"):
    table_name = base_table_name
    for key in sorting_keys: table_name += f"_{key}"
    print(f"table name is : {table_name}")
    df \
        .sortWithinPartitions(sorting_keys) \
        .write.mode("overwrite") \
        .saveAsTable(f"{schema_name}.{table_name}")
    print_table_size(table_name)

print_file_size_from_sort_keys(joined_df)
# Output : Total table size: 6.801128 Mb

print_file_size_from_sort_keys(joined_df, ["match_id"])
# Output : Total table size: 6.801128 Mb

print_file_size_from_sort_keys(joined_df, ["match_id", "player_gamertag"])
# Output : Total table size: 6.801128 Mb

print_file_size_from_sort_keys(joined_df, ["match_id", "player_gamertag", "map_name"])
# Output : Total table size: 6.801128 Mb

print_file_size_from_sort_keys(joined_df, ["match_id", "player_gamertag", "map_name", "playlist_id"])
# Output : Total table size: 6.801128 Mb

print_file_size_from_sort_keys(joined_df, ["map_name", "playlist_id"])
# Output : Total table size: 6.433709 Mb

print_file_size_from_sort_keys(joined_df, ["playlist_id", "map_name"])
# Output : Total table size: 6.405169 Mb

print_file_size_from_sort_keys(joined_df, ["playlist_id"])
# Output : Total table size: 6.518134 Mb

print_file_size_from_sort_keys(joined_df, ["map_name"])
# Output : Total table size: 6.558627 Mb

# Hence the best keys to sort partitions are ["playlist_id", "map_name"] with a total size of 6.405169 Mb