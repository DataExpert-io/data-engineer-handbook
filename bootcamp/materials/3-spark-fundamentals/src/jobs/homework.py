import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import shutil,time, os

class Session:
    # creating Session class for creating SparkSession
    def __init__(self,SparkSession):
        self.sparkSession = SparkSession

    def create_session(self):
        # This method will create the spark session that can be used throughout all the other inherited classes
        spark = self.sparkSession.builder \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .appName("MatchDetails") \
            .getOrCreate()
        return spark

class create_DF (Session):
    # inherited Session class and created a subclass called create_DF that is responsible for creating all the Dataframes
    # used in our application
    def maps_df(self,spark):
        mapsDf = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema",True)\
                .load("C:\\Data-boot-camp\\data-engineer-handbook\\bootcamp\\materials\\3-spark-fundamentals\\data\\maps.csv")

        return mapsDf

    def medals_df(self,spark):
        medalsDf = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema",True)\
                .load("C:\\Data-boot-camp\\data-engineer-handbook\\bootcamp\\materials\\3-spark-fundamentals\\data\\medals.csv")

        return medalsDf

    def medals_matches_players_df(self,spark):
        medals_matches_players_Df = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema",True)\
                .load("C:\\Data-boot-camp\\data-engineer-handbook\\bootcamp\\materials\\3-spark-fundamentals\\data\\medals_matches_players.csv")

        return medals_matches_players_Df

    def matches_df(self,spark):
        matchesDf = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema",True)\
                .load("C:\\Data-boot-camp\\data-engineer-handbook\\bootcamp\\materials\\3-spark-fundamentals\\data\\matches.csv")

        return matchesDf

    def matchDetails_df(self,spark):
        matchDetailsDf = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema",True)\
                .load("C:\\Data-boot-camp\\data-engineer-handbook\\bootcamp\\materials\\3-spark-fundamentals\\data\\match_details.csv")

        return matchDetailsDf

    def bucketBy_matches(self,spark,df):
        # Removing the files in the source directory to enable re-running the application
        path = "./spark-warehouse/matches_bucketed"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table
        spark.sql("DROP TABLE IF EXISTS matches_bucketed")
        numBuckets = 16
        col = "match_id"
        df.write.format("parquet").bucketBy(numBuckets,f"{col}").mode("overwrite").saveAsTable("matches_bucketed")


        # Reading the bucketed table
        matches_bucketed_df = spark.table("matches_bucketed")
        print("----------------Success: bucketed matches dataframe---------------")
        return matches_bucketed_df

    def bucketPartitionBy_matches(self,spark,df):
        # Removing the files in the source directory to enable re-running the application
        path = "./spark-warehouse/matches_bucketed_partitioned"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table.
        # Here we also repartitioned to 8 partitions and sorted the partitions to enable Run Length Encoding
        # and effective compression
        spark.sql("DROP TABLE IF EXISTS matches_bucketed_partitioned")
        numBuckets = 16
        col = "match_id"
        partitioned_df = df.repartition(8,"match_id").sortWithinPartitions("match_id","mapid")
        partitioned_df.write.format("parquet").bucketBy(numBuckets,f"{col}").mode("overwrite").saveAsTable("matches_bucketed_partitioned")

        # Reading the bucketed cum Partitioned table
        matches_bucketed_partitioned_df = spark.table("matches_bucketed_partitioned")
        print("----------------Success: bucketed cum Partitioned matches dataframe---------------")
        return matches_bucketed_partitioned_df


    def bucketBy_matchDetails(self,spark,df):

        # Removing the files in the source directory to enable re-running the application
        path = "./spark-warehouse/matchDetails_bucketed"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table
        spark.sql("DROP TABLE IF EXISTS matchDetails_bucketed")
        numBuckets = 16
        col = "match_id"
        df.write.format("parquet").bucketBy(numBuckets,f"{col}").mode("overwrite").saveAsTable("matchDetails_bucketed")

        # Reading the bucketed table
        matchDetails_bucketed = spark.table("matchDetails_bucketed")
        print("----------------Success: bucketed match details dataframe---------------")
        return matchDetails_bucketed

    def bucketPartitionBy_matchDetails(self,spark,df):
        # Removing the files in the source directory to enable re-running the application
        path = "./spark-warehouse/matchDetails_bucketed_partitioned"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table.
        # Here we also repartitioned to 8 partitions and sorted the partitions to enable Run Length Encoding
        # and effective compression
        spark.sql("DROP TABLE IF EXISTS matchDetails_bucketed_partitioned")
        numBuckets = 16
        col = "match_id"
        partitioned_df = df.repartition(8,"match_id").sortWithinPartitions("match_id")
        partitioned_df.write.format("parquet").bucketBy(numBuckets,f"{col}").mode("overwrite").saveAsTable("matchDetails_bucketed_partitioned")

        # Reading the bucketed table
        matchDetails_bucketed_partitioned_df = spark.table("matchDetails_bucketed_partitioned")
        print("----------------Success: bucketed cum Partitioned matches dataframe---------------")
        return matchDetails_bucketed_partitioned_df

    def bucketBy_medalMatchPlayers(self, spark, df):
        # Removing the files in the source directory to enable re-running the application
        path = "./spark-warehouse/medalMatchPlayers_bucketed"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table
        spark.sql("DROP TABLE IF EXISTS medalMatchPlayers_bucketed")
        numBuckets = 16
        col = "match_id"
        df.write.format("parquet").bucketBy(numBuckets, f"{col}").mode("overwrite").saveAsTable("medalMatchPlayers_bucketed")

        # Reading the bucketed table
        medalMatchPlayers_bucketed = spark.table("medalMatchPlayers_bucketed")
        print("----------------Success: bucketed medalMatchPlayers dataframe---------------")
        return medalMatchPlayers_bucketed

    def bucketPartitionBy_medalMatchPlayers(self, spark, df):

        path = "./spark-warehouse/medalMatchPlayers_bucketed_partitioned"
        try:
            shutil.rmtree(path)
        except OSError as e:
            print("Error: %s : %s " % (path, e.strerror))

        # This method creates the bucketed dataframe and writes it to managed table.
        # Here we also repartitioned to 8 partitions and sorted the partitions to enable Run Length Encoding
        # and effective compression
        spark.sql("DROP TABLE IF EXISTS medalMatchPlayers_bucketed_partitioned")
        numBuckets = 16
        col = "match_id"
        partitioned_df = df.repartition(8,"match_id").sortWithinPartitions("match_id","medal_id")
        partitioned_df.write.format("parquet").bucketBy(numBuckets, f"{col}").mode("overwrite").saveAsTable("medalMatchPlayers_bucketed_partitioned")

        # Reading the bucketed table
        medalMatchPlayers_bucketed_partitioned = spark.table("medalMatchPlayers_bucketed_partitioned")
        print("----------------Success: bucketed cum partitioned medalMatchPlayers dataframe---------------")
        return medalMatchPlayers_bucketed_partitioned



    def bucketJoinDFs(self,spark,matches,matchDetails,medals_matches_players):
        joinedDf = matches.join(medals_matches_players,"match_id").join(matchDetails,["match_id","player_gamertag"])
        return joinedDf

    def aggregated_stats(self,df, medals, maps):
        df.show(5)
        start_time = time.time()

        avg_df = df.groupBy("player_gamertag","match_id").agg(f.avg("player_total_kills").alias("avg_total_kills_per_game"))
        avg_df.show()

        # Which player averages the most kills per game?
        player_name = avg_df.orderBy("avg_total_kills_per_game", ascending=False).limit(1).collect()[0]["player_gamertag"]
        print(f"The player with the most average kills per game is {player_name}")

        # Which playlist gets played the most?
        playlist = df.groupBy("playlist_id").count().orderBy("count",ascending = False).limit(1).collect()[0]["playlist_id"]
        print(f"The most played playlist is {playlist}")

        # Which map gets played the most?

        # Broadcast join to maps dataframe since it is considerably small and well within the threshold limits for broadcast join
        mapName = df.groupBy("mapid").count().orderBy("count", ascending=False).limit(1)\
                .join(f.broadcast(maps),"mapid").collect()[0]["name"]
        print(f"The most played map is {mapName}")

        # Which map do players get the most Killing Spree medals on?

        # Broadcast join on both medals and maps dataframes
        medalMapJoinDf = df.join(f.broadcast(medals),"medal_id").join(f.broadcast(maps),"mapid")
        mapHighest = medalMapJoinDf.where("classification = 'KillingSpree'").groupBy(maps["name"])\
                    .agg(f.sum("count").alias("medalCount"))\
                    .orderBy("medalCount",ascending=False)\
                    .limit(1)\
                    .collect()[0]["name"]
        print(f"The map which gets most Killing Spree is {mapHighest}")

        end_time = time.time()
        print(type(end_time))
        return end_time-start_time

    def size_difference(self):
        # Comparing the file sizes for tables before and after partitioning and sorting within the partitions
        tables_before = ["matchdetails_bucketed", "matches_bucketed","medalmatchplayers_bucketed"]
        tables_after = ["matchdetails_bucketed_partitioned", "matches_bucketed_partitioned","medalmatchplayers_bucketed_partitioned"]
        for table1,table2 in zip(tables_before,tables_after):
            file_path_1 = "./spark-warehouse/" + table1
            file_path_2 = "./spark-warehouse/" + table2
            if os.path.exists (file_path_1):
                size_1,cnt_1=0,0
                for file in os.listdir(file_path_1):
                    size_1+=os.path.getsize(file_path_1+"/"+file)
                    cnt_1+=1
            if os.path.exists(file_path_2):
                size_2,cnt_2 = 0,0
                for file in os.listdir(file_path_2):
                    size_2 += os.path.getsize(file_path_2 + "/" + file)
                    cnt_2+=1
            print(f"{table1} size: {size_1} B with {cnt_1} files, {table2} size: {size_2} B with {cnt_2} files")



def main():
    session_obj = Session(SparkSession)
    new_session = session_obj.create_session()
    createDFs = create_DF(session_obj)
    maps = createDFs.maps_df(new_session)
    medals = createDFs.medals_df(new_session)
    medals_matches_players = createDFs.medals_matches_players_df(new_session)
    matches = createDFs.matches_df(new_session)
    matchDetails = createDFs.matchDetails_df(new_session)

    # maps.show(truncate = False)
    # medals_matches_players.show(truncate=False)
    # matches.show(truncate=False)
    # matchDetails.show(truncate=False)
    # medals.show(truncate=False)
    matches_bucketed = createDFs.bucketBy_matches(new_session,matches)
    matchDetails_bucketed = createDFs.bucketBy_matchDetails(new_session, matchDetails)
    medals_matches_players_bucketed = createDFs.bucketBy_medalMatchPlayers(new_session, medals_matches_players)
    joinedDf = createDFs.bucketJoinDFs(new_session, matches_bucketed,matchDetails_bucketed,medals_matches_players_bucketed)

    time_taken = createDFs.aggregated_stats(joinedDf, medals, maps)
    print(f"Time to execute aggregations using Non-partitioned tables: {time_taken} seconds")

    matches_bucketed_partitioned = createDFs.bucketPartitionBy_matches(new_session,matches)
    matchDetails_bucketed_partitioned = createDFs.bucketPartitionBy_matchDetails(new_session, matchDetails)
    medals_matches_players_bucketed_partitioned = createDFs.bucketPartitionBy_medalMatchPlayers(new_session, medals_matches_players)

    joinedDf_2 = createDFs.bucketJoinDFs(new_session, matches_bucketed_partitioned, matchDetails_bucketed_partitioned, medals_matches_players_bucketed_partitioned)

    time_taken2 = createDFs.aggregated_stats(joinedDf_2, medals, maps)
    print(f"Time to execute aggregations using partitioned tables: {time_taken2} seconds")

    print(f"The time saved in computing aggregations by using sortWithinPartitions: {time_taken - time_taken2} seconds")

    createDFs.size_difference()






if __name__ == '__main__':
    main()

            