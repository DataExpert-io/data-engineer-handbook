from pyspark.sql import SparkSession


def main():
    spark=SparkSession.builder\
        .master("local") \
        .appName("turn_off_automatic_broadcast_join") \
        .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

if __name__ == "__main__":
    main()