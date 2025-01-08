from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def make_explicit_broadcast_join(spark_instance: SparkSession):
    """
    Perform an explicit broadcast join between medals and maps datasets.

    Args:
        spark_instance (SparkSession): The active SparkSession.

    Returns:
        DataFrame: Result of the broadcast join.
    """
    # Load datasets
    medals = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
    maps = spark_instance.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")

    # Add a mock join key for demonstration if none exists
    # Assuming 'name' is not a valid join key, create mock keys
    medals = medals.withColumnRenamed("name", "medal_name")
    maps = maps.withColumnRenamed("name", "map_name")

    # Create a mock join key column for demonstration purposes
    medals = medals.withColumn("mock_key", medals["medal_name"].substr(1, 2))  # Example: Use first 2 characters
    maps = maps.withColumn("mock_key", maps["map_name"].substr(1, 2))

    # Explicit broadcast join
    result = medals.join(broadcast(maps), on="mock_key", how="left")

    return result

def main():
    """
    Main function to initiate the SparkSession and perform the explicit broadcast join.
    """
    spark = SparkSession.builder\
        .master("local") \
        .appName("explicit_broadcast_join") \
        .getOrCreate()

    result = make_explicit_broadcast_join(spark)
    result.show(truncate=False)  # Display the result

if __name__ == "__main__":
    main()
