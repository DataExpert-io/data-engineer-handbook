from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as spark_sum, lit, min as spark_min, max as spark_max
from pyspark.sql.window import Window


def do_actors_scd_transformation(spark, dataframe):
    """
    Performs Slowly Changing Dimensions (SCD) transformation for actors.

    Args:
        spark (SparkSession): The active Spark session.
        dataframe: The input DataFrame containing the `actors` table.

    Returns:
        DataFrame: Transformed DataFrame for SCD logic.
    """
    # Define a window specification for partitioning by actorid and ordering by current_year
    window_spec = Window.partitionBy("actorid").orderBy("current_year")

    # Add columns to identify changes
    has_changed_df = dataframe.withColumn(
        "did_change_quality",
        (lag("quality_class", 1).over(window_spec) != col("quality_class")) |
        (lag("quality_class", 1).over(window_spec).isNull())
    ).withColumn(
        "did_change_active",
        (lag("is_active", 1).over(window_spec) != col("is_active")) |
        (lag("is_active", 1).over(window_spec).isNull())
    )

    # Identify changes and group by change identifiers
    change_identified_df = has_changed_df.withColumn(
        "change_identified",
        spark_sum(
            (col("did_change_quality") | col("did_change_active")).cast("int")
        ).over(window_spec)
    )

    # Group data by change identifier and calculate start and end years
    grouped_df = change_identified_df.groupBy(
        "actorid", "change_identified", "quality_class", "is_active"
    ).agg(
        spark_min("current_year").alias("start_year"),
        spark_max("current_year").alias("end_year")
    ).withColumn(
        "current_year", lit(2024)  # Add the current year as a fixed column
    )

    # Select final columns for the output
    result_df = grouped_df.select(
        "actorid", "quality_class", "start_year", "end_year", "is_active", "current_year"
    )

    return result_df


def main():
    """
    Main function to execute the SCD transformation for actors.
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()

    # Retrieve the input DataFrame
    input_df = spark.table("actors")

    # Perform the transformation
    output_df = do_actors_scd_transformation(spark, input_df)

    # Write the result back to the table
    output_df.write.mode("overwrite").saveAsTable("actors_history_scd")


if __name__ == "__main__":
    main()
