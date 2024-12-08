from pyspark.sql import SparkSession

def do_actors_history_scd_transformation(spark, actors_df):
    # Register the actors DataFrame as a temp view
    actors_df.createOrReplaceTempView("actors")

    # Spark SQL query adapted for Spark syntax
    query = """
    WITH streak_started AS (
        SELECT
            actorid,
            actor,
            current_year,
            quality_class,
            is_active,
            CASE
                WHEN NOT(LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) <=> quality_class)
                     OR NOT(LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) <=> is_active)
                     OR LAG(actorid) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
                THEN 1
                ELSE 0
            END AS did_change
        FROM actors
    ),
    streak_identified AS (
        SELECT
            actorid,
            actor,
            quality_class,
            is_active,
            current_year,
            SUM(did_change) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
        FROM streak_started
    ),
    aggregated AS (
        SELECT
            actorid,
            actor,
            quality_class,
            is_active,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date,
            MAX(current_year) AS current_year
        FROM streak_identified
        GROUP BY actorid, actor, quality_class, is_active
    )
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date,
        current_year
    FROM aggregated
    ORDER BY actor, start_date
    """

    # Return the DataFrame without inserting into a table
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_history_scd_job") \
        .getOrCreate()

    # Here you'd typically load real data. For the sake of example:
    input_data = [
        (1, "Actor A", "High", True, 2020),
        (1, "Actor A", "High", True, 2021),
        (1, "Actor A", "Medium", True, 2022),
        (2, "Actor B", "Low", True, 2020),
        (2, "Actor B", "Low", False, 2021),
    ]
    columns = ["actorid", "actor", "quality_class", "is_active", "current_year"]
    actors_df = spark.createDataFrame(input_data, columns)

    output_df = do_actors_history_scd_transformation(spark, actors_df)
    output_df.show()


if __name__ == "__main__":
    main()
