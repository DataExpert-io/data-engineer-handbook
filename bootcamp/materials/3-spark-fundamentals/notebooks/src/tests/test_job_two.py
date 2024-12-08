import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from ..jobs.job_two import do_actors_current_year_transformation
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType


def test_actors_current_year_transformation(spark):
    # Define the schema for 'films' as an array of structs
    film_struct = StructType([
    StructField("film", StringType(), True),
    StructField("votes", LongType(), True),    # changed to LongType
    StructField("rating", DoubleType(), True),
    StructField("filmid", LongType(), True)     # changed to LongType
    ])
   
    actors_schema = StructType([
        StructField("actorid", LongType(), True),
        StructField("actor", StringType(), True),
        StructField("current_year", LongType(), True),
        StructField("films", ArrayType(film_struct), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])

    # Setup test input data
    # Note: Even though the arrays are empty, Spark now knows they should be arrays of the defined struct.
    actors_data = [
        (1, "Actor A", 1971, [], "good", True),
        (2, "Actor B", 1971, [], "bad", False)
    ]
    actors_df = spark.createDataFrame(actors_data, actors_schema)

    # For actor_films, we have actual data with films, so Spark can infer the schema safely.
    actor_films_data = [
        (1, "Actor A", "Film A1", 100, 8.5, 101, 1972),
        (1, "Actor A", "Film A2", 50, 7.0, 102, 1972),
        (2, "Actor B", "Film B1", 200, 9.0, 201, 1972)
    ]
    actor_films_cols = ["actorid", "actor", "film", "votes", "rating", "filmid", "year"]
    actor_films_df = spark.createDataFrame(actor_films_data, actor_films_cols)

    # Run the transformation
    actual_df = do_actors_current_year_transformation(spark, actors_df, actor_films_df)

    # Expected results
    expected_data = [
        (1, "Actor A", 1972,
         [("Film A1", 100, 8.5, 101), ("Film A2", 50, 7.0, 102)],
         "good",
         True),
        (2, "Actor B", 1972,
         [("Film B1", 200, 9.0, 201)],
         "star",
         True)
    ]

    expected_schema = StructType([
    StructField("actorid", LongType(), True),
    StructField("actor", StringType(), True),
    StructField("current_year", LongType(), True),
    # Set nullable to False to match actual schema
    StructField("films", ArrayType(film_struct), False), 
    StructField("quality_class", StringType(), True),
    # Set nullable to False to match actual schema
    StructField("is_active", BooleanType(), False)        
])

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(actual_df, expected_df)
