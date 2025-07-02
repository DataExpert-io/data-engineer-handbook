from chispa.dataframe_comparer import assert_df_equality
from ..jobs.actor_films_job import do_actor_films_transformation
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, BooleanType

# Define namedtuples for input data
Actor = namedtuple("Actor", "actorid actor films quality_class current_year")
Film = namedtuple("Film", "actorid actor film votes rating filmid year")

# Define schema for films array (to match Spark's NAMED_STRUCT)
FilmStruct = StructType([
    StructField("film", StringType(), True),
    StructField("votes", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("filmid", StringType(), True)
])

# Define output namedtuple
Actor = namedtuple("Actor", "actorid actor films quality_class is_active current_year")

def test_actor_films_transformation(spark):
    # Input data for actors (1969 data)
    actors_data = [
        Actor(1, "Actor A", [], "good", 1969),  # Actor active in 1969
        Actor(2, "Actor B", [], "average", 1969),  # Actor active in 1969
    ]

    # Input data for actor_films (1970 data)
    actor_films_data = [
        Film(1, "Actor A", "Movie X", 1000, 8.5, "f1", 1970),  # Actor A in 1970
        Film(1, "Actor A", "Movie Y", 500, 7.5, "f2", 1970),   # Actor A in 1970
        Film(3, "Actor C", "Movie Z", 200, 6.0, "f3", 1970),   # Actor C only in 1970
    ]

    # Create DataFrames
    actors_df = spark.createDataFrame(actors_data)
    actor_films_df = spark.createDataFrame(actor_films_data)

    # Run transformation
    actual_df = do_actor_films_transformation(spark, actors_df, actor_films_df)

    # Expected output
    expected_output = [
        Actor(
            actorid=1,
            actor="Actor A",
            films=[
                {"film": "Movie X", "votes": 1000, "rating": 8.5, "filmid": "f1"},
                {"film": "Movie Y", "votes": 500, "rating": 7.5, "filmid": "f2"}
            ],
            quality_class="star",  # Avg rating (8.5 + 7.5)/2 = 8.0 -> 'star'
            is_active=True,
            current_year=1970
        ),
        Actor(
            actorid=2,
            actor="Actor B",
            films=[],
            quality_class="average",  # No 1970 films, retains 1969 quality_class
            is_active=False,
            current_year=1970
        ),
        Actor(
            actorid=3,
            actor="Actor C",
            films=[{"film": "Movie Z", "votes": 200, "rating": 6.0, "filmid": "f3"}],
            quality_class="bad",  # Avg rating 6.0 -> 'bad'
            is_active=True,
            current_year=1970
        ),
    ]

    # Create expected DataFrame with proper schema
    schema = StructType([
        StructField("actorid", IntegerType(), True),
        StructField("actor", StringType(), True),
        StructField("films", ArrayType(FilmStruct), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), True)
    ])
    expected_df = spark.createDataFrame(expected_output, schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)