from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from jobs.actors_scd_jobs import do_actors_scd_transformation  # Adjust the import path as needed

# Define namedtuples for source and expected data
Actor = namedtuple("Actor", "actorid quality_class is_active current_year")
ActorScd = namedtuple("ActorScd", "actorid quality_class start_year end_year is_active current_year")

def test_scd(spark):
    """
    Test the do_actors_scd_transformation function to verify SCD logic.

    Args:
        spark (SparkSession): Active Spark session for testing.
    """
    # Source data setup
    source_data = [
        Actor("11", 'Good', True, 2024),
        Actor("22", 'Good', False, 2024),
        Actor("33", 'Bad', True, 2024),
        Actor("44", 'Bad', False, 2024)
    ]
    source_df = spark.createDataFrame(source_data)

    # Call the transformation function
    actual_df = do_actors_scd_transformation(spark, source_df)

    # Expected data setup
    expected_data = [
        ActorScd("11", 'Good', 2023, 2024, True, 2024),
        ActorScd("22", 'Good', 2023, 2024, False, 2024),
        ActorScd("33", 'Bad', 2023, 2024, True, 2024),
        ActorScd("44", 'Bad', 2023, 2024, False, 2024)
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Validate equality of actual and expected DataFrames
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

