import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from ..jobs.job_one import do_actors_history_scd_transformation

def test_actors_history_scd_transformation(spark):
    # Create fake input data similar to the main job example
    input_data = [
        (1, "Actor A", "High", True, 2020),
        (1, "Actor A", "High", True, 2021),
        (1, "Actor A", "Medium", True, 2022),  # quality changes
        (2, "Actor B", "Low", True, 2020),
        (2, "Actor B", "Low", False, 2021)      # is_active changes
    ]
    columns = ["actorid", "actor", "quality_class", "is_active", "current_year"]
    actors_df = spark.createDataFrame(input_data, columns)

    actual_df = do_actors_history_scd_transformation(spark, actors_df)

    # Based on the logic:
    # Actor A:
    #   * Streak 1: High, True from 2020 to 2021
    #   * Streak 2: Medium, True from 2022 to 2022
    # Actor B:
    #   * Streak 1: Low, True from 2020 to 2020
    #   * Streak 2: Low, False from 2021 to 2021

    expected_data = [
        (1, "Actor A", "High", True, 2020, 2021, 2021),
        (1, "Actor A", "Medium", True, 2022, 2022, 2022),
        (2, "Actor B", "Low", True, 2020, 2020, 2020),
        (2, "Actor B", "Low", False, 2021, 2021, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data, actual_df.schema)

    assert_df_equality(actual_df, expected_df)
