# tests/test_player_progress.py

from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from ..jobs.player_progress_job import do_progress_calculation


#  Define input and expected schemas using namedtuples
Player = namedtuple("Player", "player_name seasons current_season")
SeasonStats = namedtuple("SeasonStats", "season pts ast reb weight")
ExpectedProgress = namedtuple("ExpectedProgress", "player_name first_season last_season progress")


def test_player_progress(spark):
    # Input data: Kobe with two seasons
    input_data = [
        Player(
            "Kobe Bryant",
            [
                SeasonStats(2002, 15.4, 3.5, 4.3, 210),
                SeasonStats(2016, 17.6, 2.8, 3.5, 212),
            ],
            2016
        )
    ]
    input_df = spark.createDataFrame(input_data)

    # Call transformation logic (from job function)
    result_df = do_progress_calculation(input_df)

    # Expected result
    expected_data = [
        ExpectedProgress("Kobe Bryant", 15.4, 17.6, 17.6 / 15.4)
    ]
    expected_df = spark.createDataFrame(expected_data)

    #  Assertion
    assert_df_equality(result_df, expected_df, ignore_nullable=True, precision=7)
