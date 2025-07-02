from chispa.dataframe_comparer import assert_df_equality
from ..jobs.dedupe_job import do_game_details_transformation
from collections import namedtuple

# Define namedtuple for input data structure
GameDetails = namedtuple("GameDetails", "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk TO pf pts plus_minus")

def test_game_details_transformation(spark):
    # Sample input data with duplicate records
    input_data = [
        GameDetails(22200163, 1610612765, "DET", "Detroit", 1630191, "Isaiah Stewart", "Isaiah", "C", "", "20:29", 3, 6, 0.5, 1, 4, 0.25, 5, 6, 0.833, 3, 7, 10, 1, 0, 0, 1, 4, 12, -2),  # Original record
        GameDetails(22200163, 1610612765, "DET", "Detroit", 1630191, "Isaiah Stewart", "Isaiah", "C", "", "20:29", 3, 6, 0.5, 1, 4, 0.25, 5, 6, 0.833, 3, 7, 10, 1, 0, 0, 1, 4, 12, -2),  # Duplicate record
        GameDetails(22200163, 1610612765, "DET", "Detroit", 1630191, "Isaiah Stewart", "Isaiah", "C", "", "20:29", 3, 6, 0.5, 1, 4, 0.25, 5, 6, 0.833, 3, 7, 10, 1, 0, 0, 1, 4, 12, -2),  # Another duplicate record
        GameDetails(22200163, 1610612738, "BOS", "Boston", 1628369, "Jayson Tatum", "Jayson", "F", "", "31:35", 10, 20, 0.5, 5, 11, 0.455, 6, 7, 0.857, 0, 1, 1, 5, 1, 1, 1, 4, 31, 16),  # Unique record
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_game_details_transformation(spark, input_dataframe)
    
    # Expected output after deduplication
    expected_output = [
        GameDetails(22200163, 1610612765, "DET", "Detroit", 1630191, "Isaiah Stewart", "Isaiah", "C", "", "20:29", 3, 6, 0.5, 1, 4, 0.25, 5, 6, 0.833, 3, 7, 10, 1, 0, 0, 1, 4, 12, -2),  # Only the first record for game_id=22200163, team_id=1610612765, player_id=1630191
        GameDetails(22200163, 1610612738, "BOS", "Boston", 1628369, "Jayson Tatum", "Jayson", "F", "", "31:35", 10, 20, 0.5, 5, 11, 0.455, 6, 7, 0.857, 0, 1, 1, 5, 1, 1, 1, 4, 31, 16),
    ]
    expected_df = spark.createDataFrame(expected_output)
    
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)