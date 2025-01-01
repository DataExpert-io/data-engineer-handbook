from chispa.dataframe_comparer import *

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from jobs.cumulative_table_job import cum_table_generation
from collections import namedtuple

# MonthlySiteHit = namedtuple("actorDetail",  "actor actorId film year ")
# MonthlySiteHitsAgg = namedtuple("MonthlySiteHitsAgg",  "month_start num_hits_first_day num_hits_second_day num_hits_third_day")


def cumulative_table_test(spark):
    input_data = [
        # Make sure basic case is handled gracefully
        (
         "Fred Astaire","nm0000001","Ghost Story", 1981,7731, 6.3 ,"tt0082449"
        ),
        (
         "Fred Astaire", "nm0000001", "The Purple Taxi", 1981, 533, 6.6, "tt0076851"
        ),
        (
         "Fred Astaire", "nm0000001", "The Amazing Dobermans", 1979, 369, 5.3, "tt0074130"
        ),
        (
         "Lauren Bacall", "nm0000002", "Ernest & Celestine", 1979, 18793, 7.9, "tt1816518"
        ),
        (
         "Lauren Bacall", "nm0000002", "The Forger", 1979, 4472, 5.4, "tt1368858"
        )
    ]
    schema = "actor string, actorId string, filmName string, year int, votes int, rating float, filmId string"

    source_df = spark.createDataFrame(input_data, schema)
    actual_df = cum_table_generation(spark, source_df)

    expected_values = [
        ("Fred Astaire",1979,[{"filmName": 'The Amazing Dobermans', "votes": 369, "rating": 5.3}], "bad", True),
        ("Fred Astaire", 1980, [], "bad", False),
        ("Fred Astaire", 1981, [{"filmName": 'The Purple Taxi', "votes": 533, "rating": 6.6},{"filmName": 'Ghost Story', "votes": 7731, "rating": 6.3}], "average", True),
        ("Lauren Bacall", 1979, [{"filmName": 'The Forger', "votes": 4472, "rating": 5.4},{"filmName": 'Ernest & Celestine', "votes": 18793, "rating": 7.9}], "average", True),
        ("Lauren Bacall", 1980, [], "bad", False),
        ("Lauren Bacall", 1981, [], "bad", False)
        ]
    schema2 = "actor string, film_year int, film_stats array<map<string, string>>, quality_class string, is_active boolean"
    expected_df = spark.createDataFrame(expected_values, schema2)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

