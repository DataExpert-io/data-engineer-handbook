# tests/test_actors_history_scd_job.py

from chispa.dataframe_comparer import assert_df_equality
from ..jobs.actors_history_scd_job import do_actor_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor", "actor quality_class is_active year")
ActorHistory = namedtuple("ActorHistory", "actor quality_class is_active start_date end_date")

def test_actor_scd_generation(spark):
    # Input data (as per your example)
    source_data = [
        Actor("50 Cent", "bad", True, 2005),
        Actor("50 Cent", "bad", False, 2007),
        Actor("A Martinez", "bad", True, 2001),
        Actor("A Martinez", "bad", False, 2002),
        Actor("A Martinez", "bad", False, 2003),
    ]
    source_df = spark.createDataFrame(source_data)

    # Call transformation
    actual_df = do_actor_scd_transformation(spark, source_df, cutoff_year=2010)

    # Expected output
    expected_data = [
        ActorHistory("50 Cent", "bad", True, 2005, 2005),
        ActorHistory("50 Cent", "bad", False, 2007, 2007),
        ActorHistory("A Martinez", "bad", True, 2001, 2001),
        ActorHistory("A Martinez", "bad", False, 2002, 2003),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Assert equality
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_column_order=True)
