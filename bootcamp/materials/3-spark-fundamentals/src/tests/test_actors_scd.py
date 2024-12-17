from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple

ActorYear = namedtuple("ActorYear", "actor current_year quality_class")
ActorScd = namedtuple("ActorScd", "actor quality_class start_year end_year")


def test_scd_generation(spark):
    source_data = [
        ActorYear("Meat Loaf", 2018, 'Good'),
        ActorYear("Meat Loaf", 2019, 'Good'),
        ActorYear("Meat Loaf", 2020, 'Bad'),
        ActorYear("Meat Loaf", 2021, 'Bad'),
        ActorYear("Skid Markel", 2020, 'Bad'),
        ActorYear("Skid Markel", 2021, 'Bad')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Meat Loaf", 'Good', 2018, 2019),
        ActorScd("Meat Loaf", 'Bad', 2020, 2021),
        ActorScd("Skid Markel", 'Bad', 2020, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)