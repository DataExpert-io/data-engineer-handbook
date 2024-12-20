from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple

ActorYear = namedtuple("ActorYear", "actor actorid current_year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    source_data = [
        ActorYear("Meat Loaf", 123, 2018, 'Good', 1),
        ActorYear("Meat Loaf", 123, 2019, 'Good', 1),
        ActorYear("Meat Loaf", 123, 2020, 'Bad', 1),
        ActorYear("Meat Loaf", 123, 2021, 'Bad', 1),
        ActorYear("Skid Markel", 321, 2020, 'Bad',1),
        ActorYear("Skid Markel", 321, 2021, 'Bad',1)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Meat Loaf", 123, 'Good', 1, 2018, 2019,2021),
        ActorScd("Meat Loaf", 123, 'Bad', 1, 2020, 2021,2021),
        ActorScd("Skid Markel", 321, 'Bad', 1, 2020, 2021,2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df,  ignore_nullable=True)