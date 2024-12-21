from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple

Actors = namedtuple("Actors", "actor actorid current_year quality_class is_active")
ActorsScd = namedtuple("ActorsScd", "actor actorid quality_class is_active start_year end_year")

def test_scd_generation_change_quality(spark):
    source_data = [
        Actors(
            actor="actor1", actorid=1, current_year=2001, quality_class='Good', is_active=True
        ),
        Actors(
            actor="actor1", actorid=1, current_year=2002, quality_class='Good', is_active=True
        ),
        Actors(
            actor="actor1", actorid=1, current_year=2003, quality_class='Bad', is_active=True
        ),
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)

    actual_df.show()

    expected_data = [
        ActorsScd(
            actor="actor1", actorid=1, quality_class='Good', is_active=True, start_year=2001, end_year=2002
        )
        , ActorsScd(
            actor="actor1", actorid=1, quality_class='Bad', is_active=True, start_year=2003, end_year=2003
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    expected_df.show()
    assert_df_equality(actual_df, expected_df)


def test_scd_generation_change_activity(spark):
    source_data = [
        Actors(
            actor="actor1", actorid=1, current_year=2001, quality_class='Good', is_active=True
        ),
        Actors(
            actor="actor1", actorid=1, current_year=2002, quality_class='Good', is_active=False
        )
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)

    actual_df.show()

    expected_data = [
        ActorsScd(
            actor="actor1", actorid=1, quality_class='Good', is_active=True, start_year=2001, end_year=2001
        )
        , ActorsScd(
            actor="actor1", actorid=1, quality_class='Good', is_active=False, start_year=2002, end_year=2002
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    expected_df.show()
    assert_df_equality(actual_df, expected_df)
