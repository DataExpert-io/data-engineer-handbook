from chispa.dataframe_comparer import *
from ..jobs.host_activty_reduced_job import do_host_activity_reduced_transformation
from collections import namedtuple
import pyspark.sql.functions as F

Events = namedtuple("Events", "host event_time user_id")
HostActivityReduced = namedtuple("HostActivityReduced", "host month_start hit_array unique_visitors_array max_computed_date")
InitialHostActivityReduced = namedtuple("InitialHostActivityReduced", "host month_start hit_array unique_visitors_array max_computed_date")

def test_host_activity_reduced_query_existing_data(spark):

    date_to_compute = '2025-01-02'
    source_data = [
        Events(
            host="host1", event_time="2025-01-01", user_id=1
        )
        , Events(
            host="host1", event_time="2025-01-02", user_id=1
        )
        , Events(
            host="host1", event_time="2025-01-02", user_id=2
        )
        , Events(
            host="host2", event_time="2025-01-02", user_id=1
        )
    ]
    source_df = spark.createDataFrame(source_data)

    initial_data = [
        InitialHostActivityReduced(
            host="host1", month_start="2025-01-01", hit_array=[1], unique_visitors_array=[1], max_computed_date="2025-01-01"
        )
    ]

    # create already exisitng actors
    spark.createDataFrame(initial_data).createOrReplaceTempView("host_activity_reduced")

    actual_df = do_host_activity_reduced_transformation(spark, source_df, date_to_compute)

    actual_df.show()

    source_df.show()

    expected_data = [
        HostActivityReduced(
            host="host1", month_start="2025-01-01", hit_array=[1, 2], unique_visitors_array=[1, 2], max_computed_date="2025-01-02"
        )
        , HostActivityReduced(
            host="host2", month_start="2025-01-01", hit_array=[0, 1], unique_visitors_array=[0, 1], max_computed_date="2025-01-02"
        )
    ]
    expected_df = spark.createDataFrame(expected_data).withColumn("max_computed_date", F.to_date("max_computed_date"))

    expected_df.show()

    assert_df_equality(actual_df, expected_df)
