from chispa.dataframe_comparer import *
from collections import namedtuple
from ..jobs.hosts_cumulated_jobs import do_hosts_cumulated_transformation
Host = namedtuple("Host", "host dates_active date")
Host_Cumulated = namedtuple("Host_Cumulated", "host dates_active date")

def test_host_cumulated(spark):

    source_data = [
        Host("host1", ['2023-01-26','2023-01-28'],'2023-01-31'),
        Host("host2", ['2023-01-27','2023-01-28','2023-01-29'],'2023-01-31'),
        Host("host3", ['2023-01-29','2023-01-30'],'2023-01-31'),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_hosts_cumulated_transformation(spark, source_df,'2023-01-30','2023-01-31')
    expected_data = [
        Host_Cumulated("host1", ['2023-01-26','2023-01-28','2023-01-31'],'2023-01-31'),
        Host_Cumulated("host2", ['2023-01-27','2023-01-28','2023-01-29','2023-01-31'],'2023-01-31'),
        Host_Cumulated("host3", ['2023-01-29','2023-01-30','2023-01-31'],'2023-01-31')
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)

