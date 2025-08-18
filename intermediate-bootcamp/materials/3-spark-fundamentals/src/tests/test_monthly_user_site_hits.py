from chispa.dataframe_comparer import *

from ..jobs.monthly_user_site_hits_job import do_monthly_user_site_hits_transformation
from collections import namedtuple

MonthlySiteHit = namedtuple("MonthlySiteHit",  "month_start hit_array date_partition")
MonthlySiteHitsAgg = namedtuple("MonthlySiteHitsAgg",  "month_start num_hits_first_day num_hits_second_day num_hits_third_day")


def test_monthly_site_hits(spark):
    ds = "2023-03-01"
    new_month_start = "2023-04-01"
    input_data = [
        # Make sure basic case is handled gracefully
        MonthlySiteHit(
            month_start=ds,
            hit_array=[0, 1, 3],
            date_partition=ds
        ),
        MonthlySiteHit(
            month_start=ds,
            hit_array=[1, 2, 3],
            date_partition=ds
        ),
        #  Make sure empty array is handled gracefully
        MonthlySiteHit(
            month_start=new_month_start,
            hit_array=[],
            date_partition=ds
        ),
        # Make sure other partitions get filtered
        MonthlySiteHit(
            month_start=new_month_start,
            hit_array=[],
            date_partition=""
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_monthly_user_site_hits_transformation(spark, source_df, ds)

    expected_values = [
        MonthlySiteHitsAgg(
            month_start=ds,
            num_hits_first_day=1,
            num_hits_second_day=3,
            num_hits_third_day=6
        ),
        MonthlySiteHitsAgg(
            month_start=new_month_start,
            num_hits_first_day=0,
            num_hits_second_day=0,
            num_hits_third_day=0
        )
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)

