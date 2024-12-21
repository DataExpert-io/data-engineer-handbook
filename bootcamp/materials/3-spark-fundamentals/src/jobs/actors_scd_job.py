from pyspark.sql import SparkSession

query = """
with changes as (
	select
		actorid
		, actor
		, quality_class
		, is_active
		, current_year
		, lag(quality_class, 1) over (partition by actorid order by current_year) != quality_class
			or lag(is_active, 1) over (partition by actorid order by current_year) != is_active
			as changed
	from
		actors
)

, change_identifier as (
	select
		actorid
		, actor
		, is_active
		, quality_class
		, current_year
		, sum(
			case
				when changed is null then 1
				when changed then 1
				else 0
			end
		) over (partition by actorid order by current_year) as change_identifier
	from
		changes
)

, grouped as (
	select
		actorid
		, actor
		, is_active
		, quality_class
		, change_identifier
		, min(current_year) as start_year
		, max(current_year) as end_year
	from
		change_identifier
	group by
		1, 2, 3, 4, 5
)

select
	actor
    , actorid
	, quality_class
	, is_active
	, start_year
	, end_year
from grouped
"""


def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")