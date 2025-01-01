from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def cum_table_generation(spark, dataframe):
    dataframe.createOrReplaceTempView("actor_films")
    min_year = dataframe.agg(f.min("year").alias("min_year")).select("min_year").collect()[0]["min_year"]
    max_year = dataframe.agg(f.max("year").alias("max_year")).select("max_year").collect()[0]["max_year"]
    query = f"""
                with years as (
        Select explode(sequence({min_year},{max_year},1)) as year
        ),
        actor_years as (
        Select actor, min(year) as actor_first_year, max(year) as actor_last_year from actor_films
        group by actor
        ), 
        year_range_actors as (
        Select ay.actor,y.year, ay.actor_first_year, ay.actor_last_year from actor_years ay, years y
        where y.year >= ay.actor_first_year
        ),
        actor_stats as (
        Select y.actor, y.year as film_year,
        collect_list(map('filmName', filmName, 'votes',votes, 'rating', rating)) as film_stats,
        row_number() over(partition by y.actor, y.year order by y.actor, y.year) as rn,
        avg(rating) as avg_rating
        from year_range_actors y 
        left join actor_films a 
        ON a.year = y.year and a.actor = y.actor
        group by y.actor, y.year
        )
        Select actor,
        film_year,
        FILTER(film_stats, x -> CASE WHEN x['filmName'] IS NOT NULL THEN true ELSE false END) AS film_stats,
        CASE
            WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating >7 and avg_rating <8 THEN 'good'
            WHEN avg_rating >6 and avg_rating<=7 THEN 'average'
            ELSE 'bad'
        END as quality_class,
        CASE WHEN size(FILTER(film_stats, x -> CASE WHEN x['filmName'] IS NOT NULL THEN true ELSE false END)) <> 0 THEN True ELSE False END as is_active
        
        from actor_stats
        where rn=1
                
                """
    return spark.sql(query)

# def main():
#     spark = SparkSession.builder \
#       .master("local") \
#       .appName("actor_table_generation") \
#       .getOrCreate()
#
#     df = test_monthly_site_hits(spark)
#     output_df = cum_table_generation(spark, df)
#     output_df.show(truncate = False)
#     output_df.printSchema()
#
# if __name__ == '__main__':
#     main()