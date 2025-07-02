from pyspark.sql import SparkSession


def do_actor_films_transformation(spark, actors_df, actor_films_df, year):
    query = f"""
    WITH last_ds AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = '{year - 1}'
    ),
    films_compress AS (
        SELECT
            actorid,
            actor,
            NAMED_STRUCT(
                'film', film,
                'votes', votes,
                'rating', rating,
                'filmid', filmid
            ) AS film_c,
            year
        FROM
            actor_films
        WHERE
            year = '{year}'
    ),
    ds AS (
        SELECT
            actorid,
            actor,
            ARRAY_AGG(film_c) AS films,
            AVG(film_c.rating) AS average_rating,
            year
        FROM
            films_compress
        GROUP BY
            actorid,
            actor,
            year
    )
    SELECT 
        COALESCE(t.actorid, l.actorid) as actorid,
        COALESCE(t.actor, l.actor) as actor,
        COALESCE(l.films, ARRAY()) || COALESCE(t.films, ARRAY()) AS films,
        CASE
            WHEN t.average_rating IS NOT NULL THEN
            (
                CASE
                    WHEN t.average_rating > 8 THEN 'star'
                    WHEN t.average_rating > 7 AND t.average_rating <= 8 THEN 'good'
                    WHEN t.average_rating > 6 AND t.average_rating <= 7 THEN 'average'
                    ELSE 'bad'
                END
            )
            ELSE l.quality_class
        END AS quality_class,
        t.films IS NOT NULL AS is_active,
        '{year}' AS current_year
    FROM
        ds t
    FULL OUTER JOIN
        last_ds l ON t.actorid = l.actorid
    """
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")
    return spark.sql(query)

def main():
    year = 1970
    spark = SparkSession.builder \
        .master("local") \
        .appName("actor_films_scd") \
        .getOrCreate()
    output_df = do_actor_films_transformation(spark, spark.table("actors"), spark.table("actor_films"), year)
    output_df.write.mode("overwrite").insertInto("actor_films_scd")