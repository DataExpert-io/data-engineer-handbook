from pyspark.sql import SparkSession

def do_actors_current_year_transformation(spark, actors_df, actor_films_df):
    # Register input DataFrames as temp views
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    # Convert the given SQL to Spark SQL
    # We'll remove the INSERT and just SELECT at the end.
    # Also note that Spark does not require CASTs like ::films_type[].
    # We'll assume that 'films' is represented as an array of structs with schema (film STRING, votes INT, rating DOUBLE, filmid INT).
    # We'll handle empty arrays using `array()` for an empty array of the appropriate struct type.
    
    query = """
    WITH last_year AS (
        SELECT
            actorid,
            actor,
            current_year,
            films,
            quality_class,
            is_active
        FROM
            actors
        WHERE
            current_year = 1971
    ),
    current_year AS (
        SELECT
            actorid,
            actor,
            year,
            CASE WHEN year IS NULL THEN array()
                 ELSE COLLECT_LIST(named_struct('film', film, 'votes', votes, 'rating', rating, 'filmid', filmid))
            END AS films,
            AVG(rating) AS avg_rating
        FROM
            actor_films
        WHERE
            year = 1972
        GROUP BY
            actorid, actor, year
    )
    SELECT
        COALESCE(ly.actorid, cy.actorid) AS actorid,
        COALESCE(ly.actor, cy.actor) AS actor,
        COALESCE(cy.year, ly.current_year + 1) as current_year,
        COALESCE(ly.films, array()) || COALESCE(cy.films, array()) AS films,
        CASE
            WHEN cy.films IS NOT NULL THEN
                CASE
                    WHEN cy.avg_rating > 8 THEN 'star'
                    WHEN cy.avg_rating > 7 THEN 'good'
                    WHEN cy.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END
            ELSE
                ly.quality_class
        END AS quality_class,
        (cy.films IS NOT NULL) AS is_active
    FROM
        last_year ly
    FULL OUTER JOIN
        current_year cy ON ly.actorid = cy.actorid
    """

    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_current_year_job") \
        .getOrCreate()

    # Example input data
    # Actors in 1971
    actors_data = [
        (1, "Actor A", 1971, [], "good", True),
        (2, "Actor B", 1971, [], "bad", False)
    ]
    actors_cols = ["actorid", "actor", "current_year", "films", "quality_class", "is_active"]
    actors_df = spark.createDataFrame(actors_data, actors_cols)

    # Actor films in 1972
    # This represents the actor_films table with columns: actorid, actor, film, votes, rating, filmid, year
    actor_films_data = [
        (1, "Actor A", "Film A1", 100, 8.5, 101, 1972),
        (1, "Actor A", "Film A2", 50, 7.0, 102, 1972),
        (2, "Actor B", "Film B1", 200, 9.0, 201, 1972)
    ]
    actor_films_cols = ["actorid", "actor", "film", "votes", "rating", "filmid", "year"]
    actor_films_df = spark.createDataFrame(actor_films_data, actor_films_cols)

    output_df = do_actors_current_year_transformation(spark, actors_df, actor_films_df)
    output_df.show(truncate=False)



if __name__ == "__main__":
    main()
