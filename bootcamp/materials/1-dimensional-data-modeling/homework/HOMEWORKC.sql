CREATE TYPE SCD_TYPE AS (
	QUALITY_CLASS QUALITY_CLASS,
	IS_ACTIVE BOOLEAN,
	START_YEAR INTEGER,
	END_YEAR INTEGER

)


WITH LAST_YEAR_SCD AS (
    SELECT * FROM ACTORS_HISTORY_SCD
    WHERE CURRENT_YEAR = 1979
      AND END_YEAR = 1979
),
     HISTORICAL_SCD AS (
         SELECT ACTOR, QUALITY_CLASS, IS_ACTIVE, START_YEAR, END_YEAR FROM ACTORS_HISTORY_SCD
         WHERE CURRENT_YEAR = 1979
           AND END_YEAR < 1979
     ),
     THIS_YEAR_SCD AS (
         SELECT * FROM ACTORS_HISTORY_SCD
         WHERE CURRENT_YEAR = 1980
     ),
     UNCHANGED_RECORDS AS (
         SELECT TY.ACTOR,
                TY.QUALITY_CLASS,
                TY.IS_ACTIVE,
                LY.START_YEAR,
                TY.CURRENT_YEAR AS END_YEAR

         FROM THIS_YEAR_SCD TY

                  JOIN LAST_YEAR_SCD LY

                       ON LY.ACTOR = TY.ACTOR

         WHERE TY.IS_ACTIVE = LY.IS_ACTIVE
           AND TY.QUALITY_CLASS = LY.QUALITY_CLASS

     ),
     CHANGED AS (
         SELECT TY.ACTOR,
                UNNEST ( ARRAY [
                             ROW(
                                     LY.QUALITY_CLASS,
                                     LY.IS_ACTIVE,
                                     LY.START_YEAR,
                                     LY.END_YEAR
                             ):: SCD_TYPE,
                         ROW (
                                 TY.QUALITY_CLASS,
                                 TY.IS_ACTIVE,
                                 TY.CURRENT_YEAR,
                                 TY.CURRENT_YEAR
                         ):: SCD_TYPE

		]) AS RECORDS

         FROM THIS_YEAR_SCD TY

                  LEFT JOIN LAST_YEAR_SCD LY

                            ON LY.ACTOR = TY.ACTOR

         WHERE (TY.IS_ACTIVE <> LY.IS_ACTIVE
             OR TY.QUALITY_CLASS <> LY.QUALITY_CLASS)

     ),
     UNNESTED_CHANGED_RECORDS AS (

         SELECT ACTOR,
                (RECORDS::SCD_TYPE).QUALITY_CLASS,
    (RECORDS::SCD_TYPE).IS_ACTIVE,
    (RECORDS::SCD_TYPE).START_YEAR,
    (RECORDS::SCD_TYPE).END_YEAR

FROM CHANGED

    ),
    NEW_RECORDS AS (
SELECT TY.ACTOR, TY.QUALITY_CLASS, TY.IS_ACTIVE, TY.CURRENT_YEAR AS START_YEAR,
    TY.CURRENT_YEAR AS END_YEAR
FROM THIS_YEAR_SCD TY
    LEFT JOIN LAST_YEAR_SCD LY
ON LY.ACTOR = TY.ACTOR
WHERE LY.ACTOR IS NULL
    )
SELECT * FROM HISTORICAL_SCD

UNION ALL

SELECT * FROM UNCHANGED_RECORDS

UNION ALL

SELECT * FROM UNNESTED_CHANGED_RECORDS

UNION ALL

SELECT * FROM NEW_RECORDS


