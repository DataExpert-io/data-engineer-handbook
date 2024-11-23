/* In this lab, we work with the players table from lab1. Only that we need to add the 
 attribute is_active.*/
/* STEP1: Creating the types and the players table */
-- CREATE TYPE season_stats AS (
--     season Integer,
--     gp REAL,
--     pts REAL,
--     reb REAL,
--     ast REAL
--     -- weight INTEGER
-- );
-- CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
-- CREATE TABLE players (
--     player_name TEXT,
--     height TEXT,
--     college TEXT,
--     country TEXT,
--     draft_year TEXT,
--     draft_round TEXT,
--     draft_number TEXT,
--     season_stats season_stats [],
--     scoring_class scoring_class,
--     years_since_last_season INTEGER,
--     is_active BOOLEAN,
--     current_season INTEGER,
--     PRIMARY KEY (player_name, current_season)
-- );
/* -----------------------------------------------------------------------*/
/* STEP2: Populating the players table using the load_players_table_day2.sql file 
 found in the sql folder.
 Let us see what each piece of that code does. */
/* FIRST PIECE: the years table: */
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
)
/*This creates a table called years with one column called season with values ranging from
 1996 to 2022 */
/* -----------------------------------*/
/* SECOND PIECE: the p table */
p AS (
    SELECT player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
)
/*This creates a table with the first season a player started playing. */
/* -----------------------------------*/
/* THIRD PIECES: the players_and_seasons table */
SELECT *
FROM p
    JOIN years y ON p.first_season <= y.season
    /* This creates a table with multiple rows for each player, where the
     repeated info is the player_name and first_season and the new info in each
     row is the season. Basically, for a fixed player, we have a row for each
     season they played AFTER (and including) their first season. Example:
     check for the player Eldridge Recasner: */
    WITH years AS (
        SELECT *
        FROM GENERATE_SERIES(1996, 2022) AS season
    ),
    p AS (
        SELECT player_name,
            MIN(season) AS first_season
        FROM player_seasons
        GROUP BY player_name
    )
SELECT *
FROM p
    JOIN years y ON p.first_season <= y.season
where player_name = 'Eldridge Recasner';
/* The player, of course, doesn't necessarily have to have played all seasons 
 after their first season. */
/* -----------------------------------*/
/* FOURTH PIECE: */
/* first, let us understand the left join:
 FROM players_and_seasons pas
 LEFT JOIN player_seasons ps
 ON pas.player_name = ps.player_name
 AND pas.season = ps.season
 ORDER BY pas.player_name, pas.season
 
 The query in other words: for each player: for each season after (and including)
 their first season, we now have constant info about them and season-dependent
 info retrieved from the player_seasons table. */
/* Now let us see which attributes we want from the result of the left join:
 Basically, the seasons attribute. Understand this code: */
ARRAY_REMOVE(
    ARRAY_AGG(
        CASE
            WHEN ps.season IS NOT NULL THEN ROW(
                ps.season,
                ps.gp,
                ps.pts,
                ps.reb,
                ps.ast
            )::season_stats
        END
    ) OVER (
        PARTITION BY pas.player_name
        ORDER BY COALESCE(pas.season, ps.season)
    ),
    NULL
) AS seasons
/*ARRAY_REMOVE(ARRAY_AGG, NULL) removes NULL values from the array.
 ARRAY_AGG: For the same player, for each season they DID play, we add that season's info
 to the array and we cast it to type season_stats. 
 Attributes in the final table: player_name, season (which has values only of seasons
 the player did play) and season_stats up till that season.*/
/* -----------------------------------*/
/* FIFTH PIECE: the static table */
SELECT player_name,
    MAX(height) AS height,
    MAX(college) AS college,
    MAX(country) AS country,
    MAX(draft_year) AS draft_year,
    MAX(draft_round) AS draft_round,
    MAX(draft_number) AS draft_number
FROM player_seasons
GROUP BY player_name
    /*This table gives the constant attributes about each player ever participated. */
    /* -----------------------------------*/
    /* SIXTH PIECE: the final result 
     Now, we inner join the windowed and static tables. Again, for each player, for each season after
     the first season they played, we have their season_stats up till that season and their constant
     info. The scoring_class and years_since_last_active are clear. is_active basically tells us
     if they played in the season or not. */