/* Here we create the type season_stats and the players table.
 Have a look at the player_seasons table already provided */
CREATE TYPE season_stats AS (
    season Integer,
    gp REAL,
    pts REAL,
    reb REAL,
    ast REAL,
    weight INTEGER
);
-- CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons season_stats [],
    --scorer_class scoring_class,
    --is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);
/* 
 The player_seasons table has the attributes: player_name,
 const_info (height, college, country, draft_year, draft_round, draft_number),
 season_dependent_info (like gp, pts, reb, ast, weight, and season).
 The players table has:
 player_name, const_info, seasons which is an array of struct season_stats, and
 current_season. Basically, for each player we have their history from previous
 seasons.
 There are attributes in player_seasons that we don't care much about like age
 and other season-dependent values like netrtg, oreb-pct,...etc
 */