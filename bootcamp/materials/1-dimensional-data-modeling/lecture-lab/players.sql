CREATE TYPE season_stats AS (
    season Integer,
    pts REAL,
    ast REAL,
    reb REAL,
    weight INTEGER
);
CREATE TYPE scoring_class AS
ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons SEASON_STATS [],
    scorer_class SCORING_CLASS,
    years_since_last_active INTEGER,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);
