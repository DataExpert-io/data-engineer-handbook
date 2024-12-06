-- Create the table for players
CREATE TABLE players (
    player_name TEXT PRIMARY KEY,
    scoring_class TEXT,
    is_active BOOLEAN,
    current_season INTEGER
);

-- Create the table for players_scd
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class TEXT,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season, end_season)
);

-- Create a type for SCD processing (used in later scripts)
CREATE TYPE scd_type AS (
    scoring_class TEXT,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);
