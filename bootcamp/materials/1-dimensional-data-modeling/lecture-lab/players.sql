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
     seasons season_stats[],
<<<<<<< HEAD
     scorer_class scoring_class,
     years_since_last_active integer,
=======
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
>>>>>>> 715f22962d21bd6c5ebcd6ea1d90cad1742d587d
     current_season INTEGER,
     is_active BOOLEAN,
     PRIMARY KEY (player_name, current_season)
 );



