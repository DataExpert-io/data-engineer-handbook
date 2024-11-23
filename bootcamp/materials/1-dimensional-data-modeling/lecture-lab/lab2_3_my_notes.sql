/* In the lab, we created the players_scd_table like this: */
create table players_scd (
    player_name text,
    scoring_class scoring_class,
    is_active boolean,
    start_season integer,
    end_season integer,
    current_season INTEGER,
    PRIMARY KEY(player_name, start_season)
);