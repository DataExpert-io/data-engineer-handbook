-- Insert historical data into players_scd
INSERT INTO players_scd (player_name, scoring_class, is_active, start_season, end_season, current_season)
VALUES
    ('Player A', 'Good', TRUE, 2020, 2021, 2021),
    ('Player B', 'Bad', FALSE, 2019, 2021, 2021),
    ('Player C', 'Good', TRUE, 2020, 2021, 2021);

-- Insert current season data into players
INSERT INTO players (player_name, scoring_class, is_active, current_season)
VALUES
    ('Player A', 'Good', TRUE, 2022),
    ('Player B', 'Good', TRUE, 2022), -- Changed scoring class
    ('Player D', 'Bad', TRUE, 2022); -- New player
