# Graph Data Modeling Description

## Overview
This document describes the **SQL scripts** used for modeling graph data. The database structure consists of **vertices** and **edges** that represent players, teams, games, and their relationships.

## Steps
### Step 1: Enumerated Types
Define types for **vertices** and **edges** to categorize data.

```sql
CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');
CREATE TYPE edge_type AS ENUM('plays_against', 'shares_team', 'plays_in', 'plays_on');
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Step 2: Create Tables
## Vertices Table
Define the vertices table to store nodes (players, teams, or games) with their properties.
CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

## Edges Table
Define the edges table to represent relationships between nodes.
CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Step 3: Populate Data
## Insert Games into Vertices
Populate the vertices table with game information.
INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
    ) AS properties
FROM games;


##Insert Players into Vertices
Populate the vertices table with player information.
WITH players_agg AS (
    SELECT
        player_id AS identifier,
        MAX(player_name) AS player_name,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points,
        ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id
)
INSERT INTO vertices
SELECT
    identifier,
    'player'::vertex_type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    ) AS properties
FROM players_agg;

##Insert Teams into Vertices
Populate the vertices table with team information.
INSERT INTO vertices
SELECT
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) AS properties
FROM teams;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Step 4: Create Edges
Create plays_in Edges
Establish relationships between players and games.

INSERT INTO edges
SELECT
    player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) AS properties
FROM game_details;

##Create plays_against and shares_team Edges
Create edges to represent player matchups and teammates.

WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered AS (
    SELECT *
    FROM deduped
    WHERE row_num = 1
),
aggregated AS (
    SELECT
        f1.player_id AS subject_player_id,
        f2.player_id AS object_player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END AS edge_type,
        COUNT(1) AS num_games,
        SUM(f1.pts) AS subject_points,
        SUM(f2.pts) AS object_points
    FROM filtered f1
    JOIN filtered f2
        ON f1.game_id = f2.game_id
        AND f1.player_id <> f2.player_id
    WHERE f1.player_id > f2.player_id
    GROUP BY
        f1.player_id,
        f2.player_id,
        CASE
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
INSERT INTO edges
SELECT
    subject_player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    object_player_id AS object_identifier,
    'player'::vertex_type AS object_type,
    edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    ) AS properties
FROM aggregated;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
### Step 5: Analyze the Data
##Fetch Players

SELECT * FROM vertices WHERE type = 'player';

##Fetch Player Relationships
SELECT
    v.properties ->> 'player_name' AS player_name,
    e.object_identifier AS opponent_identifier,
    e.properties ->> 'num_games' AS games_against_opponent
FROM vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;

## Top Scoring Players Against Opponents

SELECT
    v.properties ->> 'player_name',
    MAX(e.properties ->> 'subject_points') AS max_points
FROM vertices v
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.edge_type = 'plays_against'::edge_type
GROUP BY v.properties ->> 'player_name'
ORDER BY max_points DESC;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 

### Notes
- Remove duplicates using **ROW_NUMBER()** to maintain data integrity.
- Use **JSON properties** to allow flexibility in storing additional data.
- Index **fields** that are frequently queried for improved performance.



