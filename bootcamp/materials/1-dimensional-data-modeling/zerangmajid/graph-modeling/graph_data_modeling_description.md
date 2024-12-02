# Graph Data Modeling Description


- [Overview](#overview)
- [Graph Data Relationships](#graph-data-relationships)
- [Flexible Schemas](#flexible-schema)
- [Repository Structure](#repository-structure)
- [Key Steps](#key-steps)



## Overview
**Graph data modeling** focuses on  **relationships** rather than  **entities**. It uses  **vertices** and  **edges** to represent interconnected data. 
Graph data modeling is a technique focused on capturing relationships between data entities. Unlike traditional models that prioritize tables and rows, graph models use nodes and edges, making them highly effective for use cases like social networks, recommendation systems, and relationship-driven datasets.

#### Why Graph Data Modeling?
- Captures complex relationships efficiently.
- Ideal for analyzing networks, social connections, and player interactions.

This document describes the **SQL scripts** used for **modeling graph data**. The repository is organized to showcase:
The database structure consists of **vertices** and **edges** that represent players, teams, games, and their relationships.

#### Additive vs. Non-Additive Dimensions
- **Additive Dimensions**: Data you can sum without overlap (e.g., Age groups in a population).
- **Non-Additive Dimensions**: Data prone to double counting (e.g., Active users across devices).

### Graph Data Relationships
This section visualizes the relationships between players, teams, and games within a graph database.

What Does the Graph Represent? This visualization illustrates relationships within a graph database:

- **Vertices** (Nodes): Represent entities like players, teams, and games.
- **Edges** (Relationships): Define the interactions or relationships between these entities, such as "plays against" or "shares teams."

![Graph Data Relationships](./resources/Graph%20Data%20Relationships.png)
<p align="center">Visualization of graph data relationships including players, teams, and games.</p>


### Flexible Schema
#### What is a Flexible Schema?
Flexible schemas allow modeling data from disparate sources into a unified shared schema. They are often used in systems where the structure of data is variable or evolves over time.

#### Benefits of Flexible Schemas
- **Ease of Modification**: Avoid frequent `ALTER TABLE` commands, making schema changes seamless.
- **Manage More Columns**: Handle complex data with numerous columns effectively.
- **Minimize NULL Columns**: Flexible schemas reduce unused or NULL columns.
- **Dynamic Properties**: Use fields like `other_properties` for rarely used but necessary data fields.
- 
#### Drawbacks of Flexible Schemas
- **Compression Issues**: Compression may be less effective, especially with JSON fields.
- **Query Complexity**: Querying and reading such schemas can be less intuitive or slower.ds.
Query Complexity: Querying and reading such schemas can be less intuitive or slower.


## Repository Structure
This section provides links to key SQL scripts for setting up and querying the graph database.

- **[1. graph_setup.sql](./1.graph_setup.sql)**: Sets up the graph database schema with vertices and edges.

- **[2. data_insertion.sql](./2.data_insertion.sql)**: Inserts sample data into the graph database.
- **[3. queries.sql](./3.queries.sql)**: Executes various queries to extract insights from the graph database.
 



## Key Steps

### 1. Database Setup
The graph_setup.sql file defines the schema of the graph database, including:

Enumerated Types: Used to categorize vertices and edges.
Vertices Table: Stores nodes (e.g., players, teams, games) with their properties.
Edges Table: Stores relationships between nodes (e.g., "plays_in", "shares_team").[View Script](./1.graph_setup.sql)

### 2. Data Insertion
 The data_insertion.sql file populates the database with sample data for vertices and edges, representing:

Players and their properties (e.g., name, performance metrics).
Teams and games, along with their attributes.
Relationships such as which player played in which game or belongs to which team.
[View Script](./2.data_insertion.sql)


### 3. Querying the Database
The queries.sql file contains SQL queries to extract insights from the graph database. These include:

Player Performance: Analyze points scored, games played, and team affiliations.
Graph Relationships: Identify connections between players, teams, and games.
Custom Queries: Calculate metrics like games-to-points ratio and relationships between players.

### Example Query: Games Against Opponent
**Purpose**: Analyze how many games players have played against specific opponents and their scoring performance.

```sql
SELECT v.properties ->> 'player_name' AS player_name,
       e.object_identifier AS opponent_identifier,
       e.properties ->> 'subject_points' AS subject_points,
       e.properties ->> 'num_games' AS games_against_opponent
FROM vertices v
JOIN edges e ON v.identifier = e.subject_identifier
WHERE e.object_type = 'player'::vertex_type;
```
## Result Preview:
| Player Name    | Opponent ID | Subject Points | Games Against Opponent |
|----------------|-------------|----------------|-------------------------|
| Vince Carter   | 977         | 13             | 2                       |
| Vince Carter   | 1495        | 55             | 5                       |
| Dirk Nowitzki  | 708         | 29             | 1                       |
| Dirk Nowitzki  | 1495        | 4              | 1                       |

Key Insight:

Vince Carter has played 5 games against the player with ID 1495 and scored a total of 55 poins

[View Script](./3.queries.sql)


## Example Query: Player Statistics Aggregation
```sql
SELECT
    v.properties ->> 'player_name' AS player_name,
    AVG(CAST(v.properties ->> 'total_points' AS NUMERIC)) AS avg_points,
    COUNT(*) AS total_games
FROM vertices v
WHERE v.type = 'player'::vertex_type
GROUP BY v.properties ->> 'player_name'
ORDER BY avg_points DESC;
```
## Result Preview:
| Player Name    | Average Points | Total Games |
|----------------|----------------|-------------|
| Dirk Nowitzki	 | 25.7	          | 82          |
| Vince Carter   | 22.3           | 74          |

Key Insight:

Dirk Nowitzki has the highest average points per game among all players.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 

### Notes
- Remove duplicates using **ROW_NUMBER()** to maintain data integrity.
- Use **JSON properties** to allow flexibility in storing additional data.
- Index **fields** that are frequently queried for improved performance.
## Conclusion
Graph data modeling is an efficient way to represent and analyze complex relationships. This guide provided a detailed overview of graph database concepts, SQL scripts for implementation, and sample queries for extracting insights. By leveraging flexible schemas and graph structures, users can address real-world use cases like social networks, recommendations, and more.

### More Detailed Setup Instructions
For a complete guide on setting up and running this project using Docker Compose and PostgreSQL, visit the [Data Modeling Setup Page](https://github.com/zerangmajid/data-engineer-handbook/tree/main/bootcamp/materials/1-dimensional-data-modeling).





