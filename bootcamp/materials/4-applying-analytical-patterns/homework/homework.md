# Week 4 Applying Analytical Patterns
The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`
  
- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  - Aggregate this dataset along the following dimensions
    - player and team
      - Answer questions like who scored the most points playing for one team?
    - player and season
      - Answer questions like who scored the most points in one season?
    - team
      - Answer questions like which team has won the most games?
      
- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch? 
  - How many games in a row did LeBron James score over 10 points a game?


Please add these queries into a folder `homework/<discord-username>`