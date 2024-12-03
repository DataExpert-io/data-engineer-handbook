# NBA Game Data Analysis

## Overview
This project focuses on analyzing NBA game data, including deduplication, fact table creation, and advanced SQL analytics for player and team performance insights.

## Repository Structure


---

## 1. Deduplication Process
### Objective
The deduplication process ensures that duplicate records in the `game_details` table are removed based on `game_id`, `team_id`, and `player_id`.

### Steps
1. **Identify Duplicates:** Use `GROUP BY` and `COUNT` to find records with more than one occurrence.
2. **Assign Row Numbers:** Utilize the `ROW_NUMBER` function to assign unique numbers to each duplicate record.
3. **Filter:** Retain only the first occurrence of each record and eliminate the rest.

### SQL Example
```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
    FROM game_details
)
SELECT *
FROM deduped
WHERE row_num = 1;
