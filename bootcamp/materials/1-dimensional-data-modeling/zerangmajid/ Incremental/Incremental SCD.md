# **Incremental Slowly Changing Dimensions (SCD) with PostgreSQL**
This project demonstrates how to efficiently implement incremental Slowly Changing Dimensions (SCD) Type 2 using PostgreSQL. The approach focuses on updating only the necessary data from the last two seasons (2021 and 2022) rather than processing the entire dataset. This optimization results in significant performance improvements.



## **Table of Contents**

1. [Introduction](#introduction)  
2. [Key Concepts](#key-concepts)  
3. [Project Implementation](#project-implementation)  
   - [Schema Setup](#schema-setup)  
   - [Data Insertion](#data-insertion)  
   - [Incremental Processing](#incremental-processing)  
4. [Performance Optimization](#performance-optimization)  
5. [Results and Insights](#results-and-insights)  
6. [Conclusion](#conclusion)  


## **Introduction**

Slowly Changing Dimensions (SCD) Type 2 tracks historical changes in data by preserving the history of modifications. This approach is widely used in data warehousing and business intelligence. However, processing large datasets can be resource-intensive.

This project optimizes the SCD implementation by:  
- Limiting processing to only recent records.  
- Updating only relevant data (changed, new, or active).  
- Excluding historical data from unnecessary re-processing.  

## **Key Concepts**
### **What is SCD Type 2?**
SCD Type 2 maintains historical data for dimensional attributes. Each record includes:  
- **Start Date** and **End Date** to track changes over time.  
- **A Current Indicator** to mark active records.  

### **Data Categorization**
We divide the data into the following categories for efficient processing:  
- **Unchanged Records:** Players whose attributes remain the same.  
- **Changed Records:** Players whose attributes have changed (e.g., scoring class or active status).  
- **New Records:** Players appearing for the first time in the dataset.  

## **Project Implementation**

### **Schema Setup**
The schema consists of:

Players Table: Contains current season data.
Players_SCD Table: Tracks historical and current records with SCD Type 2 logic.

```sql
CREATE TABLE players (
    player_name TEXT PRIMARY KEY,
    scoring_class TEXT,
    is_active BOOLEAN,
    current_season INTEGER
);

CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class TEXT,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER
);
```
## **Data Insertion**

We insert both historical and current season data into the tables for demonstration:

```sql
-- Insert historical data for 2021
INSERT INTO players_scd (player_name, scoring_class, is_active, start_season, end_season, current_season)
VALUES 
('Player A', 'Good', TRUE, 2019, 2021, 2021),
('Player B', 'Bad', FALSE, 2020, 2021, 2021);

-- Insert current season data for 2022
INSERT INTO players (player_name, scoring_class, is_active, current_season)
VALUES 
('Player A', 'Good', TRUE, 2022), -- Unchanged
('Player B', 'Good', TRUE, 2022), -- Changed
('Player C', 'Bad', FALSE, 2022); -- New
```

## Incremental Processing

We use **CTEs (Common Table Expressions)** to process the data incrementally. Below is the SQL code that implements the incremental processing logic:

```sql
WITH last_season_scd AS (
    SELECT * FROM players_scd WHERE current_season = 2021 AND end_season = 2021
),
this_season_data AS (
    SELECT * FROM players WHERE current_season = 2022
),
unchanged_records AS (
    SELECT ts.player_name, ts.scoring_class, ts.is_active, ls.start_season, ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls ON ts.player_name = ls.player_name AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT ts.player_name, unnest(ARRAY[
        ROW(ls.scoring_class, ls.is_active, ls.start_season, ls.end_season),
        ROW(ts.scoring_class, ts.is_active, ts.current_season, ts.current_season)
    ]) AS records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ts.scoring_class <> ls.scoring_class OR ts.is_active <> ls.is_active
),
unnested_changed_records AS (
    SELECT player_name, (records::scd_type).scoring_class, (records::scd_type).is_active, (records::scd_type).start_season, (records::scd_type).end_season
    FROM changed_records
),
new_records AS (
    SELECT ts.player_name, ts.scoring_class, ts.is_active, ts.current_season AS start_season, ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT * FROM last_season_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
```

## Performance Optimization

This approach reduces the volume of data processing by **20x**:

- **Processes only records from the last two seasons (2021 and 2022).**
- **Excludes historical data** (e.g., records from 2002–2020).
- **Focuses only on relevant records** (unchanged, changed, and new).


## Results and Insights

### 20x Performance Improvement

By limiting data processing to two seasons and excluding irrelevant records, we achieved:

- A **reduction in processed data volume** from 20 seasons to just 2 seasons.

### Data Categorization:
1. **Unchanged Records**: Players with no changes between seasons.
2. **Changed Records**: Players with updates to scoring class or active status.
3. **New Records**: Players appearing for the first time in the dataset.

---

## Conclusion

This incremental SCD implementation demonstrates:

- **Efficient data processing** by focusing on relevant records.
- **Scalable and practical methods** for managing large datasets.
- A **real-world approach** to minimizing resource usage while improving query performance.

