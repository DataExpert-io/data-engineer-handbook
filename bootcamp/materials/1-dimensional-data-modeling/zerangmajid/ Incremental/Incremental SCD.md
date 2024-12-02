
This project demonstrates an efficient strategy for managing slowly changing dimensions (SCDs)

Key Features
Efficient Data Processing: This project processes only the relevant data from the last two seasons, reducing the load on the database by approximately 20 times compared to processing the entire dataset.
Focus on Changes: Only processes:
Stable records that have not changed (unchanged_records).
Changed records that have updated attributes (changed_records).
New records from the current season (new_records).


Results and Insights
Summary
This query processes only the records related to the 2021 and 2022 seasons, drastically reducing the volume of data processed by approximately 20 times compared to analyzing the entire dataset.

Key Advantages:

Limits data to the last two seasons.
Excludes historical, non-relevant records.
Focuses on only the relevant data types: unchanged, changed, and new records.
Details:
1. Limiting Data to 2021 and 2022 Seasons:
If the table contains data for players spanning 20 past seasons (e.g., 2002 to 2022), this query restricts processing to just the last two seasons (2021 and 2022).
Comparison:

Complete Dataset: Includes all player information across 20 seasons.
Optimized Query: Focuses only on the last two seasons, reducing the dataset size by 20 times.
2. Processing Only Key Records:
The query evaluates three record types:
Stable Players (unchanged_records): Players whose attributes (e.g., active status, scoring class) have not changed between 2021 and 2022.
Updated Players (changed_records): Players whose attributes (e.g., active status, scoring class) have changed.
New Players (new_records): Players appearing for the first time in the 2022 season.
3. Excluding Historical Records:
For players with historical data from 2002 to 2020 (pre-2021 data), this query excludes these records entirely.
This ensures that only the last two seasons are analyzed.
