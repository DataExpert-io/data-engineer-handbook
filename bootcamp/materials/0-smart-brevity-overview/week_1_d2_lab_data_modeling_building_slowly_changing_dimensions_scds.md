# Building Slowly Changing Dimensions (SCD) in Data Modeling

*A technical deep dive into implementing SCD Type 2 for tracking historical changes in dimensional data using PostgreSQL.*

```mermaid
mindmap
  root((Data Modeling &
    SCDs Intro Lab))
    Table Structure
        Primary Key
            Player Name
            Start Season
        Tracking Columns
            Scoring Class
            Is Active
        Time Columns
            Start Season
            End Season
            Current Season
    Full History Approach
        Window Functions
            LAG for Previous Values
            Partition by Player Name
            Order by Season
        Change Detection
            Compare Current vs Previous
            Generate Change Indicators
        Streak Identification
            Sum Change Indicators
            Group Records by Streaks
        Advantages
            Simple to Understand
            Good for Smaller Dimensions
        Disadvantages
            Memory Intensive
            Processes All History
            Multiple Window Functions
    Incremental Approach
        Components
            Historical SCD
            Last Season SCD
            This Season Data
        Record Types
            Unchanged Records
                Extend End Date
            Changed Records
                Close Old Record
                Create New Record
            New Records
                Create First Record
        Advantages
            Less Data Processing
            More Efficient
            Better for Large Dimensions
        Disadvantages
            More Complex Logic
            Sequential Dependencies
            Harder to Backfill
    Best Practices
        Handle Null Values
        Check Data Quality
        Consider Data Volume
        Document Assumptions
        Test Edge Cases
```

**Big picture:** Two approaches demonstrated for tracking dimensional changes over time - a full historical rebuild and an incremental update method.

**Key components of SCD Type 2:**
- Start and end dates for each dimension record
- Support for tracking multiple changing attributes
- Maintains complete historical record of changes
- Primary key based on entity name and start date

**Full historical rebuild approach:**
- Uses window functions to detect changes
- Generates streak identifiers for tracking changes
- More memory-intensive but simpler to implement
- Works well for smaller dimensional tables (millions of records)
- Requires scanning all historical data

**Incremental update method:**
- Processes only changed records and new data
- More complex query logic but better performance
- Handles three scenarios:
  - Unchanged records (extend end date)
  - Changed records (close old record, create new)
  - New records (create initial record)
- Better for larger datasets but requires sequential processing

**Bottom line:** Choice between approaches depends on data volume and processing requirements. Full rebuild is simpler but less efficient; incremental update is more complex but better performing at scale.

**Watch out for:** Null handling in dimensional attributes can break comparison logic. Always validate assumptions about data quality when implementing either approach.