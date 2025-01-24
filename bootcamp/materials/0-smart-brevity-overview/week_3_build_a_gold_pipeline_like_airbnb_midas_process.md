# Building Gold-Standard Data Pipelines: The Airbnb Midas Process


```mermaid
mindmap
  root((Building High
    Quality Data
    Pipelines))
    (Documentation & Trust)
        [Good Documentation]
            (Spec Review Process)
                (Technical Review)
                (Stakeholder Review)
            (Flow Diagrams)
            (Schema Documentation)
            (Quality Checks)
            (Metric Definitions)
            (Example Queries)
        [Building Trust]
            (Stakeholder Involvement)
            (Clear Business Impact)
            (Consider Future Needs)
            (Validate with Partners)
    (Midas Process Steps)
        [1. Create Spec]
        [2. Spec Review]
        [3. Build & Backfill]
        [4. SQL Validation]
        [5. Manura Validation]
        [6. Data Review]
        [7. Code Review]
        [8. Metric Migration]
        [9. Launch PSA]
    (Data Quality)
        [Basic Checks]
            (Not Null)
            (No Duplicates)
            (Valid Enums)
            (Data Exists)
        [Intermediate Checks]
            (Week over Week Counts)
            (Seasonality)
            (Row Count Trends)
        [Advanced Checks]
            (Machine Learning)
            (Complex Relationships)
            (Seasonality Adjusted)
    (Schema Best Practices)
        [Naming Conventions]
            (fact_ prefix)
            (dim_ prefix)
            (scd_ prefix)
            (agg_ prefix)
        [Documentation]
            (Column Comments)
            (Table Comments)
            (Business Context)
        [Quality Standards]
            (Dimension Checks)
            (Fact Checks)
            (Relationship Validation)
    (Business Value)
        [Direct Revenue]
        [Cost Savings]
        [Strategic Decisions]
        [Leading Indicators]
    (Metrics)
        [Guardrail Metrics]
            (Critical Business KPIs)
            (Launch Blockers)
        [Non-Guardrail Metrics]
            (Informational)
            (Contextual)
```


**Why it matters**: High-quality data pipelines are crucial for building trust and driving business value. Airbnb's Midas process offers a comprehensive framework for creating reliable, long-lasting data pipelines.

**The big picture**: The Midas process consists of 9 key steps:
1. Create a spec
2. Get technical and stakeholder reviews
3. Build and backfill pipeline
4. SQL validation
5. Manura validation (metrics)
6. Data review
7. Code review
8. Migrate metrics
9. Launch PSA

**Key insights**:
* Good documentation upfront prevents painful backfills and builds stakeholder trust
* Not every pipeline needs the full Midas treatment - reserve it for critical, long-term data assets
* Strong specs include flow diagrams, schema definitions, quality checks, and example queries
* Different quality checks are needed for dimension vs. fact tables
* Use week-over-week rather than day-over-day comparisons for more reliable monitoring

**What to watch**: Quality checks should include:
* Basic checks (nulls, duplicates, enum values)
* Intermediate checks (row counts, week-over-week comparisons)
* Advanced checks (seasonality adjustments, machine learning)

**Bottom line**: While the full Midas process may seem heavy, even implementing a few steps can dramatically improve data quality and stakeholder trust. The upfront investment in documentation and validation pays off in reduced maintenance and stronger analytics partnerships.