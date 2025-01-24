# WAP (Write Audit Publish) Pattern for Data Quality Management


```mermaid
mindmap
  root((Data Quality))
    Data Contracts
      WAP Pattern
        Write to staging
        Audit with quality checks
        Publish to production
        Pros
            No bad data in production
            Better for ad-hoc queries
            Intuitive for downstream
        Cons
            Slower due to data movement
      Signal Table Pattern
        Write directly to production
        Run quality checks
        Update signal table
        Pros
            Faster execution
            Less compute/IO
        Cons
            Risk of bad data queries
            Less intuitive
    Causes of Bad Data
        Logging Errors
            Schema changes
            Button click issues
            Software bugs
        Snapshotting Errors
            Missing dimensions
            Missing users
            Production data issues
        Pipeline Mistakes
            Join errors
            Case statement issues
            Non-idempotent pipelines
        Third-party API Changes
            Contract changes
            Schema modifications
    Validation Best Practices
        Backfill small amount first
        Check assumptions
            Nulls
            Duplicates
            Enumerations
            Time series
            Row counts
        Have others validate
    Quality Checks
        Blocking
            Serious issues
            Stops pipeline
            Requires troubleshooting
        Non-blocking
            Data weirdness
            Continues pipeline
            Sends alerts
    Data Propagation
        Consequences
            Affects downstream pipelines
            Expensive to fix
            Requires backfilling
            Can impact thousands of tables
        Risk Factors
            Critical datasets
            Heavily used tables
            Deep dependency chains
    Metric Definition
        Avoid over-filtering
        Keep metrics broad
        Limit dimensional cuts
        Watch for noise in narrow definitions
```


**Why it matters**: Poor data quality can destroy trust, waste time, and cost companies millions in cleanup efforts. The WAP pattern prevents 80-90% of data quality issues before they hit production.

**The big picture**: Two main approaches exist for ensuring data quality:

1. **WAP Pattern** (used by Netflix, Airbnb)
   - Write data to staging
   - Audit with quality checks
   - Publish to production if checks pass
   - Preferred for ad-hoc queries and data consistency

2. **Signal Table Pattern** (used by Facebook)
   - Write directly to production
   - Run quality checks
   - Update signal table to mark data as ready
   - Faster but riskier for ad-hoc queries

**Key causes of bad data**:
- Logging errors and schema changes
- Snapshotting issues
- Production data quality problems
- Pipeline mistakes
- Insufficient validation
- Third-party API changes

**Best practices**:
- Have someone else validate your assumptions
- Start with small backfills (e.g., one month)
- Implement both blocking and non-blocking quality checks
- Keep metrics broad enough to avoid noise
- Be extra careful with heavily-used datasets

**Real-world impact**: At Facebook, one broken contract with their Dim All Users table affected 20,000 downstream pipelines, costing nearly $1 million to fix.