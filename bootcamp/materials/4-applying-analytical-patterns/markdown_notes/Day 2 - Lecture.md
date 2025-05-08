# Day 2 - Lecture

# Intro

In this lecture we will talk about data engineering design patterns used at Meta.

Sometimes, you’ll see things in DE SQL interviews that you will almost never do on the job, like:

- Rewrite the query without window functions
- Write a query that leverages recursive CTEs
- Using correlated subqueries in any capacity

The long story short is that what you get asked in SQL interviews vs what you will have to do on the job often can different, and the job has a much more pragmatic approach.

However, there are some things about DE interviews that are right to ask:

- Care about the number of table scans
  - `COUNT(CASE WHEN)` is a very powerful combo for interviews and on the job
  - Cumulative table design minimizes table scans
- Write clean SQL code
  - CTEs are your friend
  - Use aliases

> Also, if you understand how ASTs are generated with SQL, the odds of writing bad performing queries kind of go away.
>

# Advanced SQL techniques to try out

- GROUPING SETS / GROUP BY CUBE / GROUP BY ROLLUP
  - This is essentially a way to do multiple aggregations in one query without having to do nasty unions. E.g. you can GROUP BY `gender` AND `country`, but then just `gender` and then just `country`, and then also `overall`.
- Self-joins
  - In the table, we will use self-joins to create a funnel
- Window functions
  - Lag, Lead, Rows clause
  - Can calculate stuff like rolling averages and stuff
- CROSS JOIN UNNEST / LATERAL VIEW EXPLODE
  - `UNNEST` is how you can turn an array column back into rows, essentially it explodes the array
  - UNNEST is same as LATERAL VIEW EXPLODE, it depends on the query engine

## Grouping sets

```sql
FROM events_augmented
GROUP BY GROUPING SETS (
 (os_type, device_type, browser_type),
 (os_type, device_type),
 (os_type),
 (browser_type)
)
```

Grouping sets are the most complicated. This is like doing 4 queries / aggregations at once.

The way you would do this without grouping sets, you’d need to copy the query 4 times, and then need to put dummy stuff in the values not being grouped, and then UNION ALL everything.

With `GROUPING SETS` you gain both in performance and readability.

One thing that’s important to do when using grouping sets, is to **make sure** that these columns are **never NULL.** Thats because they get already “nullified” when they’re excluded from the group bys, and if you already have nulls, you don’t know which is which.

> A best practice here is, before doing any of these grouping patterns, you want to **COALESCE** all the grouping dimensions to things like **“unknown”**.
>

## Cube

```sql
FROM events_augmented
GROUP BY CUBE(os_type, device_type, browser_type)
```

What `CUBE` does is it gives you all possible permutations here. In case of 3, it’s total of 8 possible combinations: with 3 cols, with 2 cols, with 1 col and with 0 cols.

Don’t use cube with more than 3-4 total dimensions because that explodes into so many different combinations.

Also, another problem with CUBE is that it does too much. It can even give you combinations that you don’t care about, and waste compute time on them.

## Rollup

```sql
FROM events_augmented
GROUP BY ROLLUP(os_type, device_type, browser_type)
```

You use `ROLLUP` for hierarchical data (imagine like country, state, then city).

In `ROLLUP`, the number of dimensions is equal to the number of aggregations you get. In the country example, you’d get a group by country, then country and state, then country and state and city.

# Window functions

- Very important part of analytics.

The function is composed of two pieces:

- The function (usually RANK, SUM, AVG, DENSE_RANK, …) - *[Zach’s comment on `RANK`, never use it because it skips values. In most cases he either uses `DENSE_RANK` or `ROW_NUMBER`. — Ed.]*
- The window
  - PARTITION BY → How you cut up your window (basically like GROUP BY)
  - ORDER BY → This is how you sort the window (great for ranking or rolling sums etc…)
  - ROWS → Determines how big the window can be (can be all rows, or less)
    - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` → this is the default.
    - Window functions basically always look back.

# Data modeling vs advanced SQL

If your data analysts need to do SQL gymnastics to solve their analytics problems, you’re doing a bad job as a data engineer. Obviously this is not ALWAYS the case, sometimes they will do crazy queries because that’s what’s necessary on that specific moment.

The point is for you to remove as much complexity as you can.

Don’t make the assumption that your analysts are as proficient with SQL as you are. Don’t give them garbage!

Obviously, focusing on data modeling and data quality makes some of these problems disappear!

Understanding what the analysts are doing, querying and presenting, just to understand where the bottlenecks are and how to remove them and speed up their processes.

When you make analysts faster as a DE, you’re doing your job.

## Symptoms of bad data modeling

- Slow dashboards
  - This is where grouping sets can be really useful
  - If you’re using row or daily lvl data in your dashboards, without pre-aggregating, eventually it’s gonna get slow if your company scales to a big enough level → Your dashboard is not gonna work
  - With pre-aggregation instead, it will be infinitely scalable
  - What Zach did at Facebook, since providing row lvl data to Tableau would be impossible, was to pre-aggregate data to the dimensions that people cared about (e.g. country, device, app, etc…) → going from billions of rows to few rows.
- Queries with a weird number of CTEs
  - If there’s 10 CTEs in the queries, especially if you see the same queries over and over, then you probably want to implement a staging step. If it’s always the same 10 CTEs, probably the first 5s should be materialized somewhere in a table.
  - Storage is cheaper than compute, remember this about data modeling (and you get time back by storing things).
- Lots of CASE WHEN statements in the analytics queries
  - This means that you data model is not robust enough, or not conformed enough, i.e. you’re not conforming the values to what they need to be.
