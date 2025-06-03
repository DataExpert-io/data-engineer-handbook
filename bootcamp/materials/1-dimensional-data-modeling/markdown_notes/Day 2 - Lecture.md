# Day 2 - Lecture

# Intro

The topic is **slowly changing dimensions (SCD).**

A SCD is an attribute that can change over time, such as favorite food (e.g. some years it can be lasagna, and then later it can be curry chicken). Not all dimensions are slowly changing. Some never change, like your birthday, for instance.

SCDs need to be modeled properly. If they’re not, you risk hindering “idempotency” (a property of your data pipeline to always return the same result if processing the same data more than once).

# Idempotent pipelines are CRITICAL

*Idempotent → denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself.*

Terrible definition, so let’s review it.

**Pipelines should produce the same result (given the same exact inputs)**

- Regardless of the day you run it
- Regardless of how many times you run it
- Regardless of the hour that you run it

This is important because imagine having a pipeline that you run today, and then in a week you backfill it, you will end up with different data.

## Why is troubleshooting non-idempotent pipelines hard?

They fail silently. They don’t crash, but the end result is different every time. In other words, it’s non reproducible. You only notice when data inconsistencies show up (and your analyst yells at you).

### What can make a pipeline not idempotent

- `INSERT INTO` without `TRUNCATE` → *This creates duplicates!*
  - Better idea, never use `INSERT INTO`
  - Use `MERGE` or `INSERT OVERWRITE` every time instead
- Using `start_date >` without a corresponding `end_date <`
→ Imagine a pipeline with a clause like `WHERE date > yesterday`. If you run it today, you get one day of data. If you run it tomorrow, you get two days of data, and so on. Every time you run it you get one more day of data, and this is not idempotent.
  - Instead, you should be using a window of data, i.e. 1 day of data, 2 days of data, 1 week of data etc.
  - The **date range** should **not be unbounded**.
- Not using a full set of partition sensors
→ Your pipeline is going to run with an incomplete set of inputs, i.e. you aren’t checking for the full set of inputs that you need for your pipeline. So it runs but it runs too early, before all inputs are ready. This again creates inconsistencies.
- Not using `depends_on_past` (it’s an Airflow term) for cumulative pipelines. Another term is “sequential processing”.
→ Imagine you have a cumulative pipeline, so it depends on “*yesterday’s*” data: the pipeline cannot run in parallel. So it has to run like **“yesterday → today → tomorrow → …”**. Most pipelines aren’t like that, most pipelines can be backfilled and ran in parallel.
In cumulative pipelines, without sequential processing, it will make a mess as the data is not processed in the order it needs to be.

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

**Production** and **backfill** are the same, for idempotent pipelines

</aside>

- Relying on the “latest” partition of a not properly modeled SCD table.
  - *Example at Facebook*: a `users` table, where an account can be labeled “fake” or “not fake”, and this value can change over time depending on what the account does (e.g. starts as fake, then completes the challenge so is not fake, but then does sketchy thing so gets labeled as fake again etc…).
    There was this table `dim_all_fake_accounts` which was relying on “**latest**” data from `users` table instead of “**today’s**” data. This meant that whenever `dim_all_fake_accounts` would pull data from `users`, sometimes it would effectively pull “today’s” data, and sometimes `users` had not processed yet, so it would pull from the latest partition, which happened to be “yesterday’s”.
  - Cumulative table design AMPLIFIES this bug.

    This is a bit convoluted, more info on this in the lab.

### The pains of not having idempotent pipelines

- Backfill and production are not gonna create the same data, in other words, old and restated data are inconsistent.
- Very hard to troubleshoot bugs
- Unit testing cannot replicate the production behavior
  - Unit tests can still pass even if the pipeline is not idempotent
  - But if you write idempotent pipelines, unit tests become better because now they ensure the pipeline stays idempotent
- Silent failures

# Should you model as Slowly Changing Dimensions?

Remember that an SCD is a dimension that changes over time. E.g.: age, favorite food, phone brand, country of residence, etc…

Other dimensions don’t change, like birthday, eye color, etc…

There’s also a concept of rapidly changing dimensions (e.g. heart rate, which changes minute to minute).

- Max (Beauchemin), creator of Airflow, HATES SCD data modeling.
  - His whole point is that SCDs are inherently **not idempotent**
  - [Link](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) to Max’s article about why SCD’s suck
  - In short, the gist of it is that every day you have whatever the dimension value is. This creates a lot of duplicated data, but storage cost is so cheap that it’s better than sustaining the cost of fixing errors due to SCDs.
- Options for modeling SCDs
  - Latest snapshot [also known as SCD type 1 — Ed.]. This only takes in consideration the latest value of a dimension.
  - Daily snapshot — this is Max’s approach. Each day the dimensional data is snapshotted in full [and added to its own partition so as to isolate it from the other snapshots].
  - SCD. See [later paragraph](#types-of-scd) for explanation. The resulting table is very compressed with respect to the daily snapshot.
- The slower a dimension changes, the better results (in term of compression) one gets if modeling using SCD2 vs full snapshot.
→ Imagine this: age in years changes once a year, so a `dim_user` table with an `age` column would have a row per user per year. Very compressed w.r.t. a daily snapshot of all users. Conversely, if we’re considering `age_in_weeks` or even `age_in_days` [i know this is absurd, it’s for the sake of the example], then the compression would be much less because there would be many more rows in the `dim_user` table.

**Why do dimensions change?**

- Someone decides they hate iPhone and want Android now
- Someone migrates from team dog to team cat
- Someone migrates from USA to another country
- Etc…

## **How can you model dimensions that change?**

Like we saw above:

- Singular (latest) snapshots [AKA SCD type 1 — Ed.]→ **not idempotent! Never really do this.**
- Daily partitioned snapshots (Max’s strategy)
- SCD types 2, 3

### Types of SCD

- **Type 0**
  - Dimensions that don’t change (e.g. birth date)
- **Type 1**
  - You only care about the latest value.
  - **Don’t use this (in OLAP) because it’s not idempotent!**
- **Type 2**
  - You care about what the value was from `start_date` to `end_date`
  - Current values usually have either an `end_date` that is:
    - `NULL`
    - Far into the future like `9999-12-31`
  - Often has also an `is_current` boolean column
  - Hard to use
    - Since there’s more than 1 row per dimension, you need to be careful about filtering on time
  - **The only SCD that is purely** **idempotent**
- **Type 3**
  - You only care about “original” and “current”. Doesn’t hold on to all history. Just the first and the last.
  - Benefits
    - You only have 1 row per dimension
  - Drawbacks
    - You lose the history in between original and current
  - Is this idempotent? Partially, which means it’s not (if something changes more than once).

### Which types are idempotent

- Type 0 and Type 2 are idempotent
  - Type 0 because the values are unchanging
  - Type 2 is but need to be careful with using `start_date` and `end_date`
- Type 1 isn’t idempotent
  - If you backfill with this dataset, you’ll get the dimension as it is now, not as it was then!
- Type 3 isn’t idempotent
  - If you backfill with this dataset, it’s impossible to know when to pick “original” vs “current”

# SCD2 Loading

There’s two ways one can load these tables:

1. One giant query that crunches all daily data and crunches it down
    - Inefficient but nimble
    - 1 query and you’re done
2. Incrementally load the data after the previous SCD is generated
    - Has the same `depends_on_past` constraint
    - Efficient but cumbersome
    - Generally, you want your production run to be this one, but it’s not a rule of thumb, especially if the dataset is small

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

**Regarding efficiency**

There’s a tradeoff here and Zach makes a valid point:

“*As a data engineer, you don’t want to get caught up with the idea that every pipeline that you build has to be a Ferrari, and has to be perfect and the most efficient.*

*What that means is you’re wasting time on marginal value, when you could be looking at new, [more valuable] stuff.”*

</aside>
