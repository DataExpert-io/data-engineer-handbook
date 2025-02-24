# Day 2 - Lecture

# Intro

In this lecture we will talk about

- difference between Spark Server vs Spark Notebook
- PySpark and Scala Spark usage scenarios
- Implications of using UDFs
- PySpark UDF vs Scala Spark UDF
- DataFrame, SparkSQL, RDD, DataSet APIs

This lecture is a bit all over the place, miscellaneous, so it doesn’t have a lot of linearity.

# Spark Server vs Spark Notebooks

- Spark Server (how Airbnb does it) → Where you have to submit your Spark jobs via CLI
  - Every run is fresh, things get uncached automatically
  - Nice for testing
- Notebook (how Netflix does it) → You have just 1 Spark session that stays live and you have to terminate it later.
  - Make sure to call `.unpersist()`

## Databricks considerations

- Should be connected with Github
  - PR review process for EVERY change
  - CI/CD check

The problem with DBX is that any change you make on a notebook gets immediately picked up, so you can potentially fuck up production with bad data in the bat of an eye. Notebooks can just be changed if they’re not checked in and part of a VCS and CI/CD pipeline, and this is dangerous.

# Caching and temporary views

**Temporary views**

- Is kinda like a CTE
- The problem with temp views is that if you use it multiple times downstream, they’re gonna get recomputed every time, UNLESS CACHED (`.cache()`).

**Caching**

- Storage levels
  - MEMORY_ONLY → Really fast
  - DISK_ONLY
    ”Materialized view” and caching to disk are more or less the same thing. It just means writing it out.
  - MEMORY_AND_DISK (the default)
- Caching really only is good if it fits into memory. *[Zach doesn’t really recommend caching to disk — Ed.]*
  - Otherwise there’s probably a staging table in your pipeline you should add!
- In notebooks
  - Call `.unpersist()` when you’re done otherwise the cached data will just hang out!

## Caching vs Broadcast

- Caching
  - Stores pre-computed values for re-use
  - Stays partitioned
- Broadcast Join
  - Small data that gets cached and shipped in entirety to each executor (not partitioned anymore) — A couple of GBs is prob the maximum to work with in this one.

In other words, the difference is that in caching, each executor is only going to place in memory their fraction of the data.

Example: say the data is 100GBs, but you have 200 partitions, then you can cache it, because each executor is gonna have 4 tasks, each task is gonna get 500MBs so 2GBs per executor, and that’s how you can cache this.

### Broadcast join optimization

Broadcast JOINs prevent shuffle. For the most part, it gets trigger automatically, when it can.

Like said before, the way it works is one side of the JOIN is “small”, it gets broadcasted to all executors.

This means, the dataset doesn’t get shuffled around, but instead shipped to all executors.

The setting `spark.sql.autoBroadcastJoinThreshold`, which defaults to **10MB,** is what determines the maximum data size for broadcast join to happen automatically.

You can crank this up even 200x, as long as you have enough memory. You can go up to some gigabytes, but definitely not 10s of gigabytes or more. Single digit gigabytes are still somewhat ok.

> You can explicitly wrap a dataset with `broadcast(df)` too
>
- This will trigger the broadcast regardless of the size of the DF. This is more deterministic than just hoping your little table size stays below the threshold you set, because when it goes a above, then you have to go and update your code.
- Doing this explicitly is probably recommended because other people reading your code will understand your intent.

# UDFs

Stands for User Defined Function. They allow you to do all sorts of complex logic and processing when you are in the DataFrame world. It’s quite powerful but there are some gotchas.

One problem is with Python UDFs:

- When Spark (running on the JVM) hits a Python UDFs, what it does is it serializes the data and then passes it to a Python process, which will run your code, return some value, serialize it back and send it back to Spark where it deserialize it again.
- There are a lot of steps, so as you can imagine, this makes PySpark UDFs a lot less performant in Spark.

> Howevear, Apache Arrow has made all these nasty SerDe steps a lot better, so Python UDFs now have become **mostly** in line with Scala Spark UDFs.
>

There are still some performance hits in Python when we’re considering UD**A**Fs, which are **A**ggregating functions.

So when people ask “should one use Scala Spark or PySpark”, this is about it:

- Scala Spark give better performing UDAFs, but it’s a very niche case.
- The **DataSet API** you get in Scala, which you don’t get in Python.

**Dataset API**

This allows you to do completely functional programming with your Scala Spark code, where you don’t have to do any DataFrame or declarative programming.

# DataFrame vs Dataset vs SparkSQL

Dataset is Scala only!

Zach thinks of this as a continuum, where on one side you have SparkSQL, and on the other you have Dataset.

**SparkSQL**

- Lowest barrier to entry
- Useful when you have many cooks in the kitchen (e.g. analysts, data scientists, etc…)
- Quickest to change
- For short-lived pipelines, that require fast and frequent iterations

**DataFrame**

- Modularize the code
- Put stuff into separate functions
- Make things more testable
- For pipelines that don’t need to be iterated as quickly, and for the long haul

**Dataset**

- The closest to software engineering
- Allows you to create fake mock data a lot more easily
- Handles NULL better
→ you have to model it (e.g. declare columns nullable), otherwise you get an exception when you encounter nulls. **DataFrame and SparkSQL can’t do this!**
- Best for pipelines that you need for the long haul and you’re already in Scala!

# Parquet

- Used by default by Iceberg
- Amazing file format
  - Run-length encoding allows for powerful compression
- Don’t use global `.sort()`
- Use `.sortWithinPartitions`
  - Parallelizable, gets you good distribution

# Spark tuning

- Executor memory
  - Don’t just set it to 16GBs and call it a day, it’s a waste
- Driver memory
  - Only needs to be bumped up if:
    - You’re calling `df.collect()`
    - Have a very complex job
- Shuffle partitions
  - Default is 200
  - **Aim for ~100-200 MBs per partition to get the right sized output datasets** (that is, assuming your data is uniform, which often won’t be)
  - But it’s not a rule fixed in stone, try a range of values to see which one performs best
- AQE (adaptive query execution)
  - Helps with skewed datasets, wasteful if the dataset isn’t skewed
  - Don’t just enabled it by default on the off-chance that your dataset is skewed
