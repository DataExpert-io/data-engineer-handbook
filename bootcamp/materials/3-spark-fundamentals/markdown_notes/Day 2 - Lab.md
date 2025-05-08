# Day 2 - Lab

In this lab we will use Scala, and take a look at the **Dataset API.**

Open the notebook `DatasetApi.ipynb`.

Take a look at the 1st cell

```scala
import org.apache.spark.sql.SparkSession

val sparkSession = SparkSession.builder.appName("Juptyer").getOrCreate()

case class Event (
   //Option is a way to handle NULL more gracefully.
   // In other words, it means it's nullable.
    user_id: Option[Integer],
    device_id: Option[Integer],
    referrer: Option[String],
    host: String,
    url: String,
    event_time: String
)
```

Just by using this `case class`, we can guarantee that `host`, `url` and `event_time` are never null, otherwise the pipeline will fail. This, again, is NOT AVAILABLE in SparkSQL or DataFrame API.

What we’re gonna do in this lab is take a bunch of tables and join them together (**event + device → eventWithDeviceInfo**).

A little more below, you will see this piece of code

```scala
val events: Dataset[Event] = sparkSession.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/events.csv")
                        .as[Event] // notice this last line
```

The idea here is we read a CSV,  and `as[Event]` describes that the `events` is now a **Dataset** of **Event**

The same is true for the other Dataset in this lab (Device).

What this does is it gives you the ability to work with this data in Scala directly. See these lines

```scala
val filteredViaDataset = events.filter(event => event.user_id.isDefined && event.device_id.isDefined)
val filteredViaDataFrame = events.toDF().where($"user_id".isNotNull && $"device_id".isNotNull)
val filteredViaSparkSql = sparkSession.sql("SELECT * FROM events WHERE user_id IS NOT NULL AND device_id IS NOT NULL")
```

They do exactly the same thing, but:

- In the 1st one, we’re working with the Dataset directly.
- In the second, we’re using the DataFrame API.
- The third uses SparkSQL.

As you can see, the Dataset API is quite convenient.

Let’s now do a join, starting with SQL

```scala
//Creating temp views is a good strategy if you're leveraging SparkSQL
filteredViaSparkSql.createOrReplaceTempView("filtered_events")
val combinedViaSparkSQL = spark.sql(f"""
    SELECT
        fe.user_id,
        d.device_id,
        d.browser_type,
        d.os_type,
        d.device_type,
        fe. referrer,
        fe.host,
        fe.url,
        fe.event_time
    FROM filtered_events fe
    JOIN devices d ON fe.device_id = d.device_id
""")
```

The we follow with DataFrame

```scala
// DataFrames give up some of the intellisense because you no longer have static typing
val combinedViaDataFrames = filteredViaDataFrame.as("e")
            //Make sure to use triple equals when using data frames
            .join(devices.as("d"), $"e.device_id" === $"d.device_id", "inner")
            .select(
              $"e.user_id",
              $"d.device_id",
              $"d.browser_type",
              $"d.os_type",
              $"d.device_type",
              $"e.referrer",
              $"e.host",
              $"e.url",
              $"e.event_time"
            )
// the `$` is the equivalent of `col('e.user_id')`.
```

Last one is with the Dataset API

```scala
// This will fail if user_id is None, which is why we have to manage nulls
// and use this syntax: `user_id.get`
// Alternative you can write `getOrElse(..), which is like COALESCE
// Here in specific we can use only .get
// because we're filtering out null ids in a previous step
val combinedViaDatasets = filteredViaDataset
    .joinWith(devices, events("device_id") === devices("device_id"), "inner")
    .map{ case (event: Event, device: Device) => EventWithDeviceInfo(
                  user_id=event.user_id.get,
                  device_id=device.device_id,
                  browser_type=device.browser_type,
                  os_type=device.os_type,
                  device_type=device.device_type,
                  referrer=event.referrer.getOrElse("unknow"),
                  host=event.host,
                  url=event.url,
                  event_time=event.event_time
              ) }
```

What’s nice about this last join is you get access to the left and right side of the join, their schemas and type, and it’s really easy to map everything to the new schema.

Another thing about Dataset API is that it’s really nice with UDFs. Imagine we create a function like `toUpperCase` (it already exists, but it’s just for simplicity of the example).

```scala
def toUpperCase(s: String): String (
  return s.toUpperCase()
)

// What you can do with this, with the Dataset API, you can call `.map` again, and do

.map( case (row: EventWithDeviceInfo) => {
  row.browser_type = toUpperCase(row.browser_type)
  return row
})
```

W.r.t. the DataFrame API, this example above is much simpler. Let’s see the difference

```scala
val toUpperCaseUdf = udf(toUpperCase _ )

// then in the schema definition
// [..]
toUpperCaseUdf($"d.browser_type").as("browser_type")
```

Another nice thing from these APIs is that you can create dummy data very easily, e.g.

```scala

val dummyData = List(
        Event(user_id=Some(1), device_id=Some(2), referrer=Some("linkedin"), host="eczachly.com", url="/signup", event_time="2023-01-01"),
        Event(user_id=Some(3), device_id=Some(7), referrer=Some("twitter"), host="eczachly.com", url="/signup", event_time="2023-01-01")
    )
```

---

Now open the `Caching.ipynb` notebook and run the first cell.

We have two datasets here, `users` and `devices`, in a many-to-many relationship. What we’re doing here is create both sides → A users and all of their devices, and a device and all of its users.

We’re gonna start with this

```scala
val eventsAggregated = spark.sql(f"""
  SELECT user_id,
          device_id,
        COUNT(1) as event_counts,
        COLLECT_LIST(DISTINCT host) as host_array
  FROM events
  GROUP BY 1,2
""")
```

The pipeline is quite trivial, it’s to showcase the re-use of the Dataframe. In fact it’s later used in two different spots:

```scala
val usersAndDevices = users
  .join(eventsAggregated, eventsAggregated("user_id") === users("user_id"))
  .groupBy(users("user_id"))
  .agg(
    users("user_id"),
    max(eventsAggregated("event_counts")).as("total_hits"),
    collect_list(eventsAggregated("device_id")).as("devices")
  )

val devicesOnEvents = devices
      .join(eventsAggregated, devices("device_id") === eventsAggregated("device_id"))
      .groupBy(devices("device_id"), devices("device_type"))
      .agg(
        devices("device_id"),
        devices("device_type"),
         collect_list(eventsAggregated("user_id")).as("users")
      )
```

Now run `eventsAggregated.unpersist()`, then remove the `.cache()` call and rerun the first cell. Copy the first query plan, then run it again but this time with `.cache()` for `eventsAggregated`.

Now go to [Diffchecker](https://www.diffchecker.com/), and paste one query plan in one box and the other query plan in the other box, then check the differences.

You will see that most changes are just about ids and numbers, but one in specific is `InMemoryTableScan`, which is the indication that the table was read from memory, i.e. from the cache.

> Keep in mind that cache is only going to be useful if you’re going to **reuse** the cached data, and not just use it once!
>

Remember there’s several ways to cache:

- Memory only → `StorageLevel.MEMORY_ONLY`
- Disk → `StorageLevel.DISK_ONLY`
  - This one however doesn’t make make a lot of sense. If you want to persist data to disk, it’s much better to just `saveAsTable('tableName')`, which can be easily queried separately.
- Both → `StorageLevel.MEMORY_AND_DISK` or simply `.cache()`

---

<aside>
<img src="https://www.notion.so/icons/warning_yellow.svg" alt="https://www.notion.so/icons/warning_yellow.svg" width="40px" />

**WARNING:** due to some iceberg shenanigans with bucketing etc… the 1st table (”matches”) has not been partitioned by `completion_date` explicitly, like Zach does in the video.

That’s in order to properly show bucket join behavior.

Read more:

- <https://www.guptaakashdeep.com/storage-partition-join-in-apache-spark-why-how-and-where/>
- <https://discord.com/channels/1106357930443407391/1313962674333159475>

</aside>

Now let’s open the last notebook, `bucket-joins-in-iceberg.ipynb`.

We have 2 datasets, `matches` and `matchDetails`, which can be joined on `match_id` .

Let’s start by creating the tables, by running only this code

```scala
import org.apache.spark.sql.functions.{broadcast, split, lit}

val matchesBucketed = spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/matches.csv")
val matchDetailsBucketed =  spark.read.option("header", "true")
                        .option("inferSchema", "true")
                        .csv("/home/iceberg/data/match_details.csv")

spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
val bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
     match_id STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDDL)

val bucketedDetailsDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketedDetailsDDL)
```

You can see that in the table DDL, we specify the partition schema (`PARTITIONED BY…`). But how you write out the data matters too. Notice the `bucketBy(16, "match_id")`.

```scala
matchesBucketed.select(
  $"match_id", $"is_team_game", $"playlist_id", $"completion_date"
  )
  .write.mode("append")
  .bucketBy(16, "match_id")
  .saveAsTable("bootcamp.matches_bucketed")

matchDetailsBucketed.select(
    $"match_id", $"player_gamertag", $"player_total_kills", $"player_total_deaths")
    .write.mode("append")
    .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")
```

After this one runs, if you run

```scala
spark.sql("select * from bootcamp.match_details_bucketed.files").count()
```

You will see that it amounts to 16, exactly our number of required buckets.

Finally, you can run this

```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

matchesBucketed.createOrReplaceTempView("matches")
matchDetailsBucketed.createOrReplaceTempView("match_details")

spark.sql("""
    SELECT * FROM bootcamp.match_details_bucketed mdb JOIN bootcamp.matches_bucketed md
    ON mdb.match_id = md.match_id
""").explain()

spark.sql("""
    SELECT * FROM match_details mdb JOIN matches md ON mdb.match_id = md.match_id
""").explain()
```

You will notice that the plan for the 1st query, that uses bucketed tables, **does NOT have EXCHANGE** steps. In other words, it’s not performing a shuffle!
