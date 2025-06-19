# Day 2 - Lecture

# Is it a fact or a dimension

How do we know the way to really differentiate these things?

When Zach was working in Growth at Facebook, he was often dealing with two similar fields: `dim_is_active` and `dim_is_activated`. Both of them are “dimensions” on a user object, but with different meanings.

- `dim_is_active` was based on whether the user had any activity for at least a minute or engaged in the app in some way.
- `dim_is_activated` this was related to whether the user had deactivated or not their Facebook account (e.g. to take a break from social media).

You can see however that `dim_is_active` is a dimension is based on an **event** (liking, sharing, doing something), so is it really a dimension at that point? Or is it just an aggregation of facts?

*Zach says: it’s both. This is where modeling can get dicey.*

Compare it with `dim_is_activated`. In that case, you just go to your settings and click “deactivate account”, and that flags it and that’s all. It’s not an aggregation of facts. This is a proper dimension, an attribute on a user object.

Something to think about if you’re creating a dimension over an aggregation of fact is “What is the cardinality of that dimension?”. A lot of times, you want to bucketize it, to reduce the total possible values, and make your `GROUP BYs` simpler.

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

When you’re doing a GROUP BY, the implicit assumption is that you’re forming **GROUPS.** If you have loads of groups of very few items, worst case, 1 item, it makes no sense.

</aside>

**You can aggregate facts and turn them into dimensions**

- Is user a “high engager” or a “low engager”?
  - Think `scoring_class` from Week 1
  - `CASE WHEN` to bucketize aggregated facts can be very useful to reduce the cardinality

Nice rule of thumb: bucketize between 5 and 10 max values. (your buckets **HAVE** to make sense tho).

Anyway, the TL;DR is that facts and events are not so clear cut, and more blurry, because for instance, in the case of `dim_is_activated` the fact of you clicking that button makes the dimension. So in this case, clicking the button is both a fact and a dimension too. [Idk it’s peculiar — Ed.].

## Properties of facts vs dimensions

- Dimensions
  - Usually show up in `GROUP BY` when doing analytics
  - Can be “high cardinality” or “low cardinality” (e.g. user_id → high, country → mid, gender → low)
  - Generally come from a **snapshot of state** → Zach says: at Netflix, Facebook, Airbnb, there’s a prod DB, and they snapshot it at any given time, and whatever those values are in that moment are the values for the day
- Facts
  - Usually aggregated when doing analytics by things like `SUM, AVG, COUNT`
  - Almost always higher volume than dimensions, although some fact sources are low-volume, think “rare events”
  - Generally come from events and logs

Keep in mind you can aggregate facts and turn them into dimensions. Facts can also **change** dimensions (e.g. think about change data capture, CDC).

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

**CDC** is a perfect example of the blurry line between facts and dims. In CDC, you model a state change of a dimension as a fact, and you can recreate a dimension at any time based on the stack of changes that have happened.

</aside>

## Airbnb example

In Airbnb, Zach was working in Pricing and Availability.

On a specific night, an Airbnb listing has a specific price. That price, is it a fact or a dimension?

It might seem like a fact, because you can `SUM` it or `AVG` it, also it’s a `DOUBLE` so have extremely high cardinality, but it kind of is a dimension, as it’s the **attribute of the night.**

The fact, in this case, would be the host changing the setting that impacts the price. A fact has to be **logged**, a dimension comes from the **state of things**.

Price is actually derived from all of the settings that the host has set, and those setting are **state**, so price is a dimension.

# Boolean / existence-based fact/dimensions

Let’s talk a bit more about dimensions that are based on facts.

- `dim_is_active` or `dim_bought_something`, etc…
  - These are usually on the daily/hour grain too
- `dim_has_ever_booked`, `dim_ever_active`, `dim_ever_labeled_fake`
  - These “ever” dimensions look to see if there has “ever” been a log, and once it flips one way, it never goes back.
  - Interesting, simple and powerful features for machine learning → An Airbnb host with active listings who has **NEVER** been booked looks sketchier and sketchier over time
- “Days since” dimensions (e.g. `days_since_last_active`, `days_since_signup`, etc…)
  - Very common in Retention analytical patterns
  - Look up *“J curves”* for more details on this

# Categorical fact/dimensions

- Scoring class in week 1 (”star”, “good”, “avg”… based on points)
  - A dimension that is derived from fact data
- Often calculated with `CASE WHEN` logic and “bucketizing”
  - Example: Airbnb Superhost

Often, these conditional columns are not bucketized based just on one single columns, but a combination of them, based on some sort of meaningful criteria.

Once done however, these criteria can be very hard to change, because usually they become important business definitions, and therefore changing them would cause problems.

Metaphorical example: in facebook, the hard limit for friends is 5k. If they increase it, then they can never go back.

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

In other words, once you have determined the definition of a dimension, changing that, especially if it’s used throughout the company, can be very painful and take a lot of time.

**Corollary**: when you’re thinking of these definitions, get as many people involved and really think about the business!

</aside>

# Should you use dimensions or facts to analyze users?

- Is the `dim_is_activated` state or `dim_is_active` logs a better metric?
  - It depends!
- It’s like the difference between “signups” and “growth” in some perspectives

Intuitively, “active users” sounds more important in this example, but both can be. For instance, you can calculate the ratio of active users over activate users, which is another great metric to look at.

In general, it depends on the question you’re trying to answer.

# The extremely efficient date list data structure

- Read Max Sung’s writeup on this: <https://www.linkedin.com/pulse/datelist-int-efficient-data-structure-user-growth-max-sung/>
- Very efficient way to manage user growth

One of the most common questions at Facebook was: how many monthly / weekly / daily active users we have? At that scale, a `GROUP BY` over 30 days is quite expensive. Also, redundant when done every day, cause out of 30 days, each day, only one changes, the other stay exactly the same.

So the very naive approach is to process 30 days of data and do a `GROUP BY`. Not too smart. Also with over 2 billion users, where each user generates 50 rows a day in the fact table, so it’s 100 billion rows a day, and over a month that’s 3 trillion, imagine doing a group by on that.

- Imagine a cumulative table design like `users_cumulated`
  - user_id
  - date
  - dates_active - an array of all the recent days that a user was active

👆 This is the **naive** approach. But it kinda sucks because you have this big array of dates that are not really needed, as you don’t need the date but just the offset.

- So, you can turn that into a structure like this
  - user_id, date, datelist_int
  - 32, 2023-01-01, 1000000010000001 → A binary that identifies when the user was active w.r.t `date`, so in this case the user was active Jan 1st, then 25th December of the previous year, then again Dec 17th.
  - The 1s in the integer represent the activity for `date - bit_position (zero indexed)`
