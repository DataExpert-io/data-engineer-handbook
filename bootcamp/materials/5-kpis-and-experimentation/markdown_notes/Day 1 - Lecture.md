# Day 1 - Lecture

# Intro

Thinking like a product manager is critical for being a good data engineer. This looks like pipelines that actually impact the business:

- Building good metrics that change business decision making
- Build metrics that are impacted by experiments

In this lecture, we will look at metrics and Statsig.

# Why do metrics matter at all?

It really depends on the company and many other different things. In some companies, metrics matter less than in others. It depends on the culture of the company rather than the metrics themselves. E.g. difference between Airbnb and Facebook, Airbnb cared about metrics less than Facebook.

But metrics are important, they provide visibility, they explain the world and your business, especially as you get more and more of them. The more clarity and visibility you have, the fewer “icebergs” you’re gonna crash into.

---

Things we’ll cover today:

- Metrics play a huge part in data modeling
- Making sure metrics can’t be “gamed”
- The inner workings of experimentation
- The difference between feature gates and experimentation
- How do you plug metrics into experimentation frameworks?

# Complex metrics mean complex data models

When you’re building the spec (as we’ve seen in previous lectures), this concept you should keep in mind at all times.

If stakeholders are asking for something that’s more than simple aggregations, but instead some weird ultra specific metric, like average rolling percentile sum whatever, that’s more a data scientist job, most times you should push back on it. You should give them the raw aggregates, and let the analytics people figure it out afterwards.

Don’t let the data scientists dictate your data models.

# Types of metrics

- Aggregates / Counts
- Ratios
- Percentiles (p10, p50, p90 values)

Generally speaking, as a DE, you should be mostly supplying aggregates and counts. And what grain you supply those at is usually at the entity grain (daily metrics → user_id, metric_name, metric_value).

**Aggregates / counts**

The Swiss army knife and the most common type data engineers should work with.

**Ratios**

Ratio metrics are in general about indicating quality. Data engineers should supply numerators and denominators, **NOT THE RATIOS** between them

Examples:

- Conversion rate
- Purchase rate
- Cost to acquire a customer

Keep in mind that when you cut by any dimension (e.g. operative system), then the numbers might not add up with ratios (additive vs non additive properties).

**Percentile metrics**

Useful for measuring the experience at the extremes

Examples:

- P99 latency → e.g. how fast is our worst experience (top 1% latency) when a website loads?
- P10 engagement of active users

# Metrics can be “gamed” and need balance

- Experiments can move metrics up short-term but down long-term
  - E.g. Notifications at facebook:
    - Send more → get more users in the short term, in the long term you lose that lever, because what happens is that people turn of settings, and then you can’t access those users anymore
    - Create other metrics that can measure this stuff, like “reachability” (% of users who turned off settings).
    - In the case above, they would notice that spamming people with notifications would be detrimental
- Fiddle with the numerator or the denominator
  - You can really get your metrics / experiments to tell you whatever you want
  - Really have clear hypothesis when you start your experiments so you can test them
  - “P-hacking” → avoid it
- Novelty effects of experiments
  - When you introduce new things, people get excited about them, but then it fades out as the novelty of the new feature wears off
  - Be aware for how long the experiment should run
- Increased user retention at extreme cost
  - Netflix example: refreshing the feed in the background, when the app is not being used, increases retention
  - But at what cost? If it’s millions of $ in AWS costs, maybe the feature isn’t worth it
  - Make counter metrics → e.g. AWS cost for the experiment / we get this much increased retention at X cost.
  - Also, diminishing returns play a big role

# How does an experiment work?

- Make a hypothesis!
- Group assignment (think test vs control)
- Collect data
- Look at the differences between the groups

## Hypothesis testing

Where you have the “null hypothesis” and the alternative hypothesis.

- In the null hypothesis → there is no difference between **test** and **control**
- The alternative hypothesis → there is significant difference from the changes

Remember: you never prove the alternative hypothesis → Instead, you fail to reject the null hypothesis!

# Group testing

Who is eligible? How we assign group members?

- Are these users in a long-term holdout?
  - A long-term holdout is a long running experiment over a group of people (e.g. not being sent notifications), so these shouldn’t be considered for other experiments
- What percentage of users do we want to experiment on?
  - A lot of the time, your experiment groups are not 100% of your users. It can be some fraction of your users based on certain criteria.
  - In big tech you get the luxury to experiment on a small % of users that you don’t get in smaller companies, because you don’t have a lot of users to begin with, and so you don’t have a lot of statistical power with small % of users.

### Group assignment

![image.png](images/d1le_image.png)

Logged in-experiments are more powerful than logged-out ones, because you have a lot more information about those users.

Remember that you need to track your events during the experiments. They can happen either on client or on the server, depending where you do your logging. Statsig offers APIs for both.

A missing piece of this diagram:

- What about other metrics that aren’t on Statsig?
- You can have some kind of ETL process that dums this data on it.

## Collect data

You collect data until you get a statistically significant result. Generally speaking, at least a month is a good duration.

> The smaller the effect, the longer you’ll have to wait to get a statistically significant result
>

Keeping in mind some effects may be so small you’ll never get a statistically significant result!

Make sure to use stable identifiers (Statsig has it)

- Hash IP addresses to minimize the collection of PII
- Leverage Stable ID in Statsig

- Do not UNDERPOWER your experiments
  - The more test cells you have, the more data you will need to collect!
  - It’s unlikely that you have the same data collection as Google and can test 41 different shades of blue.
- Collect all the events and dimensions you want to measure differences, and make sure you’re logging them BEFORE you start the experiment.

## Look at the data

![image.png](images/d1le_image%201.png)

In Statsig, if you have a bar that overlaps 0, so you have both positive and negative results, then it doesn’t count as statistically significant.

In this specific experiment, only 1 bar is (the green one), however Zach choose a p-value of 0.2 (80% confidence interval).

- P values are a method we use to determine if a coincidence is actually a coincidence or not
  - If p-value < 0.05, then there’s a 95% chance that the effect you’re looking at is not due to coincidence, but some other factor.
  - The lower the p-value, the higher certainty you have that the effect is not due to randomness
- P < 0.05 is the industry standard. Although depending on your situations you might want higher or lower.

**Statistically significant results may still be worthless!**

- Maybe a result is significant but it’s a tiny delta
  - like a 1% lift if using a red button vs blue button
- Maybe there are multiple statistically significant results in opposite directions

### Gotchas when looking for statistical significance

Imagine you’re measuring notifications received, and in a group you have normal people, and in another you have Beyonce, if you look at the averages, the 2nd group will be much higher. You wanna be careful about extreme outliers.

- **Winsorization** helps with this (clip the outlier to a lesser value, like 99.9 percentile)
- Or looking at user counts instead of event counts

### Statsig can create metrics. How about adding your own?

You can add your own user-level metrics to Statsig via batch ETLs.

This is a common pattern in big tech for data engineers to own these.
