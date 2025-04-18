# Day 3 - Lecture

# Intro

This lecture will talk about Unit testing and Integration testing for Spark pipelines.

# Where can you catch (data) quality bugs?

- In development (best case)
- In production, but not showing up in tables (still good)
  - Following the Write Audit Publish (WAP) pattern, your audit fails so you don’t publish to production
  - you want to minimize the amount of times that you’re doing this especially if you have a lot of checks that can give false positives
- In production, in production tables (terrible and destroys trust)
  - In this case, the quality error bypasses your checks and ends up in prod.
  - Usually a data analyst will scream at you
  - Sometimes they can go unnoticed

**How do you catch bugs in dev?**

Unit tests and integration tests of your pipelines.

In this case, a unit test can mean if you have UDFs, or other functions that perform very specific things, you want to write a test for each of those functions. **Especially and critically** if these tests call another library. So that if that library changes without you knowing, and creates a bug, your test will catch it and the issue can be fixed before it goes in prod.

**How do you catch bugs in production?**

Use the WAP pattern.

**What’s the worst situation?**

- Data analyst finds bugs in production and yells at you
- Ruins trust
- Ruins mood
- Nobody wins

Actually another bad situation can happen, which is the wrong data doesn’t get spotted immediately, and wrong decisions happen because of this data. This is the probably the worst case scenario ever.

# Software engineering has higher quality standards than data engineering

Why?

- Risks
  - If Facebook website goes down, does it lose more revenue from that, or from a data pipeline that goes down?
  - Frontend being non-responsive stops the business too
  - A lot of times in data it’s ok if things break even for a day or two
  - Consequences in SWE when things break is 1 or 2 orders of magnitude higher than in data engineering, and also more immediate
- Maturity
  - SWE is a more mature field
  - Test-driven development and behavior-driven development are new in data engineering
- Talent
  - Data engineers come from a much more diverse background than SWEs

# How will data engineering become riskier

Zach was working at Airbnb and they had this ML model that was called “smart pricing”; the thing is most hosts don’t pick their price, they let AI figure it out.

This algorithm was responsible for a very large chunk of Airbnb revenues. If this model is trained on data that is 1 or 2 days delayed, how much is lost due to this, for every day the data is delayed?

- Data delays impact machine learning
  - Every day that notification ML was behind resulted in a ~10% drop in effectiveness and click through rates
- Data quality bugs impact experimentation
→ If you have dq bugs that aren’t found for a sufficiently large period of time, people might get the wrong idea around their experiments, in that the bugs create consequences that you cannot rely upon, but you don’t know they exist, and so you end up trusting something you shouldn’t.

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

**As trust rises in data, risk rises too**

</aside>

When you build a pipeline, think what are the consequences of something breaking, and what you can do to mitigate them.

In other words, we have to level our game as the organizations start to rely more and more on data.

## Why do most organizations miss the mark on data engineering quality?

So many places Zach worked at, they don’t do quality right, at all.

- When Zach worked at Facebook, for the 1st 18 months he didn’t write a single DQ check.
- One reason is that they had this culture of “move fast and break things”, in order to iterate quickly, and so the quality check part was kind of a “nice-to-have”.
- Data analytics is about answering questions as quickly as possible, but doesn’t have the same culture of automated excellence
→ However, you can actually have both speed and automated excellence!
- “This chart looks weird” is enough for an alarming number of analytics organizations
- These are part of the reason Zach left Facebook.

Goes to show that even Facebook, that has some of the best engineers in the world, can and has missed the mark on data quality.

## Tradeoff between business velocity and sustainability

Business wants to answer questions as quickly as possible. “I need this answer yesterday — can you quickly pull this data for me?”.

And of course you know that quickly is not often feasible, so how do you decide, do you answer questions quickly, or do you avoid making tech debt, or do you stay in the middle?

- Business wants answers fast
- Engineers don’t want to die from a mountain of tech debt
- Who WINS?
- Depends on the strength of your engineering leaders and how much you push back!
- **Don’t cut corners to go faster!**

In Zach’s experience, strong engineering managers usually lean more heavily in **not creating tech debt**, by going a little bit slower on answering business questions. However, while working with weak leaders that don’t understand how to push back, then essentially “**analytics eats the data engineering lunch”** and you get burned out.

Data engineers build roads and answers, not answer questions. If you spend time only answering question, your road will be shitty, bumpy, and filled with dirt. You want to take the time to build the highway! Obviously it takes longer, but it reaps more benefits in the long run!

<aside>
<img src="https://www.notion.so/icons/light-bulb_blue.svg" alt="https://www.notion.so/icons/light-bulb_blue.svg" width="40px" />

**DATA ENGINEERING IS ENGINEERING**

</aside>

# Data engineering capability standards will increase over time

Since it’s engineering, it’s gonna get better and better over time. Think about the first Tesla, it was super expensive, and then it got cheaper and faster and self-driving in subsequent models.

Here’s 4 ways that as data engineers we’re going to create more values over time:

- Latency
  - Solved with streaming pipelines and microbatch pipelines
    → so that data is available sooner, and processed sooner etc… For things like fraud detection this is important!
  - Another different thing between streaming and batch, it runs 24/7, whereas a batch pipeline runs once or twice a day. If you’re running something 24/7, the chance something goes wrong is a lot higher, so the quality standards must be a lot higher!
- Quality
  - Solved with best practices and frameworks like Great Expectations
- Completeness
  - Solved with communication with domain experts
    → this is very important because you need to understand the bigger picture!
- Ease-of-access and usability
  - Solved by data products and proper data modeling
    - Most people / data engineers think: “our product is we give people a data table / dashboard and they can query it or play with filters etc…”
    - **There’s way more things you can do**. For instance, when Zach was at Netflix, he built a product that surfaced all the data via API call, so you could just hit an URL and get all data you want
    - You can have automated integrations that way, where people can read and write to whatever data store that you have

As data engineers, we’re going to do more, and that will require us to hold ourselves to higher quality standards.

# How to do data engineering with a software engineering mindset

How do we create code and systems that follow the software engineering mindset?

- Code is 90% read by humans and 10% executed by machines
  - You want to write code that’s meant for humans to read, not for machines to run. That’s a very powerful thing to do, because the more readable your code is the easier it is to troubleshoot, and that’s worth way more than a 5% pipeline speed improvement.
- Silent failures are your enemy
  - That’s the type of bugs that you don’t ever want to have as much as you can
  - A lot of times the better thing to do is to `raise` the exception, as that’s going to be better than a silent failure. So if it crashes it’s gonna alert you and you can act upon it and see what’s wrong.
- Loud failures are your friend
  - Fail the pipeline when bad data is introduced
    → You can even create a special exception that is specific to the problem
  - Loud failures are accomplished by testing and CI/CD
- DRY code and YAGNI principles
  - DRY → Don’t Repeat Yourself → Encapsulate and re-use logic
  - YAGNI → You Aren’t Gonna Need It → Build things out AS they’re needed, don’t try to build out the master, awesome pipeline form the get go
  - SQL and DRY are very combative
    - SQL is quite hard to encapsulate and test [dbt to the rescue! (lol) — Ed.]
  - Design documents are your friend
  - Care about efficiency!
    - DSA, Big(O) notation, etc…
    - Understanding JOIN and shuffle → how many operations happen during a join, is it a cartesian product, etc…

## Should data engineers learn software engineering best practices?

Short answer: YES!

If the role is more analytics focused, these best practices are less valuable probably, but if you learn these practices you don’t have to be a Data Engineer, you can be a SWE if you want. It opens the door to many other opportunities.

- LLMs and other things will make the analytics and SQL layer of data engineering job more susceptible to automation
- If you don’t want to learn these things → Go into analytics engineering!
