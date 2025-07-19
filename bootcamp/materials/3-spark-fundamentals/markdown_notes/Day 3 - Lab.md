# Day 3 - Lab

In this lab, we will take a look at testing in Spark.

We need to install the `requirements.txt` at `bootcamp/materials/3-spark-fundamentals/`.

```python
python -m venv .venv
pip install -r requirements.txt

# then run this to check that everything works
python -m pytest
```

Check in the `src/jobs/` directory, you will see 3 files with the suffix `_job.py`. These are all Spark jobs.

One thing one can do to make Spark jobs more testable, is to create a function outside where you build and write (e.g. `main`), that just does the transformation logic.

When you do that, then in your test code you can test just the transformation logic, as we don’t really need to test if the Spark session gets created, or if the data gets written out.

The main thing we want to test 99.99% of the times is if the transform logic works.

Let’s now look at the `tests/` folder, the file `conftest.py`.

What this file does is it gives us Spark anywhere it is referenced in a test file (this is common PyTest behavior thanks to [fixtures](https://docs.pytest.org/en/6.2.x/fixture.html)).

Now go to `players_scd_job.py` and add a +1 to `start_date` in aggregated, just to mess around, then run `pytest` in the terminal. In the output you will see that chispa will start yelling at you, also showing where your data output is wrong w.r.t. expected.

How do you generate fake data for the test? You literally write it down, like this:

```python
source_data = [
    PlayerSeason("Michael Jordan", 2001, 'Good'),
    PlayerSeason("Michael Jordan", 2002, 'Good'),
    PlayerSeason("Michael Jordan", 2003, 'Bad'),
    PlayerSeason("Someone Else", 2003, 'Bad')
]
source_df = spark.createDataFrame(source_data)

expected_data = [
    PlayerScd("Michael Jordan", 'Good', 2001, 2002),
    PlayerScd("Michael Jordan", 'Bad', 2003, 2003),
    PlayerScd("Someone Else", 'Bad', 2003, 2003)
]
expected_df = spark.createDataFrame(expected_data)
```

Let’s now create a new job and a new test [not really, it’s already written — Ed.].

It’s in the file `team_vertex_job.py`. You will see there’s two functions, besides the query.

1. `main` — creates the Spark session, calls the transform function and writes out.
2. `do_team_vertex_transformation` — effectively runs the transformation (the sql query).

Now open `test_team_vertex_job.py`. Notice how `namedtuple` is used to create rows of “fake” data.

Then follow along the python file, it’s easy to understand by reading the code.
