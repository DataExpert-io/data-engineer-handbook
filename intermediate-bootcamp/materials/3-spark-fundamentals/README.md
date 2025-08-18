# Week 3 Spark Fundamentals training

## Unit Testing PySpark Course Getting Started

You need to install the required dependencies in `requirements.txt`

Running `pip install -r requirements.txt` will install them.

> **_NOTE:_** Make sure to have spark set locally before running below.
> Step-by-Step Guide to Install Spark and Python on Windows[https://discord.com/channels/1106357930443407391/1388500306207178824]
> Step-by-Step Guide to Install Spark and Python on Mac[https://discord.com/channels/1106357930443407391/1388501607641124874]


Running the pytest is easy. You need to run `python -m pytest` and you're good to go!



## Spark Fundamentals and Advanced Spark Setup

To launch the Spark and Iceberg Docker containers, run:

```bash
make up
```

Or `docker compose up` if you're on Windows!

Then, you should be able to access a Jupyter notebook at `localhost:8888`.

The first notebook to be able to run is the `event_data_pyspark.ipynb` inside the `notebooks` folder.


## ❓ Common Errors & Fixes

Fix for Spark OutOfMemoryError: Java heap space[https://discord.com/channels/1106357930443407391/1388501197341720666]
