# Week 3 Spark Fundamentals training

## Unit Testing PySpark Course Getting Started

You need to install the required dependencies in `requirements.txt`

Running `pip install -r requirements.txt` will install them.

Running pytest is easy. You just need to run `python -m pytest` and you're good to go!



## Spark Fundamentals and Advanced Spark Setup

To launch the Spark and Iceberg Docker containers, run:

```bash
make up
```

Or `docker compose up` if you're on Windows!

Then, you should be able to access a Jupyter notebook at `localhost:8888`.

The first notebook to be able to run is the `event_data_pyspark.ipynb` inside the `notebooks` folder.