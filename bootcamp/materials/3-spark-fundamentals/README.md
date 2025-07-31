# Week 3 Spark Fundamentals training

## Unit Testing PySpark Course Getting Started

You need to install the required dependencies in `requirements.txt`

Running `pip install -r requirements.txt` will install them.

> **_NOTE:_** Make sure to have spark set locally before running below.

### Troubleshooting: PySpark & Pytest Environment Error

If you encounter an error like:

> The error indicates that while PySpark was installed in your system Python environment (`/Library/Frameworks/Python.framework/Versions/3.13/`), `pytest` is trying to run using Anaconda Python (`/opt/anaconda3/lib/python3.13/`), which doesn't have PySpark installed.

This usually means you have multiple Python environments, and PySpark is not available in the environment where `pytest` runs.

**Solution (Recommended): Use a Virtual Environment**

```bash
# Create a new virtual environment
python -m venv spark_env

# Activate the environment
source spark_env/bin/activate  # On macOS/Linux
# or
spark_env\Scripts\activate     # On Windows

# Install your requirements
pip install -r requirements.txt

# Run your tests
pytest

Running the pytest is easy. You need to run `python -m pytest` and you're good to go!



## Spark Fundamentals and Advanced Spark Setup

To launch the Spark and Iceberg Docker containers, run:

```bash
make up
```

Or `docker compose up` if you're on Windows!

Then, you should be able to access a Jupyter notebook at `localhost:8888`.

The first notebook to be able to run is the `event_data_pyspark.ipynb` inside the `notebooks` folder.
