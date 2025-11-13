## Data Cleaning Best Practices 
# Data Cleaning Best Practices

- Remove duplicate rows to avoid data leakage.
- Standardize column names (lowercase, underscores).
- Handle missing values using median/mean or domain logic.
- Convert date columns to proper datetime format.
- Validate data types before modeling.

## Python Example

import pandas as pd

df = pd.read_csv("data.csv")

df = df.drop_duplicates()
df.columns = [c.lower().replace(" ", "_") for c in df.columns]

num_cols = df.select_dtypes(include="number").columns
df[num_cols] = df[num_cols].fillna(df[num_cols].median())

if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"])

