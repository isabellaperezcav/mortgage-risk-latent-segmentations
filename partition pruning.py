import pandas as pd
import re

for file in list_of_files:
    df = pd.read_csv(file)

    match = re.match(r"(\d{4})Q(\d)", file)
    year = int(match.group(1))
    quarter = int(match.group(2))

    df["year"] = year
    df["quarter"] = quarter

    df.to_parquet(
        "bronze_parquet/",
        partition_cols=["year", "quarter"],
        compression="snappy",
        index=False
    )
