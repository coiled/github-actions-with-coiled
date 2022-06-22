"""This quickstart test reads data and performs a groupby operation
and a simple check.
"""

import os

import coiled
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

SOFTWARE = os.environ["SOFTWARE_ENV"]


cluster = coiled.Cluster(
    software=SOFTWARE,
    name=f"github-actions-{os.environ['GITHUB_RUN_ID']}", 
    n_workers=10,
    worker_memory="8Gib",
)

client = Client(cluster)

ddf = dd.read_parquet(
    "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.parquet",
    columns=["passenger_count", "tip_amount"],
    storage_options={"anon": True},
).persist()

# perform groupby aggregation
result = ddf.groupby("passenger_count").tip_amount.mean()

# write result to s3
bucket_path = "s3://coiled-github-actions-blog/github-actions/quickstart/"
try: 
    result.to_parquet(  
        bucket_path
        )
    print(f"The result was successfully written to {bucket_path}")
except:
    print("There was an error writing the result to S3.")

client.close()
cluster.close()
