"""This quickstart test reads data and performs a groupby operation
and a simple check.
"""

import os

import coiled
import dask.dataframe as dd
from dask.distributed import Client

SOFTWARE = os.environ["SOFTWARE_ENV"]

cluster = coiled.Cluster(
    software=SOFTWARE,
    n_workers=10,
    backend_options={"spot": False},
)

client = Client(cluster)

ddf = dd.read_parquet(
    "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.parquet",
    columns=["passenger_count", "tip_amount"],
    storage_options={"anon": True},
).persist()

result = ddf.groupby("passenger_count").tip_amount.mean().compute()

print(result)

client.close()
cluster.close()
