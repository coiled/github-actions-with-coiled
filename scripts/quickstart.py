"""This quickstart test reads data and performs a groupby operation
and a simple check.
"""

import os

import coiled
import dask.dataframe as dd
from dask.distributed import Client

GITHUB_RUN_ID = os.environ["GITHUB_RUN_ID"]


cluster = coiled.Cluster(
    name=f"github-actions-{GITHUB_RUN_ID}",
    n_workers=10,
    worker_memory="8Gib",
    package_sync=True,
    account="dask-engineering",
)

client = Client(cluster)

ddf = dd.read_parquet(
    "s3://coiled-datasets/nyc-tlc/2019",
    columns=["passenger_count", "tip_amount"],
    storage_options={"anon": True},
).persist()

# perform groupby aggregation
result = ddf.groupby("passenger_count").tip_amount.mean()
result = result.to_frame()

# write result to s3
bucket_path = (
    f"s3://coiled-github-actions-blog/github-action-{GITHUB_RUN_ID}/quickstart.parquet"
)
result.to_parquet(
    bucket_path,
)
print(f"The result was successfully written to {bucket_path}")


client.close()
cluster.close()
