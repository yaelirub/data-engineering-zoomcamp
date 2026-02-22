"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import datetime
import io
import json
import os

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)

    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    extracted_at = datetime.datetime.now(datetime.timezone.utc)

    dfs = []
    for taxi_type in taxi_types:
        for year, month in months:
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = base_url + filename
            response = requests.get(url)
            if response.status_code == 404:
                print(f"No data available for {filename}, skipping.")
                continue
            response.raise_for_status()
            df = pd.read_parquet(io.BytesIO(response.content))
            # Normalise vendor-specific datetime column names so the schema is
            # consistent regardless of taxi type (yellow=tpep_*, green=lpep_*)
            df = df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            })
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)
