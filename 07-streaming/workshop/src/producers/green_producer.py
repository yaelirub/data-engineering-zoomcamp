import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer

# Download NYC green taxi trip data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)
df = df.sort_values('lpep_pickup_datetime').reset_index(drop=True)

def ride_serializer(ride):
    json_str = json.dumps(ride)
    return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

topic_name = 'green-trips'
datetime_columns = df.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns
start = time.perf_counter()

for _, row in df.iterrows():
    ride = row.to_dict()
    for col, value in ride.items():
        if col in datetime_columns:
            ride[col] = value.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(value) else None
        else:
            ride[col] = None if pd.isna(value) else value
    producer.send(topic_name, value=ride)

producer.flush()

end = time.perf_counter()
print(f'took {(end - start):.2f} seconds')
