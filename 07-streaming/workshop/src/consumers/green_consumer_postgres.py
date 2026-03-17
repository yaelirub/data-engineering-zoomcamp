import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'
table_name = 'green_trips'


def to_int(value):
    if value is None:
        return None
    return int(value)


def to_float(value):
    if value is None:
        return None
    return float(value)


# Connect to PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

cur.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        pulocationid INTEGER,
        dolocationid INTEGER,
        passenger_count DOUBLE PRECISION,
        trip_distance DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION
    )
    """
)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-to-postgres',
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda data: json.loads(data.decode('utf-8')),
)

print(f"Listening to {topic_name} and writing to PostgreSQL table {table_name}...")

count = 0
for message in consumer:
    ride = message.value
    cur.execute(
        f"""INSERT INTO {table_name}
           (lpep_pickup_datetime, lpep_dropoff_datetime, pulocationid, dolocationid,
            passenger_count, trip_distance, tip_amount, total_amount)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            ride.get('lpep_pickup_datetime'),
            ride.get('lpep_dropoff_datetime'),
            to_int(ride.get('PULocationID')),
            to_int(ride.get('DOLocationID')),
            to_float(ride.get('passenger_count')),
            to_float(ride.get('trip_distance')),
            to_float(ride.get('tip_amount')),
            to_float(ride.get('total_amount')),
        ),
    )
    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()

print(f"Finished. Inserted {count} rows into {table_name}.")