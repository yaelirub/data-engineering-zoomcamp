import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda data: json.loads(data.decode('utf-8')),
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    print(
        f"Received: PU={ride.get('PULocationID')}, DO={ride.get('DOLocationID')}, "
        f"distance={ride.get('trip_distance')}, amount=${ride.get('total_amount')}, "
        f"pickup={ride.get('lpep_pickup_datetime')}"
    )
    count += 1

consumer.close()
print(f"Finished reading {count} messages from {topic_name}.")
