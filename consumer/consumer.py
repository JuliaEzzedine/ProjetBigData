import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "weather-data",
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='weather-group'
)

print("Waiting for weather data...")

for message in consumer:
    print(f"Received: {message.value}")
