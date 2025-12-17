import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

station_id = "PARIS_01"

while True:
    weather_data = {
        "station_id": station_id,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(-5, 35), 2),
        "humidity": random.randint(30, 90),
        "pressure": random.randint(980, 1030),
        "wind_speed": round(random.uniform(0, 15), 2)
    }

    producer.send("weather-data", weather_data)
    print(f"Sent: {weather_data}")

    time.sleep(2)
