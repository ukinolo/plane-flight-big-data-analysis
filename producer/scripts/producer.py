import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from opensky_api import OpenSkyApi

KAFKA_BROKER = "kafka:9092"
TOPIC = "stream-flights-raw"

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Kafka is ready")
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka")
        time.sleep(3)

api = OpenSkyApi()

for _ in range(0, 100):
    try:
        states = api.get_states()
        print(f"Fetched {len(states.states)} airplanes")
        if states and states.states:
            for s in states.states:
                record = {
                    "callsign": s.callsign,
                    "origin_country": s.origin_country,
                    "time_position": s.time_position,
                    "last_contact": s.last_contact,
                    "longitude": s.longitude,
                    "latitude": s.latitude,
                    "altitude": s.baro_altitude,
                    "velocity": s.velocity,
                    "vertical_rate": s.vertical_rate,
                    "heading": s.true_track,
                    "on_ground": s.on_ground,
                    "time": states.time
                }
                producer.send(TOPIC, record)
            producer.flush()
        else:
            print("Response was unexpected, breaking!")
            break
        time.sleep(15)

    except Exception as e:
        print("Error:", e)
        time.sleep(30)