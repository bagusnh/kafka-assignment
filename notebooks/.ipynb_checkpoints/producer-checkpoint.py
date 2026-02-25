import json
import time
import random
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)
users = ["user_1", "user_2", "user_3"]

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

while True:
    user = random.choice(users)
    user_event = {
        "user": user,
        "event_type": "click",
        "timestamp": time.time()
    }

    value = json.dumps(user_event).encode("utf-8")

    producer.produce(
        topic="events_topic",
        key=user,
        value=value,
        callback=delivery_report
    )

    producer.poll(0)
    time.sleep(5)