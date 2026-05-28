import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

events = [
    "view_product",
    "add_to_cart",
    "checkout",
    "purchase"
]

products = [
    "P100",
    "P101",
    "P102"
]

environments = [
    "prod",
    "test",
    "dev"
]

print("Event Generator Started...")

while True:

    data = {
        "user_id": f"U{random.randint(100,999)}",
        "event": random.choice(events),
        "product_id": random.choice(products),
        "environment": random.choice(environments),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send(
        "raw_events",
        value=data
    )

    producer.flush()

    print("Sent:", data)

    time.sleep(2)