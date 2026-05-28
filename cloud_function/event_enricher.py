from kafka import KafkaConsumer, KafkaProducer
import json
 
consumer = KafkaConsumer(
    'raw_events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
for message in consumer:
 
    data = message.value
 
    # ---------------------------------
    # Drop non-prod events
    # ---------------------------------
 
    if data["environment"] != "prod":
 
        print("Dropped:", data)
 
        continue
 
    # ---------------------------------
    # Add enrichment
    # ---------------------------------
 
    data["country"] = "India"
 
    data["pipeline_stage"] = "validated"
 
    data["is_conversion"] = (
        data["event"] == "purchase"
    )
 
    # ---------------------------------
    # Send to enriched topic
    # ---------------------------------
 
    producer.send(
        "enriched_events",
        value=data
    )
 
    producer.flush()
 
    print("Enriched:", data)