from kafka import KafkaConsumer, KafkaProducer
import json

# ---------------------------------------------------
# KAFKA CONSUMER
# ---------------------------------------------------

consumer = KafkaConsumer(

    'raw_events',

    bootstrap_servers='localhost:9092',

    value_deserializer=lambda x: json.loads(
        x.decode('utf-8')
    )
)

# ---------------------------------------------------
# ENRICHED PRODUCER
# ---------------------------------------------------

producer = KafkaProducer(

    bootstrap_servers='localhost:9092',

    value_serializer=lambda v: json.dumps(v).encode(
        'utf-8'
    )
)

# ---------------------------------------------------
# DEAD LETTER PRODUCER
# ---------------------------------------------------

dead_letter_producer = KafkaProducer(

    bootstrap_servers='localhost:9092',

    value_serializer=lambda v: json.dumps(v).encode(
        'utf-8'
    )
)

# ---------------------------------------------------
# REQUIRED SCHEMA
# ---------------------------------------------------

required_fields = {

    "user_id": str,

    "event": str,

    "product_id": str,

    "environment": str,

    "timestamp": str
}

# ---------------------------------------------------
# VALID EVENTS
# ---------------------------------------------------

valid_events = [

    "view_product",

    "add_to_cart",

    "checkout",

    "purchase"
]

# ---------------------------------------------------
# PROCESS EVENTS
# ---------------------------------------------------

print("Event Enricher Started...")

for message in consumer:

    try:

        data = message.value

        # ---------------------------------
        # Mandatory field validation
        # ---------------------------------

        missing_fields = [

            field for field in required_fields

            if field not in data
        ]

        if missing_fields:

            data["error"] = (
                f"Missing fields: {missing_fields}"
            )

            dead_letter_producer.send(
                "dead_letter",
                value=data
            )

            print("Dead Letter:", data)

            continue

        # ---------------------------------
        # Data type validation
        # ---------------------------------

        invalid_types = []

        for field, dtype in required_fields.items():

            if not isinstance(data[field], dtype):

                invalid_types.append(field)

        if invalid_types:

            data["error"] = (
                f"Invalid data types: {invalid_types}"
            )

            dead_letter_producer.send(
                "dead_letter",
                value=data
            )

            print("Dead Letter:", data)

            continue

        # ---------------------------------
        # Event validation
        # ---------------------------------

        if data["event"] not in valid_events:

            data["error"] = "Invalid event type"

            dead_letter_producer.send(
                "dead_letter",
                value=data
            )

            print("Dead Letter:", data)

            continue

        # ---------------------------------
        # Environment validation
        # ---------------------------------

        if data["environment"] != "prod":

            print("Dropped Non-Prod Event:", data)

            continue

        # ---------------------------------
        # Enrichment
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

    except Exception as e:

        error_record = {

            "raw_data": str(message.value),

            "error": str(e)
        }

        dead_letter_producer.send(

            "dead_letter",

            value=error_record
        )

        print("Processing Error:", error_record)