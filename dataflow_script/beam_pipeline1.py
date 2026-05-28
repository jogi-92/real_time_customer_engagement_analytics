import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.transforms.window import Sessions
import json
import logging
from datetime import datetime

# ---------------------------------------------------
# LOGGER CONFIGURATION
# ---------------------------------------------------

logging.basicConfig(

    level=logging.INFO,

    format=(
        "%(asctime)s - "
        "%(levelname)s - "
        "%(message)s"
    )
)

logger = logging.getLogger(__name__)

PROJECT_ID = "project-6e64a1a1-8e69-4a88-93d"

# ---------------------------------------------------
# USER DIMENSION TABLE
# ---------------------------------------------------

logger.info("Loading dim_users table")

dim_users = {

    "U101": {
        "country": "India",
        "user_type": "Premium"
    },

    "U102": {
        "country": "USA",
        "user_type": "Free"
    },

    "U103": {
        "country": "UK",
        "user_type": "Premium"
    }
}

# ---------------------------------------------------
# PARSE EVENTS
# ---------------------------------------------------

class ParseEvents(beam.DoFn):

    def process(self, element):

        try:

            logger.info(
                "Reading Kafka message"
            )

            message = element[1]

            data = json.loads(
                message.decode("utf-8")
            )

            logger.info(
                f"Incoming Event: {data}"
            )

            # ---------------------------------
            # ONLY PROD EVENTS
            # ---------------------------------

            if data["environment"] != "prod":

                logger.info(
                    f"Dropped Non-Prod Event: {data}"
                )

                return

            user_id = data["user_id"]

            logger.info(
                f"Processing User: {user_id}"
            )

            # ---------------------------------
            # JOIN DIM_USERS
            # ---------------------------------

            if user_id in dim_users:

                logger.info(
                    f"Joining dim_users for "
                    f"user_id={user_id}"
                )

                data["country"] = (
                    dim_users[user_id]["country"]
                )

                data["user_type"] = (
                    dim_users[user_id]["user_type"]
                )

            else:

                logger.warning(
                    f"user_id={user_id} "
                    f"not found in dim_users"
                )

                data["country"] = "Unknown"

                data["user_type"] = "Unknown"

            # ---------------------------------
            # CONVERSION FLAG
            # ---------------------------------

            data["is_conversion"] = (

                data["event"] == "purchase"
            )

            logger.info(
                f"is_conversion="
                f"{data['is_conversion']}"
            )

            # ---------------------------------
            # EVENT TIME
            # ---------------------------------

            data["event_time"] = (

                datetime.now().timestamp()
            )

            logger.info(
                f"Enriched Event: {data}"
            )

            yield data

        except Exception as e:

            logger.error(
                f"ERROR PARSING EVENT: {str(e)}"
            )

# ---------------------------------------------------
# COMPUTE SESSION METRICS
# ---------------------------------------------------

class ComputeSessionMetrics(beam.DoFn):

    def process(self, element):

        try:

            logger.info(
                "Computing Session Metrics"
            )

            user_id, events = element

            events = list(events)

            logger.info(
                f"user_id={user_id}, "
                f"event_count={len(events)}"
            )

            # ---------------------------------
            # EVENT COUNT
            # ---------------------------------

            event_count = len(events)

            # ---------------------------------
            # SESSION DURATION
            # ---------------------------------

            event_times = [

                e["event_time"]

                for e in events
            ]

            duration = (

                max(event_times)

                - min(event_times)
            )

            logger.info(
                f"Session Duration="
                f"{duration} seconds"
            )

            # ---------------------------------
            # BOUNCE RATE
            # ---------------------------------

            bounce = 1 if event_count == 1 else 0

            logger.info(
                f"Bounce Rate={bounce}"
            )

            # ---------------------------------
            # FUNNEL COUNTS
            # ---------------------------------

            purchase_count = sum(

                1 for e in events

                if e["event"] == "purchase"
            )

            view_count = sum(

                1 for e in events

                if e["event"] == "view_product"
            )

            add_cart_count = sum(

                1 for e in events

                if e["event"] == "add_to_cart"
            )

            checkout_count = sum(

                1 for e in events

                if e["event"] == "checkout"
            )

            logger.info(

                f"view_count={view_count}, "
                f"add_cart_count={add_cart_count}, "
                f"checkout_count={checkout_count}, "
                f"purchase_count={purchase_count}"
            )

            output = {

                "user_id": user_id,

                "sessions_per_user_24h": 1,

                "avg_session_duration_sec": float(
                    duration
                ),

                "bounce_rate": float(
                    bounce
                ),

                "purchase_count": purchase_count,

                "view_count": view_count,

                "add_cart_count": add_cart_count,

                "checkout_count": checkout_count,

                "country": events[0]["country"],

                "user_type": events[0]["user_type"],

                "processing_time": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            }

            logger.info(
                f"Final Metrics Output: {output}"
            )

            yield output

        except Exception as e:

            logger.error(
                f"ERROR COMPUTING METRICS: {str(e)}"
            )

# ---------------------------------------------------
# PIPELINE OPTIONS
# ---------------------------------------------------

logger.info(
    "Initializing Pipeline Options"
)

options = PipelineOptions(

    streaming=True,

    runner="DataflowRunner",

    project=PROJECT_ID,

    region="us-central1",

    temp_location=(
        "gs://realtime-dataflow-bucket/temp"
    ),

    staging_location=(
        "gs://realtime-dataflow-bucket/staging"
    ),

    job_name="advanced-kafka-analytics-v7",

    save_main_session=True,

    experiments=["use_runner_v2"]
)

# ---------------------------------------------------
# PIPELINE
# ---------------------------------------------------

logger.info(
    "Starting Beam Pipeline"
)

with beam.Pipeline(options=options) as p:

    # ---------------------------------------------------
    # READ FROM KAFKA
    # ---------------------------------------------------

    logger.info(
        "Reading Events From Kafka"
    )

    events = (

        p

        | "Read Kafka" >> ReadFromKafka(

            consumer_config={

                "bootstrap.servers":
                "10.128.0.14:9092",

                "group.id":
                "beam-consumer-group",

                "auto.offset.reset":
                "latest"
            },

            topics=["enriched_events"],

            expansion_service=
            "localhost:12345"
        )

        | "Parse Events" >> beam.ParDo(
            ParseEvents()
        )
    )

    # ---------------------------------------------------
    # SESSIONIZATION
    # ---------------------------------------------------

    logger.info(
        "Applying Session Windows"
    )

    sessionized = (

        events

        | "Key By User" >> beam.Map(

            lambda x: (
                x["user_id"],
                x
            )
        )

        | "Session Window" >> beam.WindowInto(

            Sessions(30 * 60)
        )

        | "Group Sessions" >> beam.GroupByKey()

        | "Compute Metrics" >> beam.ParDo(

            ComputeSessionMetrics()
        )
    )

    # ---------------------------------------------------
    # WRITE TO BIGQUERY
    # ---------------------------------------------------

    logger.info(
        "Writing Metrics To BigQuery"
    )

    sessionized | "Write To BigQuery" >> beam.io.WriteToBigQuery(

        f"{PROJECT_ID}:realtime_analytics.streaming_metrics",

        schema="""
            user_id:STRING,
            sessions_per_user_24h:INTEGER,
            avg_session_duration_sec:FLOAT,
            bounce_rate:FLOAT,
            purchase_count:INTEGER,
            view_count:INTEGER,
            add_cart_count:INTEGER,
            checkout_count:INTEGER,
            country:STRING,
            user_type:STRING,
            processing_time:STRING
        """,

        write_disposition=(
            beam.io.BigQueryDisposition.WRITE_APPEND
        ),

        create_disposition=(
            beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

logger.info(
    "PIPELINE STARTED SUCCESSFULLY"
)

