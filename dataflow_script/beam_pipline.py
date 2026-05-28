import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.transforms.window import Sessions
import json
from datetime import datetime
 
PROJECT_ID = "project-6e64a1a1-8e69-4a88-93d"
 
# ---------------------------------------------------
# USER DIMENSION
# ---------------------------------------------------
 
dim_users = {
    "U101": {"country": "India", "user_type": "Premium"},
    "U102": {"country": "USA", "user_type": "Free"},
    "U103": {"country": "UK", "user_type": "Premium"}
}
 
# ---------------------------------------------------
# PARSE EVENTS
# ---------------------------------------------------
 
class ParseEvents(beam.DoFn):
 
    def process(self, element):
 
        try:
 
            message = element[1]
 
            data = json.loads(message.decode("utf-8"))
 
            # only prod events
            if data["environment"] != "prod":
                return
 
            user_id = data["user_id"]
 
            # join dim_users
            if user_id in dim_users:
 
                data["country"] = dim_users[user_id]["country"]
 
                data["user_type"] = dim_users[user_id]["user_type"]
 
            else:
 
                data["country"] = "Unknown"
 
                data["user_type"] = "Unknown"
 
            # conversion flag
            data["is_conversion"] = (
                data["event"] == "purchase"
            )
 
            data["event_time"] = datetime.now().timestamp()
 
            yield data
 
        except Exception as e:
 
            print("ERROR PARSING EVENT:", str(e))
 
 
# ---------------------------------------------------
# SESSION METRICS
# ---------------------------------------------------
 
class ComputeSessionMetrics(beam.DoFn):
 
    def process(self, element):
 
        try:
 
            user_id, events = element
 
            events = list(events)
 
            event_count = len(events)
 
            bounce = 1 if event_count == 1 else 0
 
            purchase_count = sum(
                1 for e in events
                if e["event"] == "purchase"
            )
 
            duration = event_count * 30
 
            yield {
 
                "user_id": user_id,
 
                "sessions_per_user_24h": 1,
 
                "avg_session_duration_sec": float(duration),
 
                "bounce_rate": float(bounce),
 
                "purchase_count": purchase_count,
 
                "view_count": sum(
                    1 for e in events
                    if e["event"] == "view_product"
                ),
 
                "add_cart_count": sum(
                    1 for e in events
                    if e["event"] == "add_to_cart"
                ),
 
                "checkout_count": sum(
                    1 for e in events
                    if e["event"] == "checkout"
                ),
 
                "country": events[0]["country"],
 
                "user_type": events[0]["user_type"],
 
                "processing_time": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            }
 
        except Exception as e:
 
            print("ERROR COMPUTING METRICS:", str(e))
 
 
# ---------------------------------------------------
# PIPELINE OPTIONS
# ---------------------------------------------------
 
options = PipelineOptions(
 
    streaming=True,
 
    runner="DataflowRunner",
 
    project=PROJECT_ID,
 
    region="us-central1",
 
    temp_location="gs://realtime-dataflow-bucket/temp",
 
    staging_location="gs://realtime-dataflow-bucket/staging",
 
    job_name="advanced-kafka-analytics-v2",
 
    save_main_session=True,
 
    experiments=["use_runner_v2"],
 
    requirements_file=None
)
 
# ---------------------------------------------------
# PIPELINE
# ---------------------------------------------------
 
with beam.Pipeline(options=options) as p:
 
    events = (
 
        p
 
        | "Read Kafka" >> ReadFromKafka(
 
            consumer_config={
 
                "bootstrap.servers": "10.128.0.14:9092",
 
                "group.id": "beam-consumer-group",
 
                "auto.offset.reset": "latest"
            },
 
            topics=["enriched_events"]
        )
 
        | "Parse Events" >> beam.ParDo(ParseEvents())
 
    )
 
    sessionized = (
 
        events
 
        | "Key By User" >> beam.Map(
            lambda x: (x["user_id"], x)
        )
 
        | "Session Window" >> beam.WindowInto(
            Sessions(30 * 60)
        )
 
        | "Group Sessions" >> beam.GroupByKey()
 
        | "Compute Metrics" >> beam.ParDo(
            ComputeSessionMetrics()
        )
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
 
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
 
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
 
print("PIPELINE STARTED SUCCESSFULLY")