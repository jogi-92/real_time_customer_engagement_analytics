from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
 
from datetime import datetime
 
from google.cloud import bigquery
from googleapiclient.discovery import build
 
import requests
 
# ---------------------------------------------------
# PROJECT CONFIG
# ---------------------------------------------------
 
PROJECT_ID = "project-6e64a1a1-8e69-4a88-93d"
 
REGION = "us-central1"
 
DATAFLOW_JOB_NAME = "advanced-kafka-analytics"
 

 
# ---------------------------------------------------
# CHECK DATAFLOW
# ---------------------------------------------------
 
def monitor_dataflow():
 
    service = build('dataflow', 'v1b3')
 
    request = service.projects().locations().jobs().list(
        projectId=PROJECT_ID,
        location=REGION
    )
 
    response = request.execute()
 
    jobs = response.get('jobs', [])
 
    found_running = False
 
    for job in jobs:
 
        job_name = job.get('name', '')
 
        state = job.get('currentState', '')
 
        print(f"Checking Job: {job_name}")
 
        print(f"State: {state}")
 
        if DATAFLOW_JOB_NAME in job_name:
 
            # Ignore cancelled jobs
            if state == "JOB_STATE_RUNNING":
 
                found_running = True
 
                print(
                    "Dataflow Streaming Job is RUNNING"
                )
 
                break
 
    if not found_running:
 
        raise Exception(
            "No RUNNING Dataflow job found"
        )
 
# ---------------------------------------------------
# VALIDATE BIGQUERY
# ---------------------------------------------------
 
def validate_bigquery():
 
    client = bigquery.Client()
 
    query = """
    SELECT COUNT(*) AS total_rows
    FROM `project-6e64a1a1-8e69-4a88-93d.realtime_analytics.streaming_metrics`
    """
 
    results = client.query(query).result()
 
    for row in results:
 
        print("Total Rows:", row.total_rows)
 
        if row.total_rows == 0:
 
            raise Exception(
                "No rows found in BigQuery"
            )
 
# ---------------------------------------------------
# SLACK ALERT
# ---------------------------------------------------
 
# 
 
# ---------------------------------------------------
# DAG CONFIG
# ---------------------------------------------------
 
default_args = {
 
    'owner': 'jogeswar',
 
    'start_date': datetime(2026, 5, 21),
 
    'retries': 1
}
 
# ---------------------------------------------------
# DAG
# ---------------------------------------------------
 
with DAG(
 
    dag_id='realtime_pipeline_monitoring',
 
    default_args=default_args,
 
    schedule_interval='@hourly',
 
    catchup=False,
 
    on_failure_callback=send_slack_alert
 
) as dag:
 
    # ---------------------------------------------------
    # START ZOOKEEPER
    # ---------------------------------------------------
 
    start_zookeeper = BashOperator(
    task_id='start_zookeeper',
 
    bash_command="""
    cd ~/kafka_2.13-3.7.0
 
    if jps | grep -q QuorumPeerMain; then
        echo "Zookeeper already running"
    else
        nohup bin/zookeeper-server-start.sh \
        config/zookeeper.properties \
> /tmp/zookeeper.log 2>&1 &
 
        sleep 20
    fi
 
    jps | grep QuorumPeerMain || true
    """,
    )
 
    # ---------------------------------------------------
    # START KAFKA
    # ---------------------------------------------------
 
    start_kafka = BashOperator(
    task_id='start_kafka',
 
    bash_command="""
    cd ~/kafka_2.13-3.7.0
 
    if jps | grep -q Kafka; then
        echo "Kafka already running"
    else
        nohup bin/kafka-server-start.sh \
        config/server.properties \
> /tmp/kafka.log 2>&1 &
 
        sleep 30
    fi
 
    jps | grep Kafka || true
    """,
    )
 
    # ---------------------------------------------------
    # START EVENT GENERATOR
    # ---------------------------------------------------
 
    start_generator = BashOperator(
 
        task_id='start_generator',
 
        bash_command="""
        cd ~/kafka_2.13-3.7.0
        nohup python3 event_generator1.py > /tmp/generator.log 2>&1 < /dev/null &
        sleep 5
        echo "Generator Started"
        """
    )
 
    # ---------------------------------------------------
    # START EVENT ENRICHER
    # ---------------------------------------------------
 
    start_enricher = BashOperator(
 
        task_id='start_enricher',
 
        bash_command="""
        cd ~
        nohup python3 event_enricher.py > /tmp/enricher.log 2>&1 < /dev/null &
        sleep 5
        echo "Enricher Started"
        """
    )
 
    # ---------------------------------------------------
    # MONITOR DATAFLOW
    # ---------------------------------------------------
 
    monitor_task = PythonOperator(
 
        task_id='monitor_dataflow',
 
        python_callable=monitor_dataflow
    )
 
    # ---------------------------------------------------
    # VALIDATE BIGQUERY
    # ---------------------------------------------------
 
    validate_task = PythonOperator(
 
        task_id='validate_bigquery',
 
        python_callable=validate_bigquery
    )
 
    # ---------------------------------------------------
    # DAG FLOW
    # ---------------------------------------------------
 
    (
        start_zookeeper
>> start_kafka
>> start_generator
>> start_enricher
>> monitor_task
>> validate_task
    )