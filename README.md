# real_time_customer_engagement_analytics
Ingest clickstream &amp; event data from Kafka, enrich with user profile data from BigQuery, compute sessionized engagement metrics (e.g., time-on-page, bounce rate, conversion funnel steps), and load aggregates into BigQuery for BI dashboards — all orchestrated via Airflow, with fallback/event-triggered paths using Cloud Functions. 
