# Streaming-Payments-with-Kinesis-Flink-Postgres
A real-time payments pipeline using Docker Compose, LocalStack Kinesis, PyFlink (Flink 1.20), and Postgres: events stream into Kinesis, are validated/enriched and window-aggregated in PyFlink, and upserted into Postgres for SQL analytics in pgAdmin—demonstrating streaming ETL and fully reproducible local dev.
