#!/bin/bash
flink run \
  --python flink-app/flink-app.py \
  --jarfile ./connectors/flink-connector-aws-kinesis-1.20.x.jar \
  --jarfile ./connectors/flink-connector-jdbc-3.1.x-1.20.jar \
  --jarfile ./connectors/postgresql-42.7.3.jar
