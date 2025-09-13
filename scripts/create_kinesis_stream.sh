#!/bin/bash
set -e
STREAM_NAME=${KINESIS_STREAM:-payments}
REGION=${AWS_REGION:-us-east-1}
ENDPOINT=${KINESIS_ENDPOINT_URL:-http://localhost:4566}

aws --endpoint-url=$ENDPOINT kinesis create-stream \
  --stream-name $STREAM_NAME --shard-count 1

aws --endpoint-url=$ENDPOINT kinesis describe-stream-summary \
  --stream-name $STREAM_NAME
