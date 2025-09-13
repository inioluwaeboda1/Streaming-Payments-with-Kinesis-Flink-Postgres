"""
Synthetic payments generator → Kinesis.
Works with real AWS or LocalStack (set KINESIS_ENDPOINT_URL).

Env vars:
  - KINESIS_STREAM (default: 'payments')
  - AWS_REGION (default: 'us-east-1')
  - KINESIS_ENDPOINT_URL (optional; e.g., 'http://localstack:4566')
"""

import json, random, time, os
from datetime import datetime
import boto3


def generate_payment():
    """Produce a small, realistic-looking payment event."""
    return {
        "payment_id": f"payment-{random.randint(1000, 9999)}",
        "user_id": f"user-{random.randint(1, 50)}",
        "merchant_id": f"merchant-{random.randint(1, 20)}",
        "amount": round(random.uniform(10, 1000), 2),
        "payment_time": datetime.utcnow().isoformat()
    }


def main():
    stream = os.getenv("KINESIS_STREAM", "payments")
    region = os.getenv("AWS_REGION", "us-east-1")
    endpoint = os.getenv("KINESIS_ENDPOINT_URL")  # use for LocalStack

    kinesis = boto3.client("kinesis", region_name=region, endpoint_url=endpoint)

    # Sends one event per second (simple backpressure-friendly pace).
    while True:
        p = generate_payment()
        print("Sending:", p)
        kinesis.put_record(
            StreamName=stream,
            Data=json.dumps(p).encode("utf-8"),
            PartitionKey=p["user_id"]  # shard by user to distribute load
        )
        time.sleep(1)


if __name__ == "__main__":
    main()
