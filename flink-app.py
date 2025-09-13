"""
Kinesis → Flink (PyFlink) → Postgres: streaming anomaly detection per merchant.

- Source: AWS Kinesis (via LocalStack in dev), records are JSON payments.
- Processing: tumbling processing-time windows + running, stateful baseline average.
- Detection: flag a 10s window if its average amount > 1.5× historical running avg.
- Sinks: prints anomalies for live debugging and upserts to Postgres via JDBC.

Notes:
- This job uses processing time (no watermarks) to keep the demo simple.
- For production, switch to event-time + watermarks and enable checkpointing.
"""

import json
import os
from dataclasses import dataclass

from pyflink.common import Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kinesis import KinesisStreamsSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import lit


@dataclass
class Payment:
    """Typed view of a payment event coming from Kinesis."""
    payment_id: str
    user_id: str
    merchant_id: str
    amount: float
    payment_time: str


def parse_payment(json_str: str) -> Payment:
    """Parse tolerant JSON -> Payment; fills defaults for missing fields."""
    d = json.loads(json_str)
    return Payment(
        payment_id=d.get("payment_id", "unknown"),
        user_id=d.get("user_id", "unknown"),
        merchant_id=d.get("merchant_id", "unknown"),
        amount=float(d.get("amount", 0.0)),
        payment_time=d.get("payment_time", "unknown"),
    )


class PaymentsAnomaliesDetector(ProcessWindowFunction):
    """
    Window function with keyed operator state to track a running baseline.

    State:
      - total_count: long  → cumulative count per merchant
      - total_amount: double → cumulative amount per merchant

    Logic:
      - Compute window_avg over 10s window.
      - Compare to running_avg = total_amount / total_count.
      - Emit anomaly JSON if window_avg > 1.5 * running_avg.
    """

    def open(self, ctx):
        self.total_count = ctx.get_state(
            ValueStateDescriptor("total_count", Types.LONG())
        )
        self.total_amount = ctx.get_state(
            ValueStateDescriptor("total_amount", Types.DOUBLE())
        )

    def process(self, key, context, elements):
        current_total_count = self.total_count.value() or 0
        current_total_amount = self.total_amount.value() or 0

        window_total = 0.0
        window_count = 0

        for p in elements:
            window_count += 1
            window_total += p.amount

        # Only compare after we have both historical and current data
        if window_count > 0 and current_total_count > 0:
            running_avg = current_total_amount / current_total_count
            window_avg = window_total / window_count
            if window_avg > 1.5 * running_avg:
                # Minimal JSON payload to keep JDBC schema narrow
                yield json.dumps({
                    "merchant_id": key,
                    "running_average": running_avg,
                    "window_average": window_avg
                })

        # Update running totals for next windows
        self.total_count.update(current_total_count + window_count)
        self.total_amount.update(current_total_amount + window_total)


def build_kinesis_source() -> KinesisStreamsSource:
    """
    Configure Kinesis source (defaults suitable for LocalStack).
    Env:
      - KINESIS_STREAM: stream name (default: 'payments')
      - AWS_REGION: region (default: 'us-east-1')
    """
    stream = os.getenv("KINESIS_STREAM", "payments")
    region = os.getenv("AWS_REGION", "us-east-1")
    return (
        KinesisStreamsSource.builder()
        .set_stream(stream)
        .set_region(region)
        .set_serialization_schema(SimpleStringSchema())  # messages are plain JSON strings
        .set_shard_iterator_type("LATEST")               # start from tail for dev runs
        .build()
    )


def main():
    # --- DataStream API: ingest + detect anomalies ---
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # Processing-time windows for simplicity; swap to event-time + watermarks for prod
    source = build_kinesis_source()
    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kinesis_source"
    )

    payments = stream.map(parse_payment)

    anomalies_json = (
        payments
        .key_by(lambda p: p.merchant_id)  # per-merchant baseline
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(PaymentsAnomaliesDetector(), output_type=Types.STRING())
    )

    # Dev-friendly live view in TaskManager logs
    anomalies_json.print("DetectedAnomalies")

    # --- Table API: write anomalies to Postgres (JDBC sink) ---
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Connection comes from env (kept in docker-compose). Do not hardcode secrets in prod.
    pg_host = os.getenv("PG_HOST", "postgres")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db   = os.getenv("PG_DB", "paymentsdb")
    pg_user = os.getenv("PG_USER", "app")
    pg_pwd  = os.getenv("PG_PWD", "app")

    t_env.execute_sql(f"""
        CREATE TABLE anomalies_sink (
          merchant_id STRING,
          running_average DOUBLE,
          window_average DOUBLE,
          window_end TIMESTAMP(3)
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}',
          'table-name' = 'anomalies',
          'driver' = 'org.postgresql.Driver',
          'username' = '{pg_user}',
          'password' = '{pg_pwd}'
        )
    """)

    # Map JSON -> row schema expected by the sink
    from pyflink.datastream.functions import MapFunction
    from pyflink.common.typeinfo import Types as T

    class JsonToRow(MapFunction):
        def map(self, s: str):
            d = json.loads(s)
            return (d["merchant_id"], float(d["running_average"]), float(d["window_average"]))

    row_stream = anomalies_json.map(
        JsonToRow(),
        output_type=T.TUPLE([T.STRING(), T.DOUBLE(), T.DOUBLE()])
    )

    # Bridge DataStream → Table; append an ingestion-time column
    table = t_env.from_data_stream(
        row_stream,
        DataTypes.ROW([
            DataTypes.FIELD("merchant_id", DataTypes.STRING()),
            DataTypes.FIELD("running_average", DataTypes.DOUBLE()),
            DataTypes.FIELD("window_average", DataTypes.DOUBLE())
        ])
    ).add_columns(
        # For precise event-time, carry window_end from the window assigner instead.
        lit("NOW").to_timestamp_ltz(3).alias("window_end")
    )

    # Submit continuous insert into JDBC sink
    table.execute_insert("anomalies_sink")

    # Launch the pipeline (job name appears in the Flink UI)
    env.execute("Kinesis → Flink → Postgres anomalies detection")


if __name__ == "__main__":
    main()
