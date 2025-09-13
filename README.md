Streaming Payments (Kinesis → Flink → Postgres)
===============================================

A compact, cloud-parity streaming pipeline: synthetic payments flow into **Kinesis** (via LocalStack), **PyFlink** runs stateful anomaly detection (10s tumbling windows + running baselines with ValueState), and results land in **Postgres** for simple SQL/BI. Everything is reproducible with **Docker Compose**.

Stack & Highlights
------------------

*   **Ingress:** LocalStack Kinesis stream payments
    
*   **Processing:** Flink 1.20, PyFlink, keyed windows, lightweight feature engineering
    
*   **Storage/Serve:** Postgres + pgAdmin
    
*   **Ops niceties:** healthchecks, dependency gating, connector JAR bootstrap, clean teardown

```javascript
Generator → Kinesis (LocalStack) → PyFlink (windows + state) → JDBC → Postgres → pgAdmin/SQL
```


### Quickstart

```bash
# 1) Boot services (downloads connector JARs, creates Kinesis stream)
docker compose up -d --build

# 2) Submit the Flink job
docker compose exec jobmanager /opt/flink/bin/flink run -py /opt/flink/app/flink-app.py

# 3) Send sample events
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export KINESIS_STREAM=payments KINESIS_ENDPOINT_URL=http://localhost:4566
python scripts/generate_payments.py

```



**Inspect**

*   Flink UI: http://localhost:8081
    
*   pgAdmin: http://localhost:5050 (DB: paymentsdb, user/pass: app/app)
    
*   SQL:

```bash
docker compose exec postgres psql -U app -d paymentsdb \
  -c "SELECT * FROM anomalies ORDER BY window_end DESC LIMIT 25;"

```
  
