# Real-Time E-Commerce Order Streaming Pipeline

**Azure Event Hubs → Databricks Structured Streaming → Delta Lake (Medallion Architecture)**

A hands-on engineering lab implementing the end-to-end streaming architecture used by enterprise e-commerce retailers to ingest global order events in real time and serve them to analytics and operations teams within seconds.

---

## Context

Modern retailers operate global order supply chains where point-of-sale, web, and mobile channels generate continuous streams of order events that must be ingested, validated, enriched, and aggregated for real-time inventory, fraud, and revenue analytics. Production pipelines of this shape commonly sustain **10k+ events/sec** across hundreds of partitions worldwide.

This project replicates that architecture end-to-end on Azure. Because real retail order data is proprietary, the pipeline is fed by a **synthetic event generator** that produces JSON order events matching the schema, cardinality, and temporal characteristics of real retail telemetry — letting every component be exercised and validated under realistic conditions.

---

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────────┐    ┌──────────────────────────┐
│ Python producer │ →  │ Azure Event Hubs │ →  │  Azure Databricks       │ →  │ Bronze / Silver / Gold   │
│  (Kafka client) │    │ (Kafka endpoint) │    │ Structured Streaming    │    │ Delta tables (ADLS Gen2) │
└─────────────────┘    └──────────────────┘    └─────────────────────────┘    └──────────────────────────┘
```

| Layer | Service | Role |
|---|---|---|
| Event source | Python + `confluent-kafka` | Order event generator (synthetic) |
| Real-time ingestion | Azure Event Hubs Standard (Kafka surface) | Fully managed buffered ingestion |
| Near real-time processing | Azure Databricks + Spark Structured Streaming | Micro-batch transformation & enrichment |
| Storage | ADLS Gen2 + Delta Lake | Open-format lakehouse storage |
| Governance | Unity Catalog | Catalogs, schemas, volumes, external locations, storage credentials |

### Order Event Schema

```json
{
  "order_id":         "uuid",
  "user_id":          "uuid",
  "product_id":       "PROD-003",
  "product_category": "clothing",
  "price":            49.99,
  "quantity":         2,
  "country":          "US",
  "timestamp":        "2026-05-28T11:48:21.482Z"
}
```
### Repository Structure
```text
real-time-ecommerce-event-streaming/
│
├── README.md
├── LICENSE
│
├── images/
│   ├── 1st_stream_ingestion/
│   ├── 2nd_stream_ingestion/
│   ├── volume_directory.png
│   └── final_visualization.png
│
└── scripts/
    ├── 01_bronze.ipynb
    ├── 01_bronze.py
    ├── 02_silver.ipynb
    ├── 02_silver.py
    ├── 03_gold.ipynb
    ├── 03_gold.py
    ├── .gitignore
    └── event_producer/
        ├── producer.py
        └── .env.example
        
```        
---

## Medallion Implementation

### 🥉 Bronze — raw event ingestion

Reads directly from the Event Hubs Kafka endpoint over SASL/SSL (port 9093). Persists the raw JSON payload along with Kafka metadata (`topic`, `partition`, `offset`, `kafka_ts`) into a Delta table with **no transformations** — preserving source fidelity for replay, audit, and reprocessing.

**Streaming semantics:**
- **Explicit checkpoint** at `/Volumes/eh_streaming/oms/checkpoints/bronze`. Spark persists offset and commit logs to ADLS Gen2 so the stream resumes from the exact last committed offset after any cluster restart — the foundation of exactly-once delivery to the Delta sink.
- **Micro-batch trigger:** `processingTime="10 seconds"` — balances ingestion latency against output file efficiency.
- **Output mode:** `append` — every Event Hubs record becomes exactly one Delta row; no updates or deletes.
- **`failOnDataLoss=false`** — defensively skips ahead rather than failing the query if source events age out of Event Hubs retention before being consumed.

### 🥈 Silver — parsed, typed, validated

Reads from the `bronze_orders` Delta table. Parses the raw JSON `body` against a typed schema, derives `event_ts` (event-time timestamp) and `revenue` (price × quantity), and filters out malformed or invalid records (null IDs, non-positive price/quantity).

**Streaming semantics:**
- **Explicit checkpoint** at `/Volumes/eh_streaming/oms/checkpoints/silver` — distinct from Bronze. Each streaming query owns a unique checkpoint location to prevent state corruption.
- **Stateless transformation** (pure parse + filter + derive), so `outputMode("append")` is the correct and only sensible choice. No watermarking required because no aggregation or stream-stream join is performed.
- **Micro-batch trigger:** `processingTime="15 seconds"`.

### 🥇 Gold — windowed aggregations for analytics

Reads from the `silver_orders` Delta table. Applies event-time windowing to compute per-minute **revenue, order count, and unique users by `product_category` × `country`** — the shape consumed by downstream BI dashboards, alerting, and inventory systems.

**Streaming semantics:**
- **Watermark:** `withWatermark("event_ts", "10 minutes")` — tolerates up to 10 minutes of late-arriving events before discarding them and finalizing windows. This is the standard mechanism for handling out-of-order event-time data in a streaming aggregation.
- **Tumbling window:** `window("event_ts", "1 minute")` — non-overlapping 1-minute buckets keyed by event time, not processing time.
- **Output mode:** `append` — each window emits exactly one immutable row after the watermark passes its close, giving dashboard-ready, idempotent aggregates.
- **Stateful checkpoint** at `/Volumes/eh_streaming/oms/checkpoints/gold` — in addition to offset and commit logs, the checkpoint persists the *running aggregation state* itself, so partial in-flight windows survive cluster restarts without recomputation.

---

## Validation & Results

End-to-end **exactly-once delivery** was verified by reconciling source-to-sink counts across two independent ingestion runs, including a deliberate mid-pipeline cluster restart to prove checkpoint resumption:

| Run | Producer Submitted | Producer Confirmed | Producer Failed | Cumulative Bronze Count |
|---|---:|---:|---:|---:|
| Run 1 (initial backfill from `earliest`) | 31,811 | 31,811 | 0 | 31,811 |
| Run 2 (resumed from checkpoint after restart) | 25,155 | 25,155 | 0 | **56,966** |

<img width="2224" height="1430" alt="2" src="https://github.com/user-attachments/assets/4aeb5bf4-4cbb-414d-b7f9-85724161e1eb" />
<img width="3834" height="1718" alt="4" src="https://github.com/user-attachments/assets/5d717ba9-339a-44b7-b5c9-65cae9306eb3" />
<img width="2048" height="940" alt="5" src="https://github.com/user-attachments/assets/df969cf1-8366-49dc-8cda-740f9587d2b1" />

<img width="1918" height="1474" alt="1" src="https://github.com/user-attachments/assets/ee0f3b5b-dcec-4b21-82ce-96d7c5e9b5e8" />
<img width="3840" height="1808" alt="2" src="https://github.com/user-attachments/assets/b796fb67-f42c-4459-b42a-bea5406cc771" />
<img width="3838" height="1808" alt="3" src="https://github.com/user-attachments/assets/771b6e81-3834-4674-9e82-6451d5d29115" />
<img width="3834" height="1818" alt="6" src="https://github.com/user-attachments/assets/59b25764-f8d4-451f-b12d-47d5737e09fb" />


**Result: exact reconciliation across all stages.** The cumulative Bronze count of 56,966 equals the sum of producer-confirmed events (31,811 + 25,155), demonstrating:

1. **Zero data loss** through the Kafka → Event Hubs → Spark → Delta path.
2. **Correct checkpoint resumption** — Run 2 resumed from the last committed offset rather than reprocessing historical events.
3. **No duplication** despite the mid-pipeline cluster restart.

Live streaming dashboards captured during steady-state operation showed **processing rate consistently exceeding input rate** — each **micro-batch** processed faster than events arrived, confirming the pipeline operates with substantial headroom and isn't backpressured.

<img width="3840" height="1812" alt="Final Visualization" src="https://github.com/user-attachments/assets/70c76c39-aa60-4c3e-a611-d9cd29536475" />


---

## Tech Stack

- **Languages:** Python, PySpark, Spark SQL
- **Cloud:** Azure — Event Hubs Standard, Databricks Premium (Unity Catalog), ADLS Gen2
- **Frameworks:** `confluent-kafka` (producer), Spark Structured Streaming (consumer), Delta Lake
- **Governance:** Unity Catalog with a managed external location bound to ADLS Gen2 via an Azure Access Connector for Databricks
- **Tooling:** VS Code, Anaconda, Databricks Notebooks

---

## Why this architecture

- **Kafka protocol on Event Hubs** lets the same producer/consumer code run unchanged against either Apache Kafka or Event Hubs — eliminating vendor lock-in at the protocol level.
- **Delta Lake at the storage layer** unifies streaming and batch workloads against the same tables with ACID guarantees and time travel.
- **Medallion separation** keeps the raw audit trail (Bronze) intact regardless of downstream schema changes, allowing Silver/Gold to be re-derived without re-ingesting from the source.
- **Explicit per-query checkpoints** make every streaming query independently restartable and observable, which is essential for debugging and incident response in production.

---

## Note on reconciliation lag: 
Because Gold uses outputMode("append") with a 10-minute watermark, windows are emitted only after the watermark has advanced past their end. In a long-running production stream with continuous arrivals, this lag is imperceptible; in a stopped-producer lab snapshot, the most recent windows remain in pending state until the next event arrives. This trade-off — exact, immutable, idempotent outputs in exchange for emission latency is the correct choice for downstream BI consumption.
