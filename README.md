# chem-data-pipeline

Real-time chemical data pipeline using Kafka, Spark, and Flink for ML preprocessing.

## Overview

This project builds an end-to-end data pipeline that ingests public chemical datasets, processes them for machine learning, and performs real-time data quality checks.

```
Data Source → Kafka → Spark (batch ETL) → Parquet / CSV (ML-ready)
                    → Flink (real-time quality check) → Alert
```

## Tech Stack

| Tool | Role |
|------|------|
| Apache Kafka | Data ingestion and streaming |
| Apache Spark | Batch ETL and feature engineering |
| Apache Flink | Real-time data quality checks |
| Docker | Local environment setup |
| Python | Producer, consumer, and processing scripts |

## Dataset

[ESOL (Delaney)](https://github.com/deepchem/deepchem) — 1,128 molecules with measured aqueous solubility, sourced from DeepChem's public dataset repository.

## Project Structure

```
chem-data-pipeline/
├── producer/
│   └── esol_producer.py       # Streams ESOL dataset into Kafka
├── spark-batch/
│   └── etl.py                 # PySpark batch preprocessing (coming soon)
├── flink-stream/
│   └── quality_check.py       # PyFlink real-time validation (coming soon)
├── docker-compose.yml         # Kafka + Zookeeper + Kafka UI
└── README.md
```

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.8+

### 1. Start Kafka

```bash
docker compose up -d
```

Kafka UI is available at http://localhost:8080

### 2. Run the Producer

```bash
pip install kafka-python pandas requests
python producer/esol_producer.py
```

This streams 1,128 molecular records into the `chem-raw` Kafka topic at 0.5s intervals.

## Roadmap

- [x] Phase 1 — Kafka environment setup with Docker
- [x] Phase 2 — Python producer streaming ESOL dataset
- [ ] Phase 3 — PySpark batch ETL and feature engineering
- [ ] Phase 4 — PyFlink real-time data quality checks

## License

MIT
