# Real-Time Crypto Feature Pipeline

An ongoing production-style, containerized streaming pipeline that ingests live cryptocurrency market data from Coinbase, processes it through Apache Kafka, and incrementally loads engineered features to AWS S3 for downstream ML consumption.

---

## Architecture Overview

```
Coinbase WebSocket API
        │
        ▼
┌──────────────────────────────────────────────────────┐
│                 EC2 (r8g) — Docker Compose           │
│                                                      │
│  ┌─────────────────┐                                 │
│  │  Producer       │  (Python / WebSocket)           │
│  │  Microservice   │──────────────┐                  │
│  └─────────────────┘             │                   │
│          │                       │                   │
│          ├──► raw-trades         │                   │
│          └──► raw-orderbook      │                   │
│                                  ▼                   │
│                    ┌─────────────────────┐           │
│                    │  Kafka Cluster      │           │
│                    │  (KRaft mode)       │           │
│                    └─────────────────────┘           │
│                                  │                   │
│                                  ▼                   │
│  ┌─────────────────┐                                 │
│  │  Consumer       │  (Python)                       │
│  │  Microservice   │                                 │
│  └─────────────────┘                                 │
│          │                                           │
│          └──► Kafka Topic: features                  │
└──────────────────────────────────────────────────────┘
           │
           └──► CSV → AWS S3 (incremental load)
```

---

## Features

- **Live WebSocket ingestion** from Coinbase for both market trade and order book events
- **Decoupled microservices** — producer, Kafka broker, and consumer run as independent Docker containers
- **KRaft mode Kafka** — no Zookeeper dependency, simplified cluster management
- **Dual-topic architecture** — market trades and orderbook data are isolated into separate Kafka topics for independent scaling and consumption
- **Feature engineering consumer** — transforms raw events into a derived `features` topic for downstream ML use
- **Incremental S3 loading** — processed data is written to CSV and continuously synced to an S3 bucket for model training and analysis

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Coinbase Advanced Trade WebSocket API |
| Messaging | Apache Kafka (KRaft mode) |
| Producer | Python (`websockets`, `confluent-kafka`) |
| Consumer | Python (`confluent-kafka`, `boto3`) |
| Containerization | Docker / Docker Compose |
| Cloud Storage | AWS S3 |
| Feature Store | Kafka `features` topic + S3 CSV |

---

## Project Structure

```
.
├── producer/
│   ├── Dockerfile
│   ├── producer.py        # WebSocket client + Kafka producer
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile
│   ├── consumer.py        # Kafka consumer + feature engineering + S3 writer
│   └── requirements.txt
├── kafka/
│   └── setup-topics.sh    # Kafka topic creation
├── docker-compose.yml     # Orchestrates all services
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- AWS account with an S3 bucket
- Coinbase Advanced Trade API credentials

### Environment Variables

Create a `.env` file in the project root:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your_bucket_name
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

### Run the Pipeline

```bash
# Clone the repo
git clone https://github.com/aswan97/real-time-financial.git
cd real-time-financial

# Start all services
docker compose up --build
```

This will spin up:
1. The **Kafka broker** (KRaft mode)
2. The **producer** — begins streaming from the Coinbase WebSocket immediately
3. The **consumer** — begins consuming, engineering features, and writing to S3

---

## Kafka Topics

| Topic | Description |
|---|---|
| `raw-trades` | Raw trade events (price, size, side, timestamp) |
| `raw-orderbook` | Level 2 order book snapshots and diffs |
| `features` | Engineered features derived from raw events for ML consumption |

---

## Data Flow Detail

### Producer
Connects to the Coinbase WebSocket stream and subscribes to the `market_trades` and `level2` channels. Incoming events are serialized and written to their respective Kafka topics with minimal latency.

### Consumer
Reads from `raw-trades` and `raw-orderbook`, applies feature engineering logic (e.g. mid-price calculation, trade imbalance, rolling aggregations), and:
- Publishes enriched records to the `features` Kafka topic
- Appends records to a local CSV buffer
- Incrementally uploads the CSV to S3 on a configurable interval

---

## Roadmap

- [ ] Deploy containerized microservices to EC2 (r8g) with Docker Compose on AWS
- [ ] XGBoost model for price trend prediction trained on S3 features
- [ ] SageMaker endpoint for real-time inference
- [ ] Grafana dashboard for pipeline observability
- [ ] Expand to additional trading pairs and exchanges
