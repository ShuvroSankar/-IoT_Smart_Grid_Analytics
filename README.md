# Enterprise Industrial IoT & Energy Analytics Platform

<div align="center">

[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Looker%20Studio-4285F4?style=for-the-badge&logo=googleanalytics&logoColor=white)](https://datastudio.google.com/reporting/292b667c-04df-4a54-9f23-e98458c343a7)
[![Dev Notes](https://img.shields.io/badge/Dev%20Notes-Notion-000000?style=for-the-badge&logo=notion&logoColor=white)](https://app.notion.com/p/IoT-Smart-Grid-Analytics-384ef3c0ef9080429611fe76055c2b32)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![BigQuery](https://img.shields.io/badge/Google-BigQuery-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

**An enterprise-grade, cloud-hybrid BI platform bridging industrial hardware telemetry with corporate financial analysis.**

[View Live Dashboard →](https://datastudio.google.com/reporting/292b667c-04df-4a54-9f23-e98458c343a7) · [Development Notes →](https://app.notion.com/p/IoT-Smart-Grid-Analytics-384ef3c0ef9080429611fe76055c2b32) · [Report a Bug](https://github.com/ShuvroSankar/-IoT_Smart_Grid_Analytics/issues)

</div>

---

## Project Summary

This system simulates a high-scale manufacturing facility cluster, ingesting and structuring multi-variable sensor telemetry to **optimize resource schedules, mitigate equipment risks, and minimize utility financial waste**. It showcases a full end-to-end data engineering pipeline — from raw IoT sensor simulation all the way to an executive-facing live BI dashboard — deployed entirely on free-tier cloud infrastructure.

**Key achievement:** Processed **130,000+ synthetic sensor records** spanning a 3-year timeline through a fully automated, containerized pipeline into Google BigQuery, with zero cloud spend.

---

## System Architecture

```
┌─────────────────────────────────────────────┐
│     Local IoT Sensor Clusters               │
│     (Mac Mini Daemon — Python Simulator)    │
└─────────────────┬───────────────────────────┘
                  │
         Docker Compose (Linux Sandbox)
                  │
┌─────────────────▼───────────────────────────┐
│     Resilient Micro-Batch Ingestion Gateway │
│     (Python · google-cloud-bigquery SDK)    │
└─────────────────┬───────────────────────────┘
                  │
        Apache Arrow File Transport
                  │
┌─────────────────▼───────────────────────────┐
│     Google BigQuery Data Warehouse          │
│     (Optimized Star Schema)                 │
│     PARTITION BY DATE · CLUSTER BY machine  │
└─────────────────┬───────────────────────────┘
                  │
       Native Low-Latency SQL Connector
                  │
┌─────────────────▼───────────────────────────┐
│     Executive BI Command Center             │
│     (Google Looker Studio)                  │
│     ► Live Public Dashboard                 │
└─────────────────────────────────────────────┘
```

---

## Key Engineering Highlights

### 1. Resilient Micro-Batch Ingestion Architecture
Engineered a containerized Python ingestion daemon in Docker that securely aggregates raw telemetry and **bypasses BigQuery Sandbox streaming restrictions** entirely by using the Cloud Storage Load Job API — uploading Parquet dataframes at zero streaming cost.

### 2. Production-Grade Cloud Data Warehouse Design
Designed an optimized analytical **Star Schema** in Google BigQuery with:
- `PARTITION BY DATE(timestamp)` — dramatically reduces query scan costs at scale
- `CLUSTER BY machine_id` — collocates related machine records for faster lookups
- Fact + Dimension separation for clean, maintainable query logic

### 3. Synthetic Data Enrichment at Scale
Expanded an initial 3,100-row seed dataset using an automated cloud enrichment engine (`ml_engine.py`) to generate **130,000+ historical records** across a 3-year timeline — preserving realistic equipment failure probability distributions and mathematical covariance between sensor variables.

### 4. Electrical Engineering Domain Integration
Programmed real-time power engineering computations directly in the pipeline:
- **Active Power (kW)** calculations from voltage × current × power factor
- **Power Factor tracking** with threshold-based fault detection
- **Overvoltage fault flagging** cross-referenced against dynamic peak/off-peak corporate utility tariff models

### 5. Executive Decision Analytics Dashboard
Deployed a live Looker Studio dashboard featuring:
- Multi-variable anomaly filtering with drill-down capability
- Total financial expenditure scorecards per machine and per floor
- Down-sampled monthly time-series trend lines for predictive budget scheduling

---

## Cloud Data Warehouse Schema

### Fact Table — `fact_energy_consumption`
| Column | Type | Description |
|---|---|---|
| `timestamp` | TIMESTAMP | Sensor reading time (partition key) |
| `machine_id` | STRING | Foreign key to `dim_machines` (cluster key) |
| `voltage_v` | FLOAT | Voltage reading in Volts |
| `current_a` | FLOAT | Current reading in Amperes |
| `temperature_c` | FLOAT | Operating temperature in Celsius |
| `energy_consumed_kwh` | FLOAT | Calculated energy usage per interval |
| `electricity_cost_usd` | FLOAT | Tariff-adjusted cost (peak/off-peak model) |
| `is_anomaly` | BOOL | Anomaly flag from threshold detection logic |

> Partitioned by `DATE(timestamp)` · Clustered by `machine_id`

### Dimension Table — `dim_machines`
| Column | Description |
|---|---|
| `machine_id` | Unique asset identifier |
| `machine_name` | Human-readable asset name |
| `machine_type` | Equipment category (e.g., CNC, HVAC, Press) |
| `factory_floor` | Physical location within facility |
| `installation_date` | Asset commissioning date |

### Dimension Table — `dim_date`
Enables fast time-intelligence sorting across year, quarter, month, week number, and weekend flag dimensions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.11 |
| **Cloud Warehouse** | Google BigQuery (Sandbox — free tier) |
| **Data Transport** | Apache Arrow / Parquet via Load Job API |
| **Containerization** | Docker Compose (isolated Linux sandbox) |
| **Visualization** | Google Looker Studio |
| **Key Libraries** | `google-cloud-bigquery`, `pandas`, `pyarrow`, `pandas-gbq` |
| **Secret Management** | `.env` file + GCP Service Account IAM JSON Key |

---

## Quickstart

### Prerequisites
- Docker & Docker Compose installed
- A Google Cloud account (free tier is sufficient)
- A GCP Service Account JSON key with BigQuery Data Editor + Job User roles

### 1. Clone the Repository
```bash
git clone https://github.com/ShuvroSankar/-IoT_Smart_Grid_Analytics.git
cd -IoT_Smart_Grid_Analytics
```

### 2. Configure Environment Variables
Create a `.env` file in the project root:
```env
GCP_PROJECT_ID=your-gcp-project-id-here
GOOGLE_APPLICATION_CREDENTIALS=/app/gcp_credentials.json
PYTHONUNBUFFERED=1
```
Place your GCP Service Account JSON key in the root directory and name it `gcp_credentials.json`.

> **Never commit** `gcp_credentials.json` or `.env` to version control. Both are listed in `.gitignore`.

### 3. Build & Launch the Ingestion Gateway
```bash
docker compose up -d --build
```

Monitor the live micro-batch loading stream:
```bash
docker logs iot_data_pipeline -f
```

### 4. Generate the Historical Dataset (130K+ Records)
```bash
docker compose run --rm data_pipeline python app/ml_engine.py
```

Once ingestion completes, your BigQuery tables are populated and the [live Looker Studio dashboard](https://datastudio.google.com/reporting/292b667c-04df-4a54-9f23-e98458c343a7) will reflect the data automatically.

---

## Project Structure

```
-IoT_Smart_Grid_Analytics/
├── docker-compose.yml          # Container orchestration config
├── Dockerfile                  # Python ingestion daemon image
├── .env.example                # Environment variable template
├── .gitignore                  # Excludes credentials and caches
├── requirements.txt            # Python dependencies
│
├── app/
│   ├── ingest.py               # Micro-batch ingestion gateway
│   └── ml_engine.py            # Synthetic data enrichment engine
│
├── schema/
│   ├── fact_energy.sql         # BigQuery fact table DDL
│   ├── dim_machines.sql        # Machines dimension DDL
│   └── dim_date.sql            # Date dimension DDL
│
└── docs/
    └── architecture.png        # System architecture diagram
```

---

## Results & Impact

| Metric | Value |
|---|---|
| Total records ingested | **130,000+** |
| Historical timeline simulated | **3 years** |
| BigQuery streaming cost | **$0** (Load Job API) |
| Cloud infrastructure cost | **$0** (BigQuery Sandbox + Looker Studio free tier) |
| Anomaly detection coverage | Multi-variable (voltage, temperature, power factor) |
| Dashboard refresh | Near real-time via native BigQuery connector |

---

## Links

| Resource | URL |
|---|---|
| Live Dashboard | [Looker Studio Report](https://datastudio.google.com/reporting/292b667c-04df-4a54-9f23-e98458c343a7) |
| Development Notes | [Notion Page](https://app.notion.com/p/IoT-Smart-Grid-Analytics-384ef3c0ef9080429611fe76055c2b32) |
| Source Code | [GitHub Repository](https://github.com/ShuvroSankar/-IoT_Smart_Grid_Analytics) |

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

Built with ☕ by [ShuvroSankar](https://github.com/ShuvroSankar)

*If this project helped you, consider giving it a ⭐*

</div>
