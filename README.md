# BarAlgae — Public Demo (Data Infrastructure & Analytics)

[![CI](https://github.com/AmitSass/algae-data-infrastructure/actions/workflows/ci.yml/badge.svg)](https://github.com/AmitSass/algae-data-infrastructure/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/release/python-3130/)
[![dbt](https://img.shields.io/badge/dbt-1.7.19-red.svg)](https://www.getdbt.com/)

**Purpose**: a screen-ready demo showing an end-to-end data workflow with synthetic data only.  
**Scope**: minimal, reproducible example (no production code, secrets, or proprietary logic).

## 30-Second Summary

Load synthetic FlowCAM-like measurements, model with dbt, and produce an analytics-ready fact table. The demo runs locally on Postgres (Docker).

**In production:** Python ingestors write partitioned Parquet to **S3** (Bronze/Silver), and **dbt on Redshift** builds **Gold marts** from S3 (COPY/Spectrum).

*More docs: [Project Overview](docs/OVERVIEW.md)*

## Architecture

```mermaid
flowchart LR
  A["Seed CSV (synthetic)"] --> B[("Postgres - demo")]
  B --> C["dbt models: staging -> gold"]
  C --> D["Analytics table: fact_flowcam__summary"]
```

## Quickstart

```bash
git clone https://github.com/AmitSass/algae-data-infrastructure && cd algae-data-infrastructure
cp .env.example .env
# .env is git-ignored; fill with demo values only
docker compose up -d          # starts Postgres for the demo
pip install -U dbt-postgres
export DBT_PROFILES_DIR=transform/dbt
# Windows (PowerShell): $env:DBT_PROFILES_DIR="transform/dbt"
cd transform/dbt
dbt deps && dbt seed && dbt run && dbt test
```

**Optional**: The repo includes examples for Airflow / Great Expectations, but they're not required for this minimal run.

## What's Inside

- **`transform/dbt/`** — Complete Medallion architecture:
  - **Staging**: Raw data from 5 sources (FlowCam, SCADA, Weather, Growth, Harvest)
  - **Intermediate**: Daily aggregations and business logic
  - **Marts**: 5 fact tables ready for analytics + comprehensive schema tests
- **`docker-compose.yml`** — Postgres container
- **`.env.example`** — sample configuration (no real values)
- **`docs/diagrams/architecture.mmd`** — Mermaid diagram
- **(optional)** `orchestration/airflow/`, `data_quality/great_expectations/` examples

## Demo ↔ Prod Mapping

| Demo | Production (typical) |
|------|---------------------|
| Postgres (container) | Redshift / BigQuery / Snowflake |
| dbt seeds (CSV) | S3 "Silver" Parquet dataset / external tables |
| staging → intermediate → marts (full medallion) | stg / int / marts (full medallion) |
| comprehensive schema tests + dbt_utils | richer tests + data-quality tooling |
| local .env (ignored) | secrets via env/vault/CI |

## Data Sources & Models

**5 Data Sources:**
- **FlowCam** - Microscopy algae density measurements
- **SCADA** - Process control system data (temperature, pH, etc.)
- **Weather** - Environmental conditions (temperature, humidity, solar)
- **Growth Tracking** - Manual growth rate measurements
- **Harvest** - Production and quality records

**dbt Models (demo):**
- **Staging:** 7 models (FlowCam, SCADA, Weather, Growth, Harvest)
- **Intermediate:** 3 models (FlowCam daily agg/summary, SCADA daily parameters)
- **Marts:** 6 fact tables + 1 dimension (`dim_tpu`)

**Professional Features:**
- **Comprehensive testing** - Unique combinations, data ranges, relationships
- **Data freshness monitoring** - Source-level freshness checks
- **Medallion architecture** - Staging → Intermediate → Marts
- **dbt_utils integration** - Advanced testing capabilities

## Tech

Python · dbt · Postgres · Docker (examples reference Airflow & Great Expectations)

## Security & IP

Synthetic data only. No production endpoints, schemas, metrics, or secrets are included in this repository.

**Security Features:**
- All credentials use environment variables (`env_var()`)
- No hardcoded secrets in any configuration files
- Demo data only - no real business data
- Secret scanning and Dependabot alerts enabled

## Support

Questions? Open a [GitHub Issue](https://github.com/AmitSass/algae-data-infrastructure/issues)

Email (optional): hi@amitsass.dev

## Production Architecture

```mermaid
flowchart LR
  A["Ingest (Python)"] --> B["S3 Bronze/Silver (Parquet, partitioned)"]
  B -->|"COPY/Spectrum"| C[("Redshift: staging/external")]
  D["dbt (runs on Redshift)"]
  C -->|"read/query"| D
  D -->|"materialize"| E[("Redshift: Gold marts")]
  E --> F["BI / ML"]
```

## License

MIT © Amit Sasson