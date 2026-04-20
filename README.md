# IoT Manufacturing Lakehouse Pipeline

> End-to-end Azure Data Engineering pipeline ingesting real-time IoT sensor data from factory floor equipment into a governed, queryable lakehouse. Built to mirror the architecture patterns used in production manufacturing analytics.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694A?logo=dbt)](https://www.getdbt.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-00ADD8)](https://delta.io)
[![Terraform](https://img.shields.io/badge/Terraform-1.7-7B42BC?logo=terraform)](https://terraform.io)
[![Azure](https://img.shields.io/badge/Azure-Data_Engineering-0078D4?logo=microsoft-azure)](https://azure.microsoft.com)
[![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-10B981)](https://greatexpectations.io)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-181717?logo=github-actions)](https://github.com/features/actions)

---

## The Problem This Solves

Factory floors generate continuous sensor telemetry — temperature, pressure, vibration, cycle counts — from hundreds of machines. Without a structured pipeline, this data either gets discarded or sits in flat files that engineers manually download and analyse in Excel. Defect patterns are invisible until it's too late.

This pipeline ingests sensor events in near real-time, applies quality validation, enriches with machine metadata, and delivers aggregated OEE (Overall Equipment Effectiveness) metrics to a Power BI dashboard — refreshed hourly. Engineers can see machine health trends before equipment fails, not after.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ IoT Sensors  │  │  ERP System  │  │  MES System  │              │
│  │ (MQTT/HTTP)  │  │  (Oracle DB) │  │  (REST API)  │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
└─────────┼─────────────────┼─────────────────┼────────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                  │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │              Azure Data Factory (ADF)                        │   │
│  │  • Tumbling window triggers (hourly)                        │   │
│  │  • Watermark-based incremental loads                        │   │
│  │  • Event Hub connector for IoT stream                       │   │
│  │  • Parameterised, metadata-driven pipeline config           │   │
│  └──────────────────────────┬───────────────────────────────────┘   │
└─────────────────────────────┼───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STORAGE — ADLS Gen2                               │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────────────────┐  │
│  │   BRONZE    │──▶│    SILVER    │──▶│         GOLD            │  │
│  │  Raw zone   │   │  Cleansed +  │   │  Business aggregates    │  │
│  │  Parquet /  │   │  Enriched /  │   │  Star schema / Delta    │  │
│  │  Delta Lake │   │  Validated   │   │  Z-ordered + compacted  │  │
│  └─────────────┘   └──────────────┘   └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER                                │
│  ┌───────────────────────┐    ┌──────────────────────────────────┐  │
│  │  Azure Databricks     │    │  Great Expectations              │  │
│  │  PySpark notebooks    │    │  Data quality checkpoints        │  │
│  │  Bronze → Silver      │    │  - Null / completeness checks    │  │
│  │  Silver → Gold        │    │  - Schema validation             │  │
│  └───────────────────────┘    │  - Business rule assertions      │  │
│                               └──────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  dbt (Analytics Engineering Layer)                           │  │
│  │  Staging → Intermediate → Marts                              │  │
│  │  Schema tests · Source freshness · Column-level lineage      │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SERVING LAYER                                      │
│  ┌──────────────────────┐    ┌─────────────────────────────────┐   │
│  │  Azure Synapse SQL   │    │  Power BI                       │   │
│  │  Dedicated pool      │    │  OEE Dashboard                  │   │
│  │  DirectQuery         │    │  Machine Health Dashboard       │   │
│  └──────────────────────┘    │  Defect Rate Trend              │   │
│                               └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│               OBSERVABILITY & GOVERNANCE                             │
│  ┌────────────────┐  ┌───────────────┐  ┌────────────────────────┐ │
│  │ Azure Monitor  │  │    Grafana    │  │    Unity Catalog       │ │
│  │ + Log Analytics│  │  Dashboards   │  │  Lineage + RBAC        │ │
│  │ Alert rules    │  │  Pipeline SLAs│  │  Access policies       │ │
│  └────────────────┘  └───────────────┘  └────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │          Terraform — All infra version-controlled               ││
│  │          GitHub Actions — CI/CD for dbt + ADF                  ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```
<img width="955" height="596" alt="image" src="https://github.com/user-attachments/assets/afaa780d-9a63-44b7-ba90-7d5f1aff6397" />

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Azure Data Factory | Orchestration, incremental loading, tumbling windows |
| Streaming | Azure Event Hubs | IoT sensor event ingestion |
| Storage | ADLS Gen2 + Delta Lake | Medallion lakehouse storage |
| Compute | Azure Databricks + PySpark | Distributed transformation |
| Transform | dbt | SQL-based modelling, testing, documentation |
| Quality | Great Expectations | Data quality checkpoints at every layer |
| Warehouse | Azure Synapse Analytics | Star-schema serving layer |
| IaC | Terraform | Reproducible infra across dev/staging/prod |
| BI | Power BI | Executive and operational dashboards |
| Observability | Grafana + Azure Monitor | Pipeline SLA monitoring and alerting |
| Governance | Unity Catalog + Azure Purview | Data lineage, access control |
| CI/CD | GitHub Actions | Automated dbt test + deploy on PR |

---

## Repository Structure

```
iot-lakehouse-pipeline/
│
├── adf/                          # ADF pipeline ARM export configs
│   ├── pipelines/
│   │   ├── pl_ingest_sensor_bronze.json
│   │   ├── pl_transform_silver.json
│   │   └── pl_aggregate_gold.json
│   └── linked_services/
│       ├── ls_adls_gen2.json
│       ├── ls_event_hubs.json
│       └── ls_oracle_source.json
│
├── databricks/
│   ├── bronze/
│   │   └── ingest_sensor_events.py        # Raw IoT event landing
│   ├── silver/
│   │   ├── clean_sensor_data.py           # Null handling, schema enforcement
│   │   ├── enrich_machine_metadata.py     # Join with machine dimension
│   │   └── ge_checkpoint_silver.py        # Great Expectations validation
│   └── gold/
│       ├── agg_oee_hourly.py              # OEE aggregation logic
│       ├── agg_defect_rates.py            # Defect rate by line/shift
│       └── fact_machine_events.py         # Fact table construction
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_sensor_events.sql
│   │   │   ├── stg_machine_metadata.sql
│   │   │   └── stg_shift_schedule.sql
│   │   ├── intermediate/
│   │   │   ├── int_sensor_enriched.sql
│   │   │   └── int_oee_components.sql
│   │   └── marts/
│   │       ├── mart_oee_daily.sql
│   │       ├── mart_defect_summary.sql
│   │       └── mart_machine_health.sql
│   ├── tests/
│   │   ├── assert_oee_between_0_and_1.sql
│   │   └── assert_no_duplicate_sensor_readings.sql
│   ├── macros/
│   │   └── generate_surrogate_key.sql
│   └── dbt_project.yml
│
├── great_expectations/
│   ├── expectations/
│   │   ├── sensor_events_bronze.json
│   │   └── sensor_events_silver.json
│   └── checkpoints/
│       └── bronze_to_silver_checkpoint.yml
│
├── terraform/
│   ├── main.tf                    # Resource group, ADLS, ADF, Databricks
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── databricks/
│       ├── adls/
│       └── synapse/
│
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml             # Run dbt test on every PR
│       └── terraform_plan.yml     # Terraform plan on infra changes
│
├── docs/
│   └── architecture.md
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Key Engineering Decisions

**Why Delta Lake over plain Parquet?**
Delta adds ACID transactions, schema enforcement, and time travel. In a manufacturing context where machines occasionally send malformed readings, the ability to roll back a bad batch without reprocessing the full history is essential. Z-ordering on `machine_id` and `event_timestamp` reduces query scan times for the most common filter pattern (show me machine X over the last 7 days) by approximately 60–70%.

**Why dbt on top of Databricks transformations?**
PySpark handles the heavy lifting — cleaning at scale, joining large tables in distributed memory. dbt handles the business logic — the definitions of OEE, the defect rate calculations, the shift-level aggregations. Keeping these separate means analysts can own the mart layer without touching Spark code, and every transformation is tested and documented automatically.

**Why Great Expectations at the Bronze → Silver boundary?**
The most expensive place to catch bad data is in the Gold layer, when it's already been serving dashboards for six hours. Validating at the Silver promotion step means broken data fails loudly and early — the pipeline halts, an alert fires, and downstream consumers never see the problem.

**Why Terraform and not ARM templates alone?**
Terraform's state management makes environment parity between dev, staging, and prod deterministic. ARM templates alone don't prevent configuration drift over time. With Terraform, `terraform plan` on a PR shows exactly what would change — that's the infrastructure equivalent of a dbt test passing before merge.

---

## Data Model — Gold Layer

```
                    ┌──────────────────┐
                    │  DimMachine      │
                    │  machine_id (PK) │
                    │  machine_name    │
                    │  line_id         │
                    │  machine_type    │
                    └────────┬─────────┘
                             │
   ┌──────────────┐          │          ┌──────────────────┐
   │  DimDate     │          │          │  DimShift        │
   │  date_id(PK) │          │          │  shift_id (PK)   │
   │  date        │          │          │  shift_name      │
   │  week        │          │          │  start_time      │
   │  month       │          │          │  end_time        │
   └──────┬───────┘          │          └────────┬─────────┘
          │                  │                   │
          └──────────────────┼───────────────────┘
                             │
                    ┌────────▼─────────────────────┐
                    │      FactMachineEvents        │
                    │  event_id          (PK)       │
                    │  machine_id        (FK)       │
                    │  date_id           (FK)       │
                    │  shift_id          (FK)       │
                    │  availability_rate            │
                    │  performance_rate             │
                    │  quality_rate                 │
                    │  oee_score                    │
                    │  defect_count                 │
                    │  cycle_time_seconds           │
                    │  downtime_minutes             │
                    └──────────────────────────────┘
```

---

## Running Locally (Dev Mode)

```bash
# 1. Clone and install
git clone https://github.com/Neeraj600/iot-lakehouse-pipeline
cd iot-lakehouse-pipeline
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Fill in: ADLS_ACCOUNT_NAME, DATABRICKS_HOST, DATABRICKS_TOKEN, etc.

# 3. Provision infra (requires Azure CLI authenticated)
cd terraform
terraform init
terraform plan    # Review what will be created
terraform apply

# 4. Run dbt tests
cd ../dbt
dbt deps
dbt test          # All schema and custom tests
dbt run           # Build all models

# 5. Validate data quality
cd ../great_expectations
great_expectations checkpoint run bronze_to_silver_checkpoint
```

---

## CI/CD Pipeline

Every pull request triggers:
1. `dbt test` — all schema tests, freshness checks, and custom SQL assertions must pass
2. `terraform plan` — infra changes are reviewed before merge
3. Great Expectations validation on sample data in the staging environment

Merges to `main` trigger `dbt run` against the production Snowflake/Synapse target.

---

## Outcomes (Simulated on synthetic dataset)

| Metric | Before Pipeline | After Pipeline |
|---|---|---|
| Analysis latency | 3–4 hours manual | Hourly automated refresh |
| Data quality issues caught | Discovered in dashboard | Caught at Bronze → Silver |
| Engineer hours for reporting | ~40 hrs/week | ~2 hrs/week (exception handling only) |
| OEE visibility | Per-shift Excel exports | Real-time Power BI dashboard |

---

## What I'd Do Differently at Scale

- Switch from ADF tumbling windows to **Apache Airflow** with a proper DAG dependency graph for more complex workflow branching
- Add **Apache Flink** for true event-time processing on the sensor stream (ADF + Event Hubs is near-real-time, not millisecond-latency)
- Introduce **dbt Mesh** once the model count exceeds ~50 — separate staging, intermediate, and mart domains with explicit public contracts between teams

---

*Built by Neeraj Singh Guleria · [linkedin.com/in/neeraj-guleria](https://linkedin.com/in/neeraj-guleria) · [heyneeraj.netlify.app](https://heyneeraj.netlify.app)*
