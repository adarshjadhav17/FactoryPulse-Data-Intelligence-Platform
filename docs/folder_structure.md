# Folder Structure

```text
.
├── config/                # Local config templates
├── data/
│   ├── raw/               # Ignored local raw data area
│   ├── sample/            # Small generated samples for testing
│   └── profiling/         # Profiling reports and summaries
├── docs/                  # Project notes and phase docs
├── pipelines/
│   ├── airflow/
│   │   └── dags/          # Airflow DAG definitions
│   └── kafka/
│       └── producers/     # Kafka producer code
├── scripts/               # Utility scripts and local commands
├── src/
│   └── manufacturing_pipeline/
│       ├── ingestion/     # Shared ingestion helpers
│       ├── profiling/     # Data profiling logic
│       ├── utils/         # Common utilities
│       └── validation/    # Data validation helpers
├── tests/                 # Local validation tests
└── warehouse/
    ├── dbt/
    │   └── models/
    │       ├── staging/   # Cleaned source models
    │       └── marts/     # Analytics-ready models
    └── snowflake/
        └── ddl/           # Snowflake DDL files
```
