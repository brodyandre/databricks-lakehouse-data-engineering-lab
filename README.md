# Databricks Lakehouse Data Engineering Lab

End-to-end data engineering project designed to demonstrate practical experience with modern Lakehouse architecture using Databricks, Apache Spark, PySpark, Delta Lake, data quality, orchestration, governance, and CI/CD.

## Project Goal

The goal of this project is to simulate a production-oriented data engineering platform for a retail/e-commerce scenario.

The solution will process customer, product, order, payment, and shipment data through a Medallion Architecture composed of Bronze, Silver, and Gold layers.

The project is being developed incrementally, with automated tests, code quality checks, documentation, and reproducible infrastructure/configuration.

## Target Architecture

```text
Data Sources
    |
    v
Ingestion
    |
    v
Bronze Layer
Raw and historical data
    |
    v
Silver Layer
Cleaning, validation, deduplication and enrichment
    |
    v
Gold Layer
Business-ready datasets and analytical data marts
    |
    v
SQL / Analytics / Dashboards
```

## Planned Technology Stack

- Databricks
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Medallion Architecture
- Unity Catalog
- Databricks Workflows
- Structured Streaming
- Databricks Declarative Automation Bundles
- Python
- SQL
- pytest
- Ruff
- GitHub Actions
- Git

## Planned Engineering Capabilities

The project will progressively demonstrate:

- Batch data ingestion
- Streaming ingestion
- Bronze, Silver, and Gold data layers
- Delta Lake ACID transactions
- MERGE and upsert operations
- Schema enforcement and schema evolution
- Data quality rules and quality gates
- Deduplication
- Business transformations
- Dimensional and analytical data marts
- Data governance
- Pipeline orchestration
- Observability and operational metadata
- Spark performance optimization
- Automated testing
- CI/CD
- Environment-based deployment

## Repository Structure

```text
.
├── .github/
│   └── workflows/
├── data/
│   ├── generator/
│   └── sample/
├── docs/
├── notebooks/
├── resources/
├── sql/
├── src/
│   └── databricks_lakehouse/
├── tests/
├── .gitignore
├── pyproject.toml
├── requirements-dev.txt
└── README.md
```

## Development Environment

The project uses Python 3.12 and an isolated virtual environment.

Create the virtual environment:

```bash
python3.12 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

## Quality Checks

Run automated tests:

```bash
pytest
```

Run linting:

```bash
ruff check .
```

Check formatting:

```bash
ruff format --check .
```

## Current Status

### Bootstrap

- [x] Repository initialized
- [x] Main branch configured
- [x] Python 3.12 environment
- [x] Virtual environment
- [x] pytest configuration
- [x] Ruff linting and formatting
- [x] Initial automated tests
- [ ] GitHub Actions CI
- [ ] Synthetic dataset generator
- [ ] Local PySpark environment
- [ ] Bronze ingestion pipeline
- [ ] Silver transformation pipeline
- [ ] Gold analytical layer
- [ ] Delta Lake implementation
- [ ] Data quality framework
- [ ] Databricks workspace integration
- [ ] Unity Catalog
- [ ] Databricks Workflows
- [ ] Structured Streaming
- [ ] CI/CD deployment

## Business Scenario

The project will use a synthetic retail/e-commerce domain containing datasets such as:

- Customers
- Products
- Orders
- Order items
- Payments
- Shipments

The Gold layer will eventually expose analytical datasets for metrics such as:

- Revenue
- Average order value
- Customer recurrence
- Product performance
- Delivery performance
- Cancellation rate
- Customer lifetime value
- Revenue by region

## Author

**Luiz André de Souza**

Data Engineering | Python | SQL | Cloud | DataOps | DevOps