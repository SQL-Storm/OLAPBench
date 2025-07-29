# OLAPBench

A comprehensive benchmarking framework for Online Analytical Processing (OLAP) database management systems. OLAPBench provides standardized benchmarks and automated testing capabilities across multiple database systems including DuckDB, PostgreSQL, ClickHouse, Hyper, Umbra, MonetDB, SingleStore, SQL Server, and more.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Supported Database Systems](#supported-database-systems)
- [Supported Benchmarks](#supported-benchmarks)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running Benchmarks](#running-benchmarks)
- [Benchmark Results](#benchmark-results)
- [Project Structure](#project-structure)
- [Adding New Systems](#adding-new-systems)
- [Adding New Benchmarks](#adding-new-benchmarks)
- [Examples](#examples)
- [Contributing](#contributing)

## Overview

OLAPBench is designed to provide fair, reproducible, and comprehensive performance comparisons between different OLAP database systems. It automates the entire benchmarking process from data generation and database setup to query execution and result collection.

The framework supports:
- **Multiple benchmark suites** including TPC-H, TPC-DS, ClickBench, Join Order Benchmark (JOB), Star Schema Benchmark (SSB), and StackOverflow
- **Containerized database deployments** for consistent testing environments
- **Query plan analysis** and performance profiling
- **Automated result collection** and CSV export
- **Timeout handling** and error recovery
- **Version comparison** across different database releases

## Features

- 🚀 **Automated Benchmarking**: Complete automation from data generation to result analysis
- 📊 **Multiple Metrics**: Query execution time, compilation time, memory usage, and result validation
- 🐳 **Docker Integration**: Containerized database deployments for reproducible environments
- 📈 **Query Plan Analysis**: Capture and analyze query execution plans
- 🔄 **Retry Logic**: Automatic retry on failures with container cleanup
- 📝 **Comprehensive Logging**: Detailed logging with configurable verbosity levels
- 🎯 **Selective Testing**: Include/exclude specific queries or systems
- 📋 **Result Export**: CSV export for further analysis and visualization

## Supported Database Systems

| Database | Version Support | Container | Notes |
|----------|----------------|-----------|-------|
| **DuckDB** | 0.7.0+ | ✅ | In-memory and persistent modes |
| **PostgreSQL** | 12.0+ | ✅ | Full SQL support |
| **ClickHouse** | Latest | ✅ | Columnar storage optimized |
| **Hyper** | Latest | ✅ | High-performance engine |
| **Umbra** | Latest | ✅ | Research prototype |
| **MonetDB** | Latest | ✅ | Column-store pioneer |
| **SingleStore** | Latest | ✅ | Distributed SQL |
| **SQL Server** | Latest | ✅ | Microsoft's enterprise RDBMS |
| **CedarDB** | Latest | ✅ | Modern analytical engine |
| **Apollo** | Latest | ✅ | SQL Server variant |

## Supported Benchmarks

### TPC-H (Transaction Processing Performance Council - H)
- **Purpose**: Ad-hoc query performance on business data
- **Scale Factors**: 0.01, 0.1, 1, 10, 100+ GB
- **Queries**: 22 complex analytical queries
- **Data**: Order/lineitem, customer, supplier, part, nation, region tables

### TPC-DS (Transaction Processing Performance Council - Decision Support)
- **Purpose**: Decision support system performance
- **Scale Factors**: Configurable from MB to TB
- **Queries**: 99 complex analytical queries
- **Data**: Retail business model with complex relationships

### ClickBench
- **Purpose**: Real-world web analytics workload
- **Data Source**: Yandex.Metrica web analytics dataset
- **Queries**: 43 queries covering typical analytical patterns
- **Size**: ~100GB of real web traffic data

### Join Order Benchmark (JOB)
- **Purpose**: Complex join performance evaluation
- **Data Source**: Internet Movie Database (IMDb)
- **Queries**: 113 queries with complex join patterns
- **Focus**: Multi-table joins and query optimization

### Star Schema Benchmark (SSB)
- **Purpose**: Star schema performance (derived from TPC-H)
- **Schema**: Simplified star schema design
- **Queries**: 13 queries testing different analytical patterns
- **Use Case**: Data warehouse workloads

### StackOverflow
- **Purpose**: Real-world forum data analysis
- **Data Source**: StackOverflow database dump
- **Queries**: Analytical queries on posts, users, votes, comments
- **Size**: Multi-GB real dataset

## Installation

### Prerequisites

- **Python 3.7+**
- **Docker** (for containerized database systems)
- **Git** (for cloning the repository)
- **Virtual Environment** support

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/SQL-Storm/OLAPBench.git
   cd OLAPBench
   ```

2. **Run the setup script:**
   ```bash
   ./setup.sh
   ```

   This will:
   - Create a Python virtual environment in `.venv/`
   - Install all required dependencies from `requirements.txt`
   - Set up the benchmarking environment

3. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate
   ```

## Quick Start

### Basic Benchmark Run

1. **Run a simple test:**
   ```bash
   ./benchmark.sh test/duckdb.benchmark.yaml
   ```

2. **Run with verbose output:**
   ```bash
   ./benchmark.sh --verbose test/postgres.benchmark.yaml
   ```

3. **Clear previous results and run:**
   ```bash
   ./benchmark.sh --clear test/clickhouse.benchmark.yaml
   ```

### Using Python Directly

```bash
# Activate virtual environment
source .venv/bin/activate

# Run benchmark
python benchmark.py -j test/duckdb.benchmark.yaml --verbose
```

## Configuration

### Benchmark Definition Format

Benchmarks are defined using YAML configuration files. Here's a complete example:

```yaml
title: "DuckDB Performance Test"
repetitions: 3                    # Number of query repetitions
warmup: 1                        # Warmup runs before measurement
timeout: 300                     # Query timeout in seconds
global_timeout: 1800             # Total benchmark timeout in seconds
fetch_result: true               # Whether to fetch and validate results
fetch_result_limit: 1000         # Limit rows fetched for validation
output: "results/duckdb/"        # Output directory for results

systems:
  - title: "DuckDB ${version}"   # System title (supports templating)
    dbms: duckdb                 # Database system identifier
    disabled: false              # Whether to skip this system
    parameter:                   # Parameter matrix for testing
      version:
        - "0.9.0"
        - "1.0.0"
        - "latest"
      buffer_size:               # Optional: memory configuration
        - "1GB"
        - "4GB"
    settings:                    # Database-specific settings
      max_memory: "8GB"
      threads: 4

benchmarks:
  - name: tpch                   # Benchmark identifier
    scale: 1                     # Scale factor (1 = 1GB for TPC-H)
    included_queries:            # Optional: specific queries to run
      - "1"
      - "6"
      - "14"
    excluded_queries:            # Optional: queries to skip
      - "2"
  - name: clickbench
    # No scale needed for ClickBench
```

### Parameter Matrix

The framework supports parameter matrices for testing multiple configurations:

```yaml
systems:
  - title: "PostgreSQL ${version} (${shared_buffers})"
    dbms: postgres
    parameter:
      version: ["13.0", "14.0", "15.0", "16.0"]
      shared_buffers: ["1GB", "2GB", "4GB"]
    settings:
      shared_buffers: "${shared_buffers}"
      max_connections: 100
```

This creates 16 different test configurations (4 versions × 4 buffer sizes).

## Running Benchmarks

### Command Line Options

```bash
./benchmark.sh [OPTIONS] <config.yaml> [benchmark_names...]

Options:
  --clear          Clear previous results before running
  --launch         Only launch databases without running queries
  --verbose/-v     Enable verbose logging
  --veryverbose/-vv Enable very verbose logging
  --noretry        Don't retry on failure

Examples:
  ./benchmark.sh test/duckdb.benchmark.yaml
  ./benchmark.sh --clear --verbose production/tpch.benchmark.yaml
  ./benchmark.sh config.yaml tpch ssb  # Run only TPC-H and SSB
```

### Python Interface

```bash
python benchmark.py [OPTIONS]

Options:
  -j/--json FILE   Benchmark configuration file (required)
  -v/--verbose     Verbose output
  -vv/--very-verbose Very verbose output
  --db DIR         Database storage directory (default: ./db)
  --data DIR       Data storage directory (default: ./data)
  --env FILE       Environment variables file
  --clear          Clear previous results
  --launch         Launch databases only
```

### Environment Variables

Create a `.env` file or use `--env` to specify database connection parameters:

```bash
# Database connection settings
POSTGRES_PASSWORD=mypassword
CLICKHOUSE_PASSWORD=mypassword

# Resource limits
MAX_MEMORY=16GB
MAX_THREADS=8

# Docker settings
DOCKER_MEMORY_LIMIT=32GB
```

## Benchmark Results

### Output Structure

Results are organized in the specified output directory:

```
results/
├── duckdb/
│   ├── tpch_sf1.csv              # Main results CSV
│   ├── tpch_sf1_plans/           # Query plans (if enabled)
│   │   ├── query_1_plan.json
│   │   └── query_6_plan.xml
│   └── logs/                     # Detailed logs
│       ├── duckdb_1.0.0.log
│       └── benchmark.log
└── postgres/
    └── tpch_sf1.csv
```

### CSV Results Format

The main results are exported to CSV with the following columns:

| Column | Description |
|--------|-------------|
| `title` | System configuration title |
| `query` | Query identifier |
| `state` | Execution state (success/error/timeout/oom/fatal) |
| `client_total` | End-to-end execution times (JSON array) |
| `total` | Database-reported total times |
| `execution` | Query execution times |
| `compilation` | Query compilation times |
| `rows` | Number of rows returned |
| `message` | Error message (if applicable) |

### Result Analysis

```python
import pandas as pd

# Load results
df = pd.read_csv('results/duckdb/tpch_sf1.csv')

# Calculate median execution times
df['median_time'] = df['client_total'].apply(
    lambda x: pd.Series(eval(x)).median()
)

# Group by system
summary = df.groupby('title')['median_time'].agg(['mean', 'sum', 'count'])
print(summary)
```

## Project Structure

```
OLAPBench/
├── benchmark.py              # Main benchmark runner
├── benchmark.sh              # Shell wrapper script
├── setup.sh                  # Environment setup
├── requirements.txt          # Python dependencies
├── test.py                   # Test runner
├── benchmarks/               # Benchmark implementations
│   ├── benchmark.py          # Base benchmark class
│   ├── tpch/                 # TPC-H benchmark
│   │   ├── tpch.py
│   │   ├── tpch.dbschema.json
│   │   ├── dbgen.sh
│   │   └── queries/          # Query files
│   ├── clickbench/           # ClickBench benchmark
│   ├── tpcds/                # TPC-DS benchmark
│   ├── job/                  # Join Order Benchmark
│   ├── ssb/                  # Star Schema Benchmark
│   └── stackoverflow/        # StackOverflow benchmark
├── dbms/                     # Database system implementations
│   ├── dbms.py               # Base DBMS class
│   ├── duckdb.py             # DuckDB implementation
│   ├── postgres.py           # PostgreSQL implementation
│   ├── clickhouse.py         # ClickHouse implementation
│   └── ...                   # Other database systems
├── docker/                   # Docker configurations
│   ├── duckdb/
│   │   ├── Dockerfile
│   │   └── server.py
│   ├── postgres/
│   └── ...
├── queryplan/                # Query plan analysis
│   ├── queryplan.py
│   ├── parsers/              # Database-specific parsers
│   └── encoder/              # Plan serialization
├── util/                     # Utility modules
│   ├── logger.py             # Logging framework
│   ├── formatter.py          # Output formatting
│   ├── process.py            # Process management
│   └── ...
├── schemas/                  # JSON schemas
├── test/                     # Test configurations
└── data/                     # Generated benchmark data
```

## Adding New Systems

> **⚠️ Important**: New database systems must be registered in `dbms/dbms.py` in the `database_systems()` function to be recognized by the framework.

To add support for a new database system, you need to create the implementation and register it in the framework:

1. **Create DBMS implementation** (`dbms/newsystem.py`):

```python
from dbms.dbms import DBMS, Result, DBMSDescription

class NewSystem(DBMS):
    def __init__(self, benchmark, db_dir, data_dir, params, settings):
        super().__init__(benchmark, db_dir, data_dir, params, settings)
    
    @property
    def name(self) -> str:
        return "newsystem"
    
    def _connect(self, port: int):
        # Implement connection logic
        pass
    
    def _create_schema(self):
        # Implement schema creation
        pass
    
    def _load_data(self):
        # Implement data loading
        pass
    
    def _execute_query(self, query: str) -> Result:
        # Implement query execution
        pass

class NewSystemDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return "newsystem"
    
    @staticmethod
    def instantiate(benchmark, db_dir, data_dir, params, settings):
        return NewSystem(benchmark, db_dir, data_dir, params, settings)
```

2. **Create Docker configuration** (`docker/newsystem/`):

```dockerfile
FROM newsystem:latest
COPY entrypoint.sh /entrypoint.sh
EXPOSE 5432
ENTRYPOINT ["/entrypoint.sh"]
```

3. **Register the new system** in `dbms/dbms.py`:

Add the import and include it in the `database_systems()` function:

```python
def database_systems() -> Dict[str, DBMSDescription]:
    from dbms import apollo, cedardb, clickhouse, duckdb, hyper, monetdb, postgres, singlestore, sqlserver, umbra, umbradev, newsystem

    dbms_list = [
        apollo.ApolloDescription, cedardb.CedarDBDescription, clickhouse.ClickHouseDescription,
        duckdb.DuckDBDescription, hyper.HyperDescription, monetdb.MonetDBDescription,
        postgres.PostgresDescription, singlestore.SingleStoreDescription, sqlserver.SQLServerDescription,
        umbra.UmbraDescription, umbradev.UmbraDevDescription, newsystem.NewSystemDescription
    ]
    return {dbms.get_name(): dbms for dbms in dbms_list}
```

4. **Update schema** (`schemas/benchmark.schema.json`):

```json
{
  "dbms": {
    "enum": [..., "newsystem"]
  }
}
```

## Adding New Benchmarks

> **⚠️ Important**: New benchmarks must be registered in `benchmarks/benchmark.py` in the `benchmarks()` function to be recognized by the framework.

To add a new benchmark, you need to create the implementation and register it in the framework:

1. **Create benchmark implementation** (`benchmarks/newbench/newbench.py`):

```python
from benchmarks.benchmark import Benchmark, BenchmarkDescription

class NewBench(Benchmark):
    @property
    def name(self) -> str:
        return "newbench"
    
    @property
    def description(self) -> str:
        return "New Benchmark Description"
    
    def dbgen(self):
        # Implement data generation
        pass

class NewBenchDescription(BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "newbench"
    
    @staticmethod
    def instantiate(base_dir: str, args: dict):
        return NewBench(base_dir, args)
```

2. **Create schema definition** (`benchmarks/newbench/newbench.dbschema.json`):

```json
{
  "file_ending": "csv",
  "delimiter": ",",
  "format": "text",
  "tables": [
    {
      "name": "table1",
      "columns": [
        {"name": "id", "type": "INTEGER PRIMARY KEY"},
        {"name": "name", "type": "VARCHAR(255)"}
      ]
    }
  ]
}
```

3. **Add queries** (`benchmarks/newbench/queries/`):
   - Create SQL files: `1.sql`, `2.sql`, etc.
   - Each file contains one query

4. **Register the new benchmark** in `benchmarks/benchmark.py`:

Add the import and include it in the `benchmarks()` function:

```python
def benchmarks() -> dict[str, BenchmarkDescription]:
    from benchmarks.clickbench import clickbench
    from benchmarks.job import job
    from benchmarks.ssb import ssb
    from benchmarks.tpcds import tpcds
    from benchmarks.tpch import tpch
    from benchmarks.stackoverflow import stackoverflow
    from benchmarks.newbench import newbench

    benchmark_list = [
        clickbench.ClickBenchDescription,
        job.JOBDescription,
        ssb.SSBDescription,
        stackoverflow.StackOverflowDescription,
        tpcds.TPCDSDescription,
        tpch.TPCHDescription,
        newbench.NewBenchDescription
    ]
    return {benchmark.get_name(): benchmark for benchmark in benchmark_list}
```

5. **Create data generation script** (`benchmarks/newbench/dbgen.sh`):

```bash
#!/bin/bash
# Generate benchmark data
echo "Generating NewBench data..."
```

## Examples

### Example Configurations

The `test/` directory contains example configurations for most supported systems. You can use these as templates:

- `test/duckdb.benchmark.yaml` - DuckDB with version matrix
- `test/postgres.benchmark.yaml` - PostgreSQL with multiple versions
- `test/clickhouse.benchmark.yaml` - ClickHouse configuration
- `test/hyper.benchmark.yaml` - Hyper configuration
- `test/singlestore.benchmark.yaml` - SingleStore configuration
- `test/sqlserver.benchmark.yaml` - SQL Server configuration
- `test/umbra.benchmark.yaml` - Umbra configuration
- `test/umbradev.benchmark.yaml` - Umbra development version
- `test/apollo.benchmark.yaml` - Apollo configuration

**Note**: Some supported systems (MonetDB, CedarDB) don't have example test configurations yet.

### Custom Benchmark

Create a custom benchmark configuration:

```yaml
title: "Custom Performance Comparison"
repetitions: 5
warmup: 2
timeout: 600
global_timeout: 3600
output: "results/custom/"

systems:
  - title: "DuckDB Latest"
    dbms: duckdb
    parameter:
      version: ["latest"]
  
  - title: "PostgreSQL 16"
    dbms: postgres
    parameter:
      version: ["16.0"]
    settings:
      shared_buffers: "2GB"
      work_mem: "256MB"

benchmarks:
  - name: tpch
    scale: 1
    included_queries: ["1", "3", "5", "6", "10"]
  
  - name: clickbench
    included_queries: ["0", "1", "2", "3", "4"]
```

Run the custom benchmark:

```bash
./benchmark.sh --verbose custom.benchmark.yaml
```

## Contributing

Contributions are welcome! Please:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Add tests** for new functionality
4. **Follow code style** guidelines
5. **Submit a pull request**

### Development Setup

```bash
# Clone and setup
git clone https://github.com/SQL-Storm/OLAPBench.git
cd OLAPBench
./setup.sh
source .venv/bin/activate

# Run tests
python test.py

# Run specific test
./benchmark.sh test/duckdb.benchmark.yaml
```

### Code Guidelines

- Follow Python PEP 8 style guidelines
- Add docstrings for public methods
- Include type hints where appropriate
- Write unit tests for new functionality
- Update documentation for new features

---

## Related Projects

- **SQLStorm**: Query performance analysis framework - https://github.com/SQL-Storm/SQLStorm
- **TPC Benchmarks**: Official TPC benchmark specifications - http://www.tpc.org/
- **ClickBench**: ClickHouse benchmark suite - https://github.com/ClickHouse/ClickBench

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- TPC Council for benchmark specifications
- Database system developers and communities
- Academic research on query performance analysis
- Open source contributors and maintainers

---

For more examples and advanced configurations, see the [SQLStorm examples](https://github.com/SQL-Storm/SQLStorm/tree/master/definitions/v1.0) directory.
