# ResQDB

## Overview

`resqdb` is a Python package designed to run energy system simulations using `oemof-tabular` and upload the results to a PostgreSQL database.
It automates the process of simulating energy scenarios defined in data packages and storing both scalar results and time-series flows in a structured database format,
allowing for further analysis and visualization.

Key features include:
- Simulation of energy systems defined in `oemof-tabular` data packages.
- Automated database schema setup and management using SQLAlchemy.
- Support for storing scenario-specific results (scalars and flows).
- Geographical mapping of results to clusters.
- Automated creation of materialized views in DB for further usage in visualization tool (i.e. Apache Superset)

## Usage

### Prerequisites

- Python 3.13 or higher.
- A PostgreSQL database with the PostGIS extension enabled.
- [uv](https://github.com/astral-sh/uv) for dependency management (recommended).

### Installation

Clone the repository and install the dependencies using `uv`:

```bash
uv sync
```

### Configuration

The application is configured via environment variables. Create a `.env` file in the root directory with the following variables:

```env
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_db_name
DB_SCHEMA=resqenergy
OEMOF_SCENARIO=investment
```

### Database Setup

Before running simulations, you need to set up the database schema and default data:

```bash
uv run models.py setup
```

To delete the database schema (CAUTION: this will remove all data):

```bash
uv run models.py delete
```

### Running Simulations

To run the main simulation and export the results to the database:

```bash
uv run main.py
```

This will:
1. Run an `oemof-tabular` simulation for the scenario specified in `main.py` (default: "investment").
2. Create a new scenario entry in the database.
3. Export simulation results (scalars and flows) to the database.
