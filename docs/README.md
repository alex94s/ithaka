# ithaka

**ithaka** is a modular Python framework for researching, backtesting, and operating systematic trading strategies across asset classes.

It is designed around a small, stable core and a growing set of strategy modules, making it suitable for both exploratory research and live portfolio tracking.

---

## Features

- Modular strategy architecture
- Shared infrastructure for data ingestion, portfolio tracking, and execution
- Support for long-only, long/short, futures, and crypto strategies
- Optional semi- or fully-automated execution via Interactive Brokers (IBKR)
- Designed for reproducible research and extensibility

---

## Project Structure

```text
ithaka/
├── src/
│   ├── core/          # Shared infrastructure
│   ├── strategies/    # Strategy implementations
│   └── main.py        # Orchestration & dashboard
├── tests/
├── pyproject.toml
└── README.md
```

---

## Installation

### 1. Create a virtual environment

```bash
python -m venv env
source env/bin/activate
```

---

### 2. Install the package

```bash
pip install -e .
```

This installs **ithaka** in editable mode and ensures imports work consistently across scripts, tests, and IDEs.

The `pyproject.toml` file is the authoritative source for dependencies and package configuration.

---

## Configuration

The framework expects access to a MySQL database and (optionally) an Interactive Brokers account.

Set the following environment variables:

```bash
export DB_HOST="your_host"
export DB_USER="your_user"
export DB_PASSWORD="your_password"
export DB_NAME="your_database"
```

---

## Core Components

### Core
The `core` package provides shared infrastructure:

- `strategy` — abstract base class for all strategies
- `factory` — data ingestion and persistence
- `tracker` — portfolio tracking and rebalancing logic
- `utils` — shared helper functions

---

### Strategies
Each strategy is implemented as a self-contained module that produces:

- historical strategy levels
- target and effective constituent weights

Current strategies include:

- **BAM** — Balanced Asset Model with momentum-based risk overlay
- **CTA** — Futures-based momentum, autocorrelation, and seasonality strategies
- **EMM** — Emerging markets equity momentum
- **NEWT** — Intraday news-driven equity trading
- **STAB** — Statistical arbitrage in US small-cap equities
- **FAR** — Fallen Angels vs High Yield bond relative value

---

## Execution & Monitoring

The `main.py` entry point coordinates strategy execution and portfolio tracking.

A Dash-based dashboard provides visibility into:

- live strategy calculations
- current portfolio holdings
- required trades to maintain target exposures

---

## Testing

Run the full test suite with:

```bash
python -m unittest discover tests
```

---

## Versioning

- **Current version**: 1.0.6  
- See `CHANGELOG.md` for release history.

---

## Disclaimer

This project is provided for research and educational purposes only.

It is under active development and has not been audited or production-hardened.  
Nothing in this repository constitutes investment advice.