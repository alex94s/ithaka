# ithaka

## Table of Contents
1. [Description](#description)
2. [Requirements](#requirements)
3. [Modules](#modules)
4. [Testing](#testing)
5. [Version and Updates](#version-and-updates)
6. [Disclaimer](#disclaimer)

## Description
**ithaka** is a quantitative trading framework structured around two key types of modules: **core** and **strategies**. The core modules (`factory`, `strategy`, `tracker`, `utils`) handle data downloads, strategy warehousing, and portfolio management operations. The strategies modules (`bam`, `cta`, `emm`, `newt`, `stab`, `far`) are instances of systematic trading strategies configured to generate daily levels and target constituent weights for downstream integration into the core system. The framework is designed for easy expansion, allowing for the addition of new strategy modules as needed. Execution can be configured as either semi or fully automated through the Interactive Brokers (IBKR) API.

## Requirements
The following requirements must be met prior to running the ithaka modules:

1. **Python Environment**:
   - Ensure all dependent packages are installed (`requirements.txt`).
2. **Database**:
   - A MySQL database with set environment variables for database credentials.
3. **Interactive Brokers Account**:
   - A live account with IBKR.
   - Authentication through IB Gateway.
4. **Market Data Subscriptions**:
   - CFE Enhanced (NP, L1)
   - PAXOS Cryptocurrency
   - US Securities Snapshot and Futures Value Bundle (NP, L1)
   - UK LSE Equities (NP, L1)

### Environment Setup
The following steps may be taken to set up the environment for this project:

**1. Create a Virtual Environment**
#### On macOS/Linux:
```bash
python3 -m venv env
source env/bin/activate
```
#### On Windows:
```bash
python -m venv env
env\Scripts\activate
```

**2. Install Project Dependencies**
```bash
pip install -r requirements.txt
```

**3. Set Environment Variables**
- **`DB_HOST`**: The hostname or IP address of the database server.
- **`DB_USER`**: The username for database authentication.
- **`DB_PASSWORD`**: The password for the specified database user.
- **`DB_NAME`**: The name of the database to connect to.
- **`PYTHONPATH`**: The path to the **`/src`** folder for Python to locate project modules.

Running the following commands in the terminal **within the project environment** will create the necessary variables:

#### On macOS/Linux:
```bash
export DB_HOST='your_host'
export DB_USER='your_user'
export DB_PASSWORD='your_password'
export DB_NAME='your_database'
export PYTHONPATH='your_folder_path_to_src'
```
#### On Windows:
```bash
setx DB_HOST "your_host"
setx DB_USER "your_user"
setx DB_PASSWORD "your_password"
setx DB_NAME "your_database"
setx PYTHONPATH="your_folder_path_to_src"
```

### VS Code Configuration (Optional)
The following VS Code settings are recommended for use by amending `.vscode/settings.json`:

```json
{
    "terminal.integrated.env.osx": {
        "DB_HOST": "your_host",
        "DB_USER": "your_user",
        "DB_PASSWORD": "your_password",
        "DB_NAME": "your_database"
    },
    "terminal.integrated.env.windows": {
        "DB_HOST": "your_host",
        "DB_USER": "your_user",
        "DB_PASSWORD": "your_password",
        "DB_NAME": "your_database"
    },
    "python.autoComplete.extraPaths": [
        "your_folder_path_to_src"
    ]
}
```

## Modules

### Main Module
- **File**: `main.py`
- **Purpose**: The Main module orchestrates the execution of other core modules (`tracker`, `factory`, and strategies modules like `bam`, `cta`, `emm`, `newt`, `stab`, `far`) to ensure the portfolio is rebalanced according to target allocations.
- **Features**:
   - **Dash App**: Tracks live strategy calculations, shows current positions, and prompts required trades to maintain portfolio exposure.

### Factory Module
- **File**: `factory.py`
- **Purpose**: Manages the uploading of historical price data, strategy levels, and constituent weights to a MySQL database.
- **Classes**:
   - `DataManager`: Manages data connections, retrieval, and commits for productionised strategies.
- **Features**:
   - Interacts with the IBKR and Yahoo Finance APIs to download price data.
   - Utilizes strategy modules (BAM, CTA, EMM, NEWT, STAB, FAR) to compute and upload strategy levels and weights.

### Strategy Module
- **File**: `strategy.py`
- **Purpose**: Defines the `Strategy` abstract base class as a template for creating different trading strategies, ensuring consistent implementation of key methods across strategies.
- **Features**:
  - Defines a class-based structure for building and managing trading strategies.
  - Enforces implementation of core methods for subclasses.

### Tracker Module
- **File**: `tracker.py`
- **Purpose**: Compares current portfolio holdings against target positions and generates trade orders to be executed via the IBKR API.
- **Features**:
   - Facilitates portfolio rebalancing to align with target allocations.

### Utils Module
- **File**: `utils.py`
- **Purpose**: Provides a set of utility functions for data manipulation, portfolio calculations, and common operations across the project.
- **Features**:
  - Includes functions for data transformations and calculating performance metrics.
  - Contains helper functions for retrieving data from MySQL database.

### BAM Module
- **File**: `bam.py`
- **Purpose**: Computes strategy levels and constituents for a long-only investment strategy that tactically trades into cash during periods of increased risk as identified using a price momentum filter.
- **Classes**:
   - `BAMStrategy`: Orchestrates the retrieval of the BAM strategy's signals, returns, and constituent weights based on specified parameters.
- **Strategy Overview**: BAM is a Balanced Asset Model for gaining long-only exposure to differentiated asset class 'betas' which have historically shown positive risk premia. This take on the All Weather portfolio incorporates a non-traditional asset class in crypto, as well as a time-series overlay to minimize large drawdowns. The strategy operates at a low-frequency with a minimum monthly holding period and equal risk contribution reweighting on a semi-annual basis. The trade frequency and instruments used allow the strategy to be managed in a tax-free ISA which is compliant with PTA requirements.
- **Instruments Traded**:
  - **ETFs**: LSE-listed UCITS ETFs tracking differentiated asset classes (US Equities, US Treasuries, US High Yield Corps, GSCI, Gold).
  - **Cryptocurrency**: Since cryptocurrency ETFs are not currently allowed as part of the UCITS directive, exposure to spot BTC is required.
- **Output**: Target/effective constituent weights and historical levels.

### CTA Module
- **File**: `cta.py`
- **Purpose**: Computes strategy levels and constituents for futures trading strategies based on momentum, autocorrelation, and seasonality effects in key asset classes.
- **Classes**:
   - `CTAStrategy`: Orchestrates the retrieval of the CTA strategy's signals, returns, and constituent weights based on specified parameters.
- **Strategy Overview**: CTA is an alternative take on the industry standard for strategies managed by Commodity Trading Advisors. Although time-series momentum is utilised, the strategy differentiates itself by targeting more specific anomalies in selected futures markets. This allows for more streamlined use of capital to avoid repeated exposure to correlated signals, as well as minimizing exposure to noisy signals (as observed by certain asset classes historically). The strategy signals operate on a mid-term/interday frequency - as such, signals must be acted upon either as soon as they are registered during market close, or at the subsequent day's market open. Both leverage and shorting are utilised, with a target volatility of 20% set for each individual sub-strategy. The leverage ratio is capped at 20 regardless of whether the target volatility is achieved. Seven sub-strategies are currently managed as part of this module with equal-weight rebalancing conducted on a semi-annual basis. 
- **Instruments Traded**: 
  - **Futures**: Liquid futures with manageable contract notionals that have been specified for each sub-strategy (S&P 500, 30 Day Federal Funds, Cotton, Soybean, Corn, Heating Oil, Natural Gas, DX, VIX, Housing, SDA).
- **Output**: Target/effective constituent weights and historical levels.

### EMM Module
- **File**: `emm.py`
- **Purpose**: Computes strategy levels and constituents for a long-only momentum strategy targeting equities in emerging markets. Includes a risk regime indicator to tactically short equity index futures during heightened volatility.
- **Classes**:
   - `EMMStrategy`: Orchestrates the retrieval of the EMM strategy's signals, returns, and constituent weights based on specified parameters.
- **Strategy Overview**: EMM is an Emerging Markets Momentum strategy which purchases the 25 stocks with the highest risk-normalised momentum score in equal weight for the relevant universe. A tactical risk-overlay is utilised to signal shorting equity index futures to achieve market neutrality in periods of heightened risk. The strategy is rebalanced on a semi-annual basis.
- **Instruments Traded**: 
  - **Equities**: NSE-listed equities which are members of the NIFTY 500 index. Please note that stocks listed on an Indian exchange cannot be traded directly by foreigners without regulatory approval. Investors participating in the Foreign Portfolio Investor (FPI) regime, or Non-Resident Indians (NRIs) are eligible for approval.
  - **Futures**: Liquid equity index and currency futures used for hedging purposes (S&P 500, Indian Rupee/USD).
- **Output**: Target/effective constituent weights and historical levels.

### NEWT Module
- **File**: `newt.py`
- **Purpose**: Computes strategy levels for an intraday equity trading strategy that dynamically responds to news headlines. The strategy integrates the Squawker package to generate actionable signals from curated RSS feeds.
- **Classes**:
   - `NEWTStrategy`: Orchestrates the retrieval of the NEWT strategy's signals and returns.
- **Strategy Overview**: 
  - NEWT (News Trader) is a tactical trading strategy which aims to capitalize on volatility and momentum in the hours following idiosyncratic news releases. The strategy leverages the Squawker package to filter and interpret high-conviction headlines generating actionable buy/sell signals for publicly traded equities.
- **Instruments Traded**:
  - **Equities**: Publicly traded stocks listed on major exchanges.
- **Output**: Historical levels.

### STAB Module
- **File**: `stab.py`
- **Purpose**: Computes strategy levels and constituents for a long-short trading strategy based on identifying short-term mean reversion opportunities in US Small Cap Equities.
- **Classes**:
   - `STABStrategy`: Orchestrates the retrieval of the STAB strategy's signals, returns, and constituent weights based on specified parameters.
- **Strategy Overview**: 
  - STAB uses Statistical Arbritrage to identify short-term mean-reversion signals for a universe of tickers assigned to a cluster based on the historical correlation matrix of daily returns. The strategy relies on the probabilistic likelihood for stocks which have historically shown high correlations to revert to the mean performance of its peer group.
- **Instruments Traded**:
  - **Equities**: Constituents of the S&P SmallCap 600 Index.
- **Output**: Target/effective constituent weights and historical levels.

### FAR Module
- **File**: `far.py`
- **Purpose**: Computes strategy levels and constituents for a long-short trading strategy based on identifying short-term mean reversion opportunities in Fallen Angels vs High Yield Bond ETFs.
- **Classes**:
   - `FARStrategy`: Orchestrates the retrieval of the FAR strategy's signals, returns, and constituent weights based on specified parameters.
- **Strategy Overview**: 
  - FAR is a Fallen Angels Reversion strategy based on the historical tendency for corporate bonds which have been downgraded from Investment Grade to High Yield to show similar yield characteristics to the higher yielding segment whilst exhibiting improved quality characteristics. The strategy exploits this effect by identifying periods of relative cheapness of Fallen Angel bonds relative to the High Yield universe, and vice-versa. Offsetting dollar-neutral positions are taken simultaneously to minimize directional market exposure.
- **Instruments Traded**:
  - **ETFs**: LSE-listed UCITS ETFs for Fallen Angel and High Yield Bonds.
- **Output**: Target/effective constituent weights and historical levels.

## Testing
A suite of unit tests are located in the `tests` folder. Each script targets a specific module, ensuring code reliability and functionality:

- **`test_bam.py`**: Unit tests for the BAM module.
- **`test_cta.py`**: Unit tests for the CTA module.
- **`test_emm.py`**: Unit tests for the EMM module.
- **`test_factory.py`**: Unit tests for the Factory module.
- **`test_newt.py`**: Unit tests for the NEWT module.
- **`test_stab.py`**: Unit tests for the STAB module.
- **`test_far.py`**: Unit tests for the FAR module.
- **`test_tracker.py`**: Unit tests for the Tracker module.
- **`test_utils.py`**: Unit tests for the Utils module.

The following command may be used to run all tests:

```bash
python -m unittest discover tests
```

## Version and Updates
- **Last Update**: February 23rd, 2025
- **Version**: 1.0.6

Check the [CHANGELOG](CHANGELOG.md) for detailed updates and version history.

## Disclaimer
This package is currently in the development phase and has not been thoroughly tested. The strategies included are for illustrative purposes only and should not be interpreted as financial advice. This project is shared primarily to foster collaboration and inspire the sharing of ideas.