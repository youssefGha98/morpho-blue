# Morpho Blue Analytics

A Python-based analytics framework for analyzing Morpho Blue lending protocol data. This project provides tools to process blockchain events, compute market indicators, and analyze lending/borrowing dynamics.

## Features

- 🔍 **Event Processing**: Standardize and transform Morpho Blue blockchain events
- 📊 **Market Indicators**: Calculate key metrics like utilization rates, APY, health factors
- 🏦 **Ledger Management**: Track user positions and market states over time
- 📈 **State Enrichment**: Compute cumulative positions and time-series analytics
- 💾 **Parquet Storage**: Efficient data storage and querying using Apache Parquet

## Architecture

This project follows clean architecture principles with clear separation of concerns:

- **Service Layer**: Orchestrates the full analytics pipeline
- **Domain Layer**: Core business models (events, ledgers, schemas)
- **Data Layer**: Parquet-based repository for efficient data access
- **Transformation Layer**: Event standardization, state enrichment, and feature engineering

## Installation

This project uses [Rye](https://rye.astral.sh/) for dependency management.

### Prerequisites

- Python 3.8 or higher
- Rye ([installation guide](https://rye.astral.sh/guide/installation/))

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd morpho_blue
```

2. Sync dependencies with Rye:
```bash
rye sync
```

3. Activate the virtual environment:
```bash
. .venv/bin/activate
```

## Usage

### Running the Notebooks

Explore the demo notebooks to see the analytics in action:

```bash
rye run jupyter notebook notebooks/01_market_indicators_demo.ipynb
```

### Using the Indicator Service

```python
from morpho_blue.indicator_service import IndicatorService

# Initialize the service with your data directory
service = IndicatorService(data_dir="data_examples/morpho/market")

# Compute market indicators
indicators = service.compute_market_indicators()
print(indicators)
```

### Computing Specific Indicators

```python
from morpho_blue.indicators import (
    compute_utilization_rate,
    compute_supply_apy,
    compute_borrow_apy,
    compute_health_factor
)

# Calculate utilization rate
utilization = compute_utilization_rate(
    total_supply_assets=1000000,
    total_borrow_assets=750000
)
```

## Project Structure

```
morpho_blue/
├── src/morpho_blue/
│   ├── indicator_service.py      # Main service orchestrator
│   ├── indicators.py              # Indicator calculation functions
│   ├── validation.py              # Data validation utilities
│   ├── data/
│   │   └── parquet_repository.py  # Parquet data access layer
│   ├── domain/
│   │   ├── market_events.py       # Event domain models
│   │   ├── market_ledger.py       # Ledger domain models
│   │   └── schemas.py             # Data schemas
│   └── transformations/
│       ├── event_standardization.py  # Event normalization
│       ├── state_enrichment.py       # State computation
│       └── feature_engineering.py    # Feature extraction
├── notebooks/                     # Jupyter notebooks for demos
├── data_examples/                 # Sample data for testing
└── LICENSE                        # MIT License
```

## Development

### Running Tests

```bash
rye run pytest
```

### Adding Dependencies

```bash
rye add <package-name>
```

### Code Formatting

```bash
rye run black src/
rye run isort src/
```

## Data Format

The project expects data in Parquet format organized by event type:

```
data/
├── manifests/
│   └── run_*.jsonl              # Run metadata
└── shards/
    ├── Supply/                  # Supply events
    ├── Borrow/                  # Borrow events
    ├── Withdraw/                # Withdraw events
    └── ...                      # Other event types
```

## Key Concepts

### Market Events
Events from Morpho Blue protocol including Supply, Borrow, Withdraw, Repay, Liquidate, etc.

### Market Ledger
Aggregated view of user positions tracking cumulative supplies, borrows, and collateral.

### Indicators
Calculated metrics such as:
- Utilization Rate
- Supply/Borrow APY
- Health Factor
- Total Value Locked (TVL)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

- **Author**: YoussefGha98
- **Email**: gharbi.youssef.619@gmail.com

## Acknowledgments

Built for analyzing [Morpho Blue](https://morpho.org/) lending protocol data.
