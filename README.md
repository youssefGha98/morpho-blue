# Morpho Blue Analytics

A Python-based analytics framework for analyzing Morpho Blue lending protocol data. This project provides tools to process blockchain events, compute market indicators, and analyze lending/borrowing dynamics.

## Features

- 🔍 **Event Processing**: Standardize and transform Morpho Blue blockchain events
- 📊 **Market Indicators**: Calculate key metrics like utilization rates, APY, health factors
- 🏦 **Ledger Management**: Track user positions and market states over time
- 📈 **State Enrichment**: Compute cumulative positions and time-series analytics
- 💾 **Parquet Storage**: Efficient data storage and querying using Apache Parquet
- 🎯 **Attribution Analysis**: NEW - Decompose utilization dynamics into flow contributions
- 📉 **IRM Diagnostics**: NEW - Analyze Interest Rate Model responsiveness and slope
- ⏱️ **Extended Rolling Windows**: NEW - Metrics from 5min to 365d with volatility attribution

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
# Basic market indicators
rye run jupyter notebook notebooks/01_market_indicators_demo.ipynb

# Attribution features (NEW)
rye run jupyter notebook notebooks/02_attribution_features_demo.ipynb
```

### Using the Indicator Service

```python
from morpho_blue.indicator_service import IndicatorService
from morpho_blue.data.parquet_repository import ParquetRepository
from pathlib import Path
import duckdb

# Configure paths
DATA_ROOT = Path("data_examples")
market_id = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"

# Initialize DuckDB connection and repository
con = duckdb.connect(":memory:")
repo = ParquetRepository(con=con, root=str(DATA_ROOT))
service = IndicatorService(repo=repo, con=con)

# Option 1: Build standard indicator dataset
indicators_table = service.build_market_dataset(market_id=market_id)

# Option 2: Build attribution dataset (fetches data automatically)
attribution = service.build_attribution_dataset(
    market_id=market_id,
    validate=True,
)

# Option 3: Reuse enriched ledger (more efficient, easier to test)
# Build ledger once
ledger = service.build_enriched_ledger(market_id=market_id)

# Use it for both indicators and attribution
indicators_table = service.build_market_dataset(market_id=market_id)
attribution = service.build_attribution_dataset(ledger=ledger, validate=True)

# Access as pandas
df = attribution.table.to_pandas()
print(df[['utilization_rate', 'contrib_u_from_borrow', 'contrib_u_from_withdraw']].head())
```

### Attribution Analysis Example

```python
from morpho_blue.transformations import compute_attribution_features, AttributionWindowSpec

# Direct usage: Compute attribution from an enriched ledger
attribution_table = compute_attribution_features(
    ledger=enriched_ledger,
    windows=AttributionWindowSpec(),  # 5m, 1h, 6h, 24h, 7d, 30d, 90d, 365d
)

# Analyze utilization contributions
df = attribution_table.to_pandas()

# Which flows are driving utilization changes?
print("Utilization contribution from withdrawals (6h):", df['contrib_withdraw_sum_6h'].iloc[-1])
print("Utilization contribution from borrows (6h):", df['contrib_borrow_sum_6h'].iloc[-1])

# IRM responsiveness
print("IRM slope (ΔAPR/Δu):", df['irm_slope'].mean())
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

### Attribution Features (NEW)
Advanced analytics layer that decomposes utilization dynamics:

**Flow Decomposition**: Break down each event into principal components:
- `borrow_in_assets`, `repay_out_assets`, `liquidate_repay_assets`
- `supply_in_assets`, `withdraw_out_assets`, `interest_assets`

**Utilization Attribution**: Quantify how each flow contributes to Δu:
- `contrib_u_from_borrow`, `contrib_u_from_repay`, `contrib_u_from_liquidate`
- `contrib_u_from_withdraw`, `contrib_u_from_supply`, `contrib_u_from_interest`

**IRM Diagnostics**: Measure interest rate model responsiveness:
- `irm_slope`: ΔAPR / Δu
- `irm_response_to_withdraw`, `irm_response_to_repay`, etc.

**Rolling Windows**: Comprehensive time-based aggregations:
- Windows: 5min, 1h, 6h, 24h, 7d, 30d, 90d, 365d
- Metrics: means, maxes, intensities, volatilities, attribution sums
- Volatility attribution shares: Decompose variance by flow type

**Integrity Checks**: Accounting residuals validate data quality:
- `delta_u_residual` = actual Δu - predicted Δu from contributions

For more details on the attribution features, see the [attribution demo notebook](notebooks/02_attribution_features_demo.ipynb).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

- **Author**: YoussefGha98
- **Email**: gharbi.youssef.619@gmail.com

## Acknowledgments

Built for analyzing [Morpho Blue](https://morpho.org/) lending protocol data.
