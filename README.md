# Delta Exchange Crypto Trading Bot

Production-grade architecture for trading on Delta Exchange API.

## Setup

1. `poetry install`
2. Copy `.env.example` to `.env`; configure credentials
3. `poetry run python scripts/run_bot.py --mode paper --strategy portfolio`

## Safe Live Start

1. Run preflight checks:
   - `poetry run python scripts/live_preflight.py`
2. If preflight passes, start live bot:
   - `poetry run python scripts/run_bot.py --mode live --strategy portfolio`

## Structure

- `src/delta_exchange_bot/api`: Delta API wrappers
- `src/delta_exchange_bot/data`: market data ingestion
- `src/delta_exchange_bot/strategy`: strategy engine
- `src/delta_exchange_bot/execution`: order management
- `src/delta_exchange_bot/risk`: risk checks
- `src/delta_exchange_bot/persistence`: state/DB
- `src/delta_exchange_bot/core`: runner/orchestration
- `tests`: unit/integration tests
