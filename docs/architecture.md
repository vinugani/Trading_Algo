# Architecture Overview

- API layer: `delta_exchange_bot.api` (REST + WebSocket wrapper)
- Data layer: `delta_exchange_bot.data` (market data loading, smoothing)
- Strategy layer: `delta_exchange_bot.strategy` (signals)
- Execution layer: `delta_exchange_bot.execution` (order manager)
- Risk layer: `delta_exchange_bot.risk`
- State layer: `delta_exchange_bot.persistence`
- Orchestration: `delta_exchange_bot.core`
- CLI: `delta_exchange_bot.cli`

## Core workflow
1. `TradingEngine.run()` fetches market snapshot
2. `strategy.generate()` returns signals
3. `risk_manager.assess()` passes or blocks
4. `order_manager.place_order()` and persistence stores trades
