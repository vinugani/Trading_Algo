# 🚀 Algo Trading Bot

## 1. Project Overview

* Project Name: Delta Exchange Algo Trading Bot
* Purpose: Automated crypto trading using strategies (scalping / futures)
* Exchange: Delta Exchange India
* Modes Supported:
  * paper (simulation)
  * testnet (sandbox trading)
  * live (real trading)

---

## 2. Environment Configuration

### API Base URLs

* **Production (Live)**
  * REST: [https://api.india.delta.exchange](https://api.india.delta.exchange/)
  * WebSocket: wss://socket.india.delta.exchange
* **Testnet**
  * REST: [https://cdn-ind.testnet.deltaex.org](https://cdn-ind.testnet.deltaex.org/)
  * WebSocket: wss://cdn-ind.testnet.deltaex.org

### Current Mode

* Active Mode: `testnet` # change via --mode

---

## 3. Credentials Handling

* Stored in `.env`
* Required Keys:
  * DELTA\_API\_KEY
  * DELTA\_API\_SECRET
* Important:
  * NEVER hardcode credentials
  * Ensure testnet keys are used in test mode

---

## 4. Database Configuration

* Database: PostgreSQL
* SQLite: ❌ Removed completely

### Connection

* Managed via:
  * SQLAlchemy / asyncpg (if async)
* Ensure:
  * No `state.db` files exist
  * WAL/SHM files are not generated

---

## 5. Trading Configuration

### Risk Management

* Max Risk Per Trade: 1%
* Max Daily Loss: 5%
* Max Leverage: 10x

### Capital

* Example: \$1000
* Strategy Type: Scalping (perpetual futures)

---

## 6. Order Execution Flow

1. Strategy generates signal
2. Risk manager validates
3. Order placed via REST API
4. Order tracked via WebSocket
5. Trade logged in DB

---

## 7. WebSocket Handling

* Used for:
  * Order updates
  * Position updates
  * Market data

### Known Issues (IMPORTANT)

* Reconnection failures
* Incorrect WS URL mapping
* Silent disconnects

### Fix Strategy

* Auto-reconnect with backoff
* Heartbeat/ping check
* Logging for disconnect events

---

## 8. Modes Behavior

| Mode    | Trades Executed | Real Money |
| --------- | ----------------- | ------------ |
| paper   | simulated       | ❌ No      |
| testnet | real API        | ❌ No      |
| live    | real API        | ✅ Yes     |

---

## 9. Preflight Checks

Run before live/testnet:

```bash
poetry run python scripts/live_preflight.py
```

Checks:

* API connectivity
* Credentials present
* Correct base URL
* Risk settings valid

---

## 10. Key Scripts

### Analyze Trades

```bash
poetry run python scripts/analyze_paper_trades.py --mode <mode> --lookback-days <days>
```

### Run Bot

```bash
poetry run python main.py --mode testnet
```

---

## 11. Logging

* Ensure logs include:
  * Order placement
  * WebSocket events
  * Errors
  * Reconnect attempts

---

## 12. Known Problems / To Verify

* [ ] Ensure testnet is NOT executing live trades
* [ ] Verify WebSocket stability after fixes
* [ ] Confirm PostgreSQL fully replaces SQLite
* [ ] Validate mode switching logic

---

## 13. Safety Rules (CRITICAL)

* Never run live mode without:
  * Verified testnet results
  * Risk limits enforced
* Always double-check:
  * API URLs
  * Mode flag
  * Environment variables

---

## 14. Future Improvements

* Add strategy backtesting module
* Add dashboard (PnL, trades)
* Add alerting system (Telegram/Slack)
* Improve execution latency

---

