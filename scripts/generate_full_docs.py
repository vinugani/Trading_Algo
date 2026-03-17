from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "full_project_documentation.html"


def collect_files(root: Path) -> list[Path]:
    patterns = [
        "src/**/*.py",
        "tests/unit/*.py",
        "config/*.yml",
        "scripts/*.py",
        "scripts/*.ps1",
        "docs/*.md",
    ]
    files: set[Path] = set()
    for pattern in patterns:
        files.update(p.resolve() for p in root.glob(pattern) if p.is_file())

    for fixed in [
        root / "README.md",
        root / "pyproject.toml",
        root / "requirements.txt",
        root / "Dockerfile",
        root / "docker-compose.yml",
        root / ".env.example",
    ]:
        if fixed.exists():
            files.add(fixed.resolve())

    return sorted(files, key=lambda p: str(p.relative_to(root)).replace("\\", "/"))


def classify(path: str) -> str:
    if path.startswith("src/delta_exchange_bot/api"):
        return "API Layer"
    if path.startswith("src/delta_exchange_bot/data"):
        return "Data Layer"
    if path.startswith("src/delta_exchange_bot/strategy"):
        return "Strategy Layer"
    if path.startswith("src/delta_exchange_bot/risk"):
        return "Risk Layer"
    if path.startswith("src/delta_exchange_bot/execution"):
        return "Execution Layer"
    if path.startswith("src/delta_exchange_bot/persistence"):
        return "Persistence Layer"
    if path.startswith("src/delta_exchange_bot/monitoring"):
        return "Monitoring Layer"
    if path.startswith("src/delta_exchange_bot/backtesting"):
        return "Backtesting Layer"
    if path.startswith("src/delta_exchange_bot/core"):
        return "Core Orchestration"
    if path.startswith("src/delta_exchange_bot/cli"):
        return "CLI / Entrypoints"
    if path.startswith("src/delta_exchange_bot/utils"):
        return "Utilities"
    if path.startswith("tests/"):
        return "Tests"
    if path.startswith("config/"):
        return "Config"
    if path.startswith("scripts/"):
        return "Run Scripts"
    if path.startswith("docs/"):
        return "Project Docs"
    return "Root / Build"


PURPOSE_MAP = {
    "src/delta_exchange_bot/api/delta_client.py": {
        "purpose": "Delta Exchange REST client with auth signing and endpoint wrappers.",
        "how": "Signs each authenticated request using HMAC and exposes market/order/account methods.",
        "change": "Edit here for API path/signature/payload updates.",
    },
    "src/delta_exchange_bot/data/market_data.py": {
        "purpose": "Public market-data fetchers returning pandas DataFrames.",
        "how": "Supports 1m/5m/15m candles, normalizes numeric/time fields, and sorts by timestamp.",
        "change": "Edit here for new intervals, lookback, schema normalization.",
    },
    "src/delta_exchange_bot/data/candle_builder.py": {
        "purpose": "Tick-to-OHLC candle builder.",
        "how": "Resamples timestamp+price to 1m/5m OHLC using pandas resample.",
        "change": "Edit here for more timeframes or aggregation behavior.",
    },
    "src/delta_exchange_bot/strategy/rsi_scalping.py": {
        "purpose": "RSI scalping strategy with SL/TP/trailing metadata.",
        "how": "LONG: RSI<30 and price>EMA20. SHORT: RSI>70 and price<EMA20.",
        "change": "Edit thresholds/periods/SL/TP/trailing defaults here.",
    },
    "src/delta_exchange_bot/risk/risk_management.py": {
        "purpose": "Risk sizing and validation policy.",
        "how": "Applies max risk per trade, max leverage, and max daily loss checks.",
        "change": "Edit limits/formulas here.",
    },
    "src/delta_exchange_bot/execution/order_execution_engine.py": {
        "purpose": "Order execution plus stop/tp/trailing trigger engine.",
        "how": "Places market/limit orders and executes reduce-only exits on protection triggers.",
        "change": "Edit routing, retries, trigger logic, and order-id format here.",
    },
    "src/delta_exchange_bot/persistence/db.py": {
        "purpose": "SQLite persistence for executions and open-position state.",
        "how": "Stores lifecycle rows with unique execution_id/client_order_id and trade_id linkage.",
        "change": "Edit schema/query methods here for audit/state changes.",
    },
    "src/delta_exchange_bot/cli/trading_bot.py": {
        "purpose": "Main one-minute trading loop.",
        "how": "Fetch data -> indicators -> signal -> risk check -> execute -> persist -> metrics.",
        "change": "Edit orchestration, loop behavior, strategy wiring, and mode handling here.",
    },
    "src/delta_exchange_bot/backtesting/engine.py": {
        "purpose": "Backtesting simulator and metrics engine.",
        "how": "Bar-by-bar simulation with protection exits and performance metrics.",
        "change": "Edit assumptions, fee model, and metric formulas here.",
    },
    "Dockerfile": {
        "purpose": "Container build/run recipe.",
        "how": "Python 3.11 slim, installs requirements.txt, runs scripts/run_bot.py.",
        "change": "Edit base image, install process, and CMD here.",
    },
}


def meta_for(path: str) -> dict[str, str]:
    if path in PURPOSE_MAP:
        return PURPOSE_MAP[path]
    if path.startswith("tests/"):
        return {
            "purpose": "Pytest coverage for runtime behaviors and regressions.",
            "how": "Verifies expected output and edge-cases for corresponding modules.",
            "change": "Update tests here when behavior changes.",
        }
    if path.startswith("config/"):
        return {
            "purpose": "Environment profile configuration values.",
            "how": "YAML defaults for mode/environment setup.",
            "change": "Edit profile values and align with Settings/.env.",
        }
    if path.startswith("scripts/"):
        return {
            "purpose": "Run helpers for local/dev execution.",
            "how": "Invokes CLI entrypoints with selected flags.",
            "change": "Edit commands/arguments here.",
        }
    return {
        "purpose": "Project source/config/build file.",
        "how": "Inspect this file in explorer section below.",
        "change": "Edit according to feature ownership.",
    }


def extract_symbols(content: str) -> list[str]:
    pattern = re.compile(r"^(class|def)\s+([A-Za-z_][A-Za-z0-9_]*)", re.M)
    return [f"{m.group(1)} {m.group(2)}" for m in pattern.finditer(content)]


def extract_snippet(content: str, symbol_name: str) -> str:
    lines = content.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if re.match(rf"^(class|def)\s+{re.escape(symbol_name)}\b", line):
            start = idx
            break
    if start is None:
        return ""
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if re.match(r"^(class|def)\s+[A-Za-z_][A-Za-z0-9_]*\b", lines[idx]):
            end = idx
            break
    return "\n".join(lines[start:end]).strip() + "\n"


def main() -> None:
    files = collect_files(ROOT)

    file_data: list[dict] = []
    module_data: list[dict] = []
    by_path: dict[str, dict] = {}

    for file in files:
        rel = file.relative_to(ROOT).as_posix()
        content = file.read_text(encoding="utf-8")
        symbols = extract_symbols(content)
        category = classify(rel)
        file_row = {
            "path": rel,
            "category": category,
            "symbols": symbols,
            "content": content,
        }
        file_data.append(file_row)
        by_path[rel] = file_row

        meta = meta_for(rel)
        module_data.append(
            {
                "path": rel,
                "category": category,
                "symbols": symbols,
                "purpose": meta["purpose"],
                "how": meta["how"],
                "change": meta["change"],
            }
        )

    def snippet(path: str, symbol: str, title: str, explain: str) -> dict:
        content = by_path.get(path, {}).get("content", "")
        return {
            "path": path,
            "title": title,
            "explain": explain,
            "code": extract_snippet(content, symbol),
        }

    snippets = [
        snippet(
            "src/delta_exchange_bot/strategy/rsi_scalping.py",
            "generate",
            "RSI Scalping Signal Generation",
            "Core long/short/hold rules and stop-loss/take-profit/trailing attachment.",
        ),
        snippet(
            "src/delta_exchange_bot/risk/risk_management.py",
            "calculate_position_size",
            "Risk-Based Position Sizing",
            "Size is capped by risk budget and leverage limit.",
        ),
        snippet(
            "src/delta_exchange_bot/risk/risk_management.py",
            "validate_trade",
            "Risk Validation Gate",
            "Trade-level guardrail check before execution.",
        ),
        snippet(
            "src/delta_exchange_bot/execution/order_execution_engine.py",
            "on_price_update",
            "Protection Trigger Engine",
            "Evaluates stop/tp/trailing and sends reduce-only exit orders.",
        ),
        snippet(
            "src/delta_exchange_bot/cli/trading_bot.py",
            "process_symbol",
            "Main 1-Minute Loop per Symbol",
            "Fetch data, evaluate signal, validate risk, and execute.",
        ),
        snippet(
            "src/delta_exchange_bot/persistence/db.py",
            "save_execution",
            "Unique Execution Logging",
            "Persists lifecycle rows while preventing duplicates.",
        ),
    ]

    # Filter empty snippet blocks if symbol not found
    snippets = [s for s in snippets if s["code"].strip()]

    files_json = json.dumps(file_data, ensure_ascii=False).replace("</", "<\\/")
    modules_json = json.dumps(module_data, ensure_ascii=False).replace("</", "<\\/")
    snippets_json = json.dumps(snippets, ensure_ascii=False).replace("</", "<\\/")
    generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Delta Exchange India Trading Bot - Full Project Documentation</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #1d2433;
      --muted: #5c667d;
      --line: #dfe5f1;
      --accent: #0059c9;
      --accent-soft: #e7f0ff;
      --good: #0a7a37;
      --warn: #ad5f00;
      --bad: #b00020;
      --code-bg: #0f172a;
      --code-ink: #e5eefc;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Verdana, sans-serif;
      color: var(--ink);
      background: radial-gradient(1200px 500px at 10% -10%, #dbe9ff 0%, transparent 60%), var(--bg);
    }}
    .layout {{ display: grid; grid-template-columns: 300px 1fr; min-height: 100vh; }}
    .sidebar {{
      position: sticky; top: 0; height: 100vh; overflow: auto;
      background: var(--panel); border-right: 1px solid var(--line); padding: 20px 16px;
    }}
    .brand {{ font-size: 18px; font-weight: 700; margin-bottom: 6px; color: #0b2a66; }}
    .stamp {{ font-size: 12px; color: var(--muted); margin-bottom: 16px; }}
    .nav a {{
      display: block; padding: 9px 10px; margin: 4px 0; border-radius: 10px;
      color: #22304a; text-decoration: none; font-size: 14px;
    }}
    .nav a:hover {{ background: var(--accent-soft); }}
    .content {{ padding: 26px; }}
    section {{
      background: var(--panel); border: 1px solid var(--line); border-radius: 16px;
      padding: 22px; margin-bottom: 18px; box-shadow: 0 8px 24px rgba(10, 28, 61, 0.04);
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 30px; color: #0e2557; }}
    h2 {{ font-size: 22px; color: #11316c; }}
    h3 {{ font-size: 17px; color: #16366f; }}
    p, li {{ line-height: 1.6; }}
    .callout {{
      border-left: 4px solid var(--warn); background: #fff8ec; padding: 12px 14px;
      border-radius: 10px; margin: 10px 0;
    }}
    .good {{ border-left-color: var(--good); background: #eefcf4; }}
    .bad {{ border-left-color: var(--bad); background: #fff0f3; }}
    .pill {{
      display: inline-block; padding: 3px 10px; border-radius: 999px;
      background: var(--accent-soft); color: #0b3f91; font-size: 12px;
      margin-right: 6px; margin-bottom: 6px; border: 1px solid #c8dbff;
    }}
    .grid2 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid var(--line); border-radius: 12px; padding: 14px; background: #fff; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border: 1px solid var(--line); padding: 9px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f5ff; color: #1a396d; }}
    code {{
      background: #eef2fb; padding: 1px 5px; border-radius: 6px;
      font-family: Consolas, Menlo, Monaco, monospace; font-size: 12.5px;
    }}
    pre {{
      margin: 0; white-space: pre; overflow: auto; padding: 14px; border-radius: 12px;
      background: var(--code-bg); color: var(--code-ink); font-size: 12.5px;
      border: 1px solid #223152; line-height: 1.45;
    }}
    .snippet {{ margin-bottom: 14px; }}
    .snippet-head {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; gap: 10px; flex-wrap: wrap; }}
    .btn {{
      border: 1px solid #b9c9ea; background: #fff; color: #144082;
      border-radius: 8px; padding: 6px 10px; cursor: pointer; font-size: 12px;
    }}
    .btn:hover {{ background: #edf4ff; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }}
    input, select {{
      border: 1px solid #c4d1ea; border-radius: 9px; padding: 8px 10px;
      min-width: 220px; font-size: 13px; background: #fff;
    }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .accordion {{ border: 1px solid var(--line); border-radius: 10px; margin: 8px 0; overflow: hidden; }}
    .accordion button {{
      width: 100%; text-align: left; background: #f9fbff; border: 0;
      border-bottom: 1px solid var(--line); padding: 12px; cursor: pointer;
      font-size: 14px; color: #1c396c;
    }}
    .accordion .body {{ padding: 12px; display: none; background: #fff; }}
    .flow {{
      background: #f8fbff; border: 1px dashed #c3d6fb; border-radius: 10px;
      padding: 10px; font-family: Consolas, Menlo, Monaco, monospace; font-size: 13px; white-space: pre-wrap;
    }}
    @media (max-width: 1100px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .sidebar {{ position: relative; height: auto; }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">Delta Bot Documentation</div>
      <div class="stamp">Generated: {generated_utc}</div>
      <nav class="nav">
        <a href="#overview">1. Overview</a>
        <a href="#capabilities">2. Current Capabilities</a>
        <a href="#architecture">3. Architecture</a>
        <a href="#flow">4. Runtime Flow</a>
        <a href="#snippets">5. Critical Snippets</a>
        <a href="#modules">6. File-By-File Docs</a>
        <a href="#explorer">7. Full Code Explorer</a>
        <a href="#deploy-windows">8. Windows Deployment</a>
        <a href="#deploy-linux">9. Linux Deployment</a>
        <a href="#deploy-docker">10. Docker Deployment</a>
        <a href="#state">11. State DB + Unique IDs</a>
        <a href="#testing">12. Testing</a>
        <a href="#change-map">13. Change Map</a>
        <a href="#troubleshooting">14. Troubleshooting</a>
      </nav>
    </aside>

    <main class="content">
      <section id="overview">
        <h1>Delta Exchange India Trading Bot - Full Project Documentation</h1>
        <p>This document provides architecture, module-level explanations, deployment instructions for Windows/Linux, and a change-impact map. The full project source snapshot is embedded below with interactive explorer/search.</p>
        <div class="callout bad">
          <strong>Important:</strong> Delta Exchange India is integrated directly through this custom API client. CCXT is not used/supported in this implementation.
        </div>
        <div class="callout good">
          <strong>Main runtime path:</strong> <code>src/delta_exchange_bot/cli/trading_bot.py</code> + <code>scripts/run_bot.py</code>.
        </div>
      </section>

      <section id="capabilities">
        <h2>2) Current Capabilities</h2>
        <div class="grid2">
          <div class="card"><h3>Market Data</h3><p><code>fetch_ticker</code> and <code>fetch_candles</code> return pandas DataFrames (1m/5m/15m).</p></div>
          <div class="card"><h3>Candle Builder</h3><p>Tick-to-OHLC for 1-minute and 5-minute candles.</p></div>
          <div class="card"><h3>Strategies</h3><p>Momentum and RSI scalping with SL/TP/trailing metadata.</p></div>
          <div class="card"><h3>Risk</h3><p>1% risk per trade, 10x max leverage, 5% max daily loss validation.</p></div>
          <div class="card"><h3>Execution</h3><p>Market/limit order execution, retry logic, stop/tp/trailing triggers.</p></div>
          <div class="card"><h3>Persistence</h3><p><code>state.db</code> lifecycle logging with unique execution IDs.</p></div>
          <div class="card"><h3>Backtesting</h3><p>PnL, win rate, max drawdown, profit factor.</p></div>
          <div class="card"><h3>Monitoring</h3><p>Prometheus: trade_count, win_rate, drawdown, api_latency.</p></div>
        </div>
      </section>

      <section id="architecture">
        <h2>3) Architecture</h2>
        <div class="flow">[Delta API/WS]
      |
      v
[api/delta_client.py] -> [data/market_data.py] -> [strategy/*]
                                          |           |
                                          |           v
                                          |      [risk/risk_management.py]
                                          |           |
                                          v           v
                                [execution/order_execution_engine.py]
                                          |
                                          v
                                    [persistence/db.py]
                                          |
                                          +--> [monitoring/prometheus_exporter.py]

Main runner: [cli/trading_bot.py]
Legacy runner: [core/engine.py + cli/main.py]
</div>
      </section>

      <section id="flow">
        <h2>4) Runtime Flow (One-Minute Loop)</h2>
        <ol>
          <li>Fetch candles for symbol via <code>fetch_candles(..., "1m")</code>.</li>
          <li>Compute EMA20 and RSI.</li>
          <li>Process open-position protection triggers (SL/TP/trailing).</li>
          <li>If no open position, generate strategy signal.</li>
          <li>Apply risk sizing and validation.</li>
          <li>Execute order (paper or live).</li>
          <li>Persist execution entries and open-position state.</li>
          <li>Record metrics and continue next cycle.</li>
        </ol>
      </section>

      <section id="snippets">
        <h2>5) Critical Code Snippets</h2>
        <div id="snippetContainer"></div>
      </section>

      <section id="modules">
        <h2>6) File-By-File Documentation</h2>
        <div class="toolbar">
          <input id="moduleSearch" placeholder="Filter by path/category/symbol" />
        </div>
        <div id="moduleCards"></div>
      </section>

      <section id="explorer">
        <h2>7) Full Code Explorer</h2>
        <p>Every source/config/test/build file collected for this snapshot can be viewed below.</p>
        <div class="toolbar">
          <select id="fileSelect"></select>
          <input id="codeSearch" placeholder="Search text in selected file" />
          <button class="btn" id="copyCodeBtn">Copy File</button>
        </div>
        <div class="muted" id="fileMeta"></div>
        <pre id="fileCode"></pre>
      </section>

      <section id="deploy-windows">
        <h2>8) Windows Deployment</h2>
<pre>cd C:\\path\\to\\Algo_Trading
python -m venv .venv
.\\.venv\\Scripts\\activate
pip install --upgrade pip
pip install -r requirements.txt
copy .env.example .env

# run paper mode
python scripts/run_bot.py --mode paper --strategy rsi_scalping --cycles 10

# tests
pytest -q</pre>
      </section>

      <section id="deploy-linux">
        <h2>9) Linux Deployment</h2>
<pre>cd /opt/algo_trading
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env

# run
python scripts/run_bot.py --mode paper --strategy rsi_scalping</pre>
      </section>

      <section id="deploy-docker">
        <h2>10) Docker Deployment</h2>
<pre>docker build -t delta-trading-bot .
docker run --rm -e DELTA_API_KEY=... -e DELTA_API_SECRET=... -e DELTA_MODE=paper -p 8000:8000 delta-trading-bot</pre>
      </section>

      <section id="state">
        <h2>11) State DB and Unique IDs</h2>
        <ul>
          <li><code>execution_logs.execution_id</code> is UNIQUE.</li>
          <li><code>execution_logs.client_order_id</code> is UNIQUE.</li>
          <li><code>trade_id</code> links entry and exit for the same lifecycle.</li>
          <li><code>open_position_state</code> enables restart recovery and continued trailing-stop management.</li>
        </ul>
      </section>

      <section id="testing">
        <h2>12) Testing</h2>
        <p>Unit tests cover strategy, risk, execution, persistence, market-data parsing, backtesting, metrics, and runtime flow.</p>
<pre>pytest -q
python scripts/run_bot.py --help</pre>
      </section>

      <section id="change-map">
        <h2>13) Where To Change What</h2>
        <table>
          <thead><tr><th>Change Needed</th><th>Files to Edit</th></tr></thead>
          <tbody>
            <tr><td>Strategy logic/thresholds</td><td><code>src/delta_exchange_bot/strategy/*.py</code></td></tr>
            <tr><td>Trailing stop behavior</td><td><code>src/delta_exchange_bot/execution/order_execution_engine.py</code></td></tr>
            <tr><td>Risk limits</td><td><code>src/delta_exchange_bot/risk/risk_management.py</code></td></tr>
            <tr><td>Market intervals / candle parsing</td><td><code>src/delta_exchange_bot/data/market_data.py</code></td></tr>
            <tr><td>Main loop behavior</td><td><code>src/delta_exchange_bot/cli/trading_bot.py</code></td></tr>
            <tr><td>DB schema / audit fields</td><td><code>src/delta_exchange_bot/persistence/db.py</code></td></tr>
            <tr><td>Deployment image/runtime</td><td><code>Dockerfile</code>, <code>requirements.txt</code>, <code>docker-compose.yml</code></td></tr>
          </tbody>
        </table>
      </section>

      <section id="troubleshooting">
        <h2>14) Troubleshooting</h2>
        <div class="callout"><strong>Auth issues:</strong> verify <code>DELTA_API_KEY</code>, <code>DELTA_API_SECRET</code>, and <code>DELTA_EXCHANGE_ENV</code>.</div>
        <div class="callout"><strong>No signal:</strong> ensure enough candle history is available for EMA/RSI warm-up.</div>
        <div class="callout"><strong>No trailing exits:</strong> ensure <code>on_price_update</code> is called regularly and protection state is active.</div>
      </section>
    </main>
  </div>

  <script id="fileData" type="application/json">{files_json}</script>
  <script id="moduleData" type="application/json">{modules_json}</script>
  <script id="snippetData" type="application/json">{snippets_json}</script>
  <script>
    const files = JSON.parse(document.getElementById('fileData').textContent);
    const modules = JSON.parse(document.getElementById('moduleData').textContent);
    const snippets = JSON.parse(document.getElementById('snippetData').textContent);

    function escapeHtml(s) {{
      return s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
    }}

    function copyText(text) {{
      navigator.clipboard.writeText(text).catch(() => {{}});
    }}

    const snippetContainer = document.getElementById('snippetContainer');
    snippets.forEach((snip, idx) => {{
      const wrap = document.createElement('div');
      wrap.className = 'snippet card';
      wrap.innerHTML = `
        <div class="snippet-head">
          <div>
            <h3>${{snip.title}}</h3>
            <div class="muted">${{snip.path}}</div>
          </div>
          <button class="btn" data-copy-snippet="${{idx}}">Copy Snippet</button>
        </div>
        <p>${{snip.explain}}</p>
        <pre>${{escapeHtml(snip.code)}}</pre>
      `;
      snippetContainer.appendChild(wrap);
    }});

    snippetContainer.addEventListener('click', (e) => {{
      const btn = e.target.closest('button[data-copy-snippet]');
      if (!btn) return;
      const idx = Number(btn.getAttribute('data-copy-snippet'));
      copyText(snippets[idx].code);
      btn.textContent = 'Copied';
      setTimeout(() => btn.textContent = 'Copy Snippet', 1200);
    }});

    const moduleCards = document.getElementById('moduleCards');
    const moduleSearch = document.getElementById('moduleSearch');

    function renderModules(filter = '') {{
      const q = filter.trim().toLowerCase();
      moduleCards.innerHTML = '';
      modules
        .filter((m) => m.path.toLowerCase().includes(q) || m.category.toLowerCase().includes(q) || (m.symbols || []).join(' ').toLowerCase().includes(q))
        .forEach((m, i) => {{
          const box = document.createElement('div');
          box.className = 'accordion';
          const chips = (m.symbols || []).map((s) => `<span class="pill">${{s}}</span>`).join('');
          box.innerHTML = `
            <button type="button" data-acc="${{i}}">${{m.path}} <span class="muted">(${{m.category}})</span></button>
            <div class="body" id="acc-body-${{i}}">
              <p><strong>Purpose:</strong> ${{m.purpose}}</p>
              <p><strong>How It Works:</strong> ${{m.how}}</p>
              <p><strong>Where To Change:</strong> ${{m.change}}</p>
              <div>${{chips || '<span class="muted">No class/def symbols in this file.</span>'}}</div>
            </div>
          `;
          moduleCards.appendChild(box);
        }});
    }}

    moduleCards.addEventListener('click', (e) => {{
      const btn = e.target.closest('button[data-acc]');
      if (!btn) return;
      const i = btn.getAttribute('data-acc');
      const body = document.getElementById(`acc-body-${{i}}`);
      body.style.display = body.style.display === 'block' ? 'none' : 'block';
    }});
    moduleSearch.addEventListener('input', () => renderModules(moduleSearch.value));
    renderModules('');

    const fileSelect = document.getElementById('fileSelect');
    const codeSearch = document.getElementById('codeSearch');
    const fileCode = document.getElementById('fileCode');
    const fileMeta = document.getElementById('fileMeta');
    const copyCodeBtn = document.getElementById('copyCodeBtn');

    files.forEach((f, idx) => {{
      const opt = document.createElement('option');
      opt.value = String(idx);
      opt.textContent = `${{f.path}} (${{f.category}})`;
      fileSelect.appendChild(opt);
    }});

    function renderFile() {{
      const idx = Number(fileSelect.value || 0);
      const f = files[idx];
      const q = codeSearch.value.trim().toLowerCase();
      let content = f.content;
      if (q) {{
        const lines = content.split('\\n');
        const out = [];
        lines.forEach((line, i) => {{
          if (line.toLowerCase().includes(q)) out.push(`${{String(i + 1).padStart(4, ' ')}} | ${{line}}`);
        }});
        content = out.length ? out.join('\\n') : '(no matching lines)';
      }}
      const m = modules.find((x) => x.path === f.path);
      fileMeta.textContent = m ? `Category: ${{f.category}} | Symbols: ${{(m.symbols || []).length}} | Purpose: ${{m.purpose}}` : `Category: ${{f.category}}`;
      fileCode.innerHTML = escapeHtml(content);
    }}

    fileSelect.addEventListener('change', renderFile);
    codeSearch.addEventListener('input', renderFile);
    copyCodeBtn.addEventListener('click', () => {{
      const idx = Number(fileSelect.value || 0);
      copyText(files[idx].content);
      copyCodeBtn.textContent = 'Copied';
      setTimeout(() => copyCodeBtn.textContent = 'Copy File', 1200);
    }});

    fileSelect.value = '0';
    renderFile();
  </script>
</body>
</html>
"""

    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Wrote: {OUTPUT}")
    print(f"Files embedded: {len(file_data)}")


if __name__ == "__main__":
    main()
