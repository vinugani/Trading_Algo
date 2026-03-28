from pathlib import Path
from typing import Any

import os
import yaml
import structlog
from pydantic import AliasChoices
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


# ── URL constants — single source of truth ────────────────────────────────────
_PROD_API_URL = "https://api.india.delta.exchange"
_PROD_WS_URL  = "wss://socket.india.delta.exchange"
_TEST_API_URL = "https://cdn-ind.testnet.deltaex.org"
_TEST_WS_URL  = "wss://socket-ind.testnet.deltaex.org"

# Hostnames used for cross-environment mismatch detection
_LIVE_HOSTNAMES = frozenset({"api.india.delta.exchange"})
_TEST_HOSTNAMES = frozenset({"cdn-ind.testnet.deltaex.org", "testnet.deltaex.org"})


def _load_yaml_config(mode: str) -> dict[str, Any]:
    config: dict[str, Any] = {}
    # Use absolute path for reliability on Windows
    root_dir = Path(__file__).resolve().parents[3]
    config_dir = root_dir / "config"

    # Load default first, then mode-specific overrides
    mode_filenames = [f"{mode}.yml"]
    if mode == "live":
        mode_filenames.append("prod.yml")
    elif mode == "prod":
        mode_filenames.append("live.yml")

    for fname in ("default.yml", *mode_filenames):
        fpath = config_dir / fname
        if fpath.exists():
            try:
                with open(fpath, "r") as f:
                    data = yaml.safe_load(f) or {}
                # Flatten the YAML structure (app:, delta:, etc.)
                for section in data.values():
                    if isinstance(section, dict):
                        config.update(section)
                    else:
                        # Handle cases where values are at the top level
                        config.update(data)
                        break
            except Exception as e:
                print(f"Error loading {fname}: {e}")
    return config


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DELTA_", env_file=".env", extra="ignore")

    mode: str = Field("paper", description="paper or live")
    strategy_name: str = Field(
        "momentum",
        description="portfolio or momentum or rsi_scalping or ema_crossover or trend_following or mean_reversion",
    )

    # ── Exchange environment — the ONLY source of truth for API endpoints ──────
    # Defaults to testnet-india. Must be explicitly changed to prod-india for live.
    exchange_env: str = Field(
        "testnet-india",
        description="'testnet-india' (safe default) or 'prod-india' (real money). "
                    "Drives api_url and ws_url — do not mix with manual URL fields.",
    )

    # ── Live trading safety gate ───────────────────────────────────────────────
    # Both exchange_env=prod-india AND allow_live_trading=True are required to
    # send real orders. Either condition alone is not sufficient.
    allow_live_trading: bool = Field(
        False,
        description="Must be explicitly True to permit prod-india real-money trading. "
                    "Default False ensures testnet-safe operation.",
    )

    # ── URLs — set automatically from exchange_env, never set manually ─────────
    # Empty defaults are overwritten during __init__. Do not override via env/YAML.
    api_url: str = Field("", description="Auto-set from exchange_env. Do not override.")
    ws_url: str = Field("", description="Auto-set from exchange_env. Do not override.")

    # Optional override for local/mock server testing only.
    # When set, URL cross-checking is skipped and this value is used as api_url.
    base_url: str | None = Field(
        None,
        description="Local/mock server override. Skips environment URL validation.",
        validation_alias=AliasChoices("base_url", "DELTA_BASE_URL", "BASE_URL"),
    )

    base_currency: str = "USDT"
    trade_symbols: list[str] = Field(default_factory=lambda: ["SOLUSD", "BTCUSD", "ETHUSD"])
    order_size: float = 100.0
    max_positions: int = 5
    trade_frequency_s: int = 5

    api_key: str = ""
    api_secret: str = ""
    enable_risk: bool = True
    timezone: str = "Asia/Kolkata"

    max_risk_per_trade: float = Field(
        0.01,
        validation_alias=AliasChoices("max_risk_per_trade", "MAX_RISK_PER_TRADE", "DELTA_MAX_RISK_PER_TRADE"),
    )
    max_daily_loss: float = Field(
        0.05,
        validation_alias=AliasChoices("max_daily_loss", "MAX_DAILY_LOSS", "DELTA_MAX_DAILY_LOSS"),
    )
    max_leverage: float = Field(
        10.0,
        validation_alias=AliasChoices("max_leverage", "MAX_LEVERAGE", "DELTA_MAX_LEVERAGE"),
    )
    max_asset_exposure: float = Field(
        0.25,
        validation_alias=AliasChoices("max_asset_exposure", "MAX_ASSET_EXPOSURE", "DELTA_MAX_ASSET_EXPOSURE"),
    )

    websocket_enabled: bool = True
    websocket_reconnect_interval_s: int = 1
    websocket_fallback_poll_interval_s: int = 2
    websocket_ping_interval_s: int = 20
    websocket_ping_timeout_s: int = 10
    websocket_stale_after_s: int = 60

    enable_async_runner: bool = True
    enable_smart_order_routing: bool = True
    spread_threshold_pct: float = 0.0008
    max_slippage_pct: float = 0.002
    order_chunk_size: float = 0.0
    max_retries_per_chunk: int = 3
    position_sync_tolerance: float = 1e-8
    # Live exchange order books can take 3-8 s to reflect a new position.
    # 5 retries × 2.5 s gives up to ~12.5 s settling time before halting.
    position_sync_retries: int = 5
    position_sync_retry_delay_s: float = 2.5
    shutdown_signal_path: str = "logs/bot.shutdown"
    cancel_leftover_orders_on_startup: bool = True
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005
    emergency_exit_verify_timeout_s: int = 20

    # Funding & Time-based closing
    enable_funding_awareness: bool = True
    funding_alert_threshold: float = 0.001  # 0.1% per 8h
    max_holding_time_s: int = 1800  # 30 minutes max for scalping

    enable_strategy_portfolio: bool = True
    min_signal_confidence: float = 0.6
    paper_force_buy_confidence_threshold: float = 0.3
    enable_advanced_risk: bool = True
    api_circuit_breaker_failure_threshold: int = 5
    api_circuit_breaker_cooldown_s: int = 60

    metrics_port: int = 8000
    metrics_addr: str = "0.0.0.0"
    disable_metrics_server: bool = False

    state_db_path: str = "state.db"
    redis_url: str = Field("redis://redis:6379/0")
    postgres_dsn: str = Field("postgresql://postgres:postgres@postgres:5432/trading")

    # ── Logging ───────────────────────────────────────────────────────────────
    log_dir: str = Field(
        "logs",
        description="Directory to write bot log files. Set to '' to disable file logging.",
        validation_alias=AliasChoices("log_dir", "LOG_DIR", "DELTA_LOG_DIR"),
    )
    log_level: str = Field(
        "INFO",
        description="Minimum log level: DEBUG, INFO, WARNING, ERROR.",
        validation_alias=AliasChoices("log_level", "LOG_LEVEL", "DELTA_LOG_LEVEL"),
    )

    def __init__(self, **data):
        # Determine mode from kwargs or environment
        mode = data.get("mode") or os.getenv("DELTA_MODE") or os.getenv("mode") or "paper"
        yaml_config = _load_yaml_config(mode)

        # Merge YAML config; kwargs and env vars have priority
        for key, value in yaml_config.items():
            if key not in data:
                data[key] = value

        super().__init__(**data)
        self._configure_and_validate()

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _configure_and_validate(self) -> None:
        """Set URLs from exchange_env and enforce all safety invariants.

        Execution order:
          1. Assign api_url / ws_url from exchange_env (sole source of truth).
          2. Apply base_url override when provided (local/mock testing only).
          3. Cross-validate URLs against exchange_env (skipped with base_url).
          4. Block startup if prod-india is active but allow_live_trading is False.
          5. Emit structured startup audit log.
        """
        # ── Step 1: Assign URLs from exchange_env ─────────────────────────────
        if self.exchange_env == "prod-india":
            self.api_url = _PROD_API_URL
            self.ws_url  = _PROD_WS_URL
        elif self.exchange_env == "testnet-india":
            self.api_url = _TEST_API_URL
            self.ws_url  = _TEST_WS_URL
        else:
            raise ValueError(
                f"Invalid exchange_env={self.exchange_env!r}. "
                "Accepted values: 'testnet-india' or 'prod-india'."
            )

        # ── Step 2: Apply base_url override (local/mock only) ─────────────────
        using_base_url_override = bool(self.base_url)
        if using_base_url_override:
            self.api_url = self.base_url.rstrip("/")  # type: ignore[union-attr]

        # ── Step 3: Cross-validate URL vs exchange_env ────────────────────────
        if not using_base_url_override:
            if self.exchange_env == "testnet-india":
                if any(h in self.api_url for h in _LIVE_HOSTNAMES):
                    raise ValueError(
                        f"Configuration mismatch: exchange_env='testnet-india' but "
                        f"api_url resolves to a LIVE endpoint ({self.api_url}). "
                        "Correct exchange_env or remove the conflicting override."
                    )
            elif self.exchange_env == "prod-india":
                if any(h in self.api_url for h in _TEST_HOSTNAMES):
                    raise ValueError(
                        f"Configuration mismatch: exchange_env='prod-india' but "
                        f"api_url resolves to a TESTNET endpoint ({self.api_url}). "
                        "Correct exchange_env or remove the conflicting override."
                    )

        # ── Step 4: Live trading safety gate ──────────────────────────────────
        if self.exchange_env == "prod-india" and not self.allow_live_trading:
            raise RuntimeError(
                "LIVE TRADING BLOCKED: exchange_env='prod-india' requires "
                "allow_live_trading=True. "
                "Add DELTA_ALLOW_LIVE_TRADING=true to your .env only when you "
                "intend to trade with real money on the live exchange."
            )

        # ── Step 5: Startup audit log ─────────────────────────────────────────
        log = structlog.get_logger(__name__)
        log.info(
            "settings.configured",
            exchange_env=self.exchange_env,
            mode=self.mode,
            api_url=self.api_url,
            live_trading_active=(self.exchange_env == "prod-india"),
            base_url_override=self.base_url or "none",
        )
