from pathlib import Path
from typing import Any 

import os
import yaml
from pydantic import AliasChoices
from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from pydantic_settings import PydanticBaseSettingsSource


def _load_yaml_config(mode: str) -> dict[str, Any]:
    config: dict[str, Any] = {}
    # Use absolute path for reliability on Windows
    root_dir = Path(__file__).resolve().parents[3]
    config_dir = root_dir / "config"

    # Load default first, then mode-specific overrides
    for fname in ("default.yml", f"{mode}.yml"):
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
    exchange_env: str = Field("prod-india", description="testnet-india or prod-india")
    base_currency: str = "USDT"
    trade_symbols: list[str] = Field(default_factory=lambda: ["SOLUSD", "BTCUSD", "ETHUSD"])
    order_size: float = 100.0
    max_positions: int = 5
    trade_frequency_s: int = 60
    api_url: str = Field("https://api.india.delta.exchange", description="API base URL")
    ws_url: str = Field("wss://socket.india.delta.exchange", description="WebSocket base URL")
    base_url: str | None = Field(
        None,
        description="Optional API base URL override (DELTA_BASE_URL)",
        validation_alias=AliasChoices("base_url", "DELTA_BASE_URL", "BASE_URL"),
    )
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
    websocket_reconnect_interval_s: int = 5
    websocket_fallback_poll_interval_s: int = 2

    enable_async_runner: bool = True
    enable_smart_order_routing: bool = True
    spread_threshold_pct: float = 0.0008
    max_slippage_pct: float = 0.002
    order_chunk_size: float = 0.0
    max_retries_per_chunk: int = 3
    position_sync_tolerance: float = 1e-8
    position_sync_retries: int = 3
    position_sync_retry_delay_s: float = 1.0
    shutdown_signal_path: str = "logs/bot.shutdown"
    cancel_leftover_orders_on_startup: bool = True
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005
    emergency_exit_verify_timeout_s: int = 20

    # Funding & Time-based closing
    enable_funding_awareness: bool = True
    funding_alert_threshold: float = 0.001  # 0.1% per 8h
    max_holding_time_s: int = 86400  # 24 hours default

    enable_strategy_portfolio: bool = True
    enable_advanced_risk: bool = True
    api_circuit_breaker_failure_threshold: int = 5
    api_circuit_breaker_cooldown_s: int = 60

    metrics_port: int = 8000
    metrics_addr: str = "0.0.0.0"
    disable_metrics_server: bool = False

    state_db_path: str = "state.db"
    redis_url: str = Field("redis://redis:6379/0")
    postgres_dsn: str = Field("postgresql://postgres:postgres@postgres:5432/trading")

    def __init__(self, **data):
        # Determine mode from kwargs or environment
        mode = data.get("mode") or os.getenv("DELTA_MODE") or os.getenv("mode") or "paper"
        yaml_config = _load_yaml_config(mode)
        
        # Merge YAML config if not already in data (kwargs have priority)
        for key, value in yaml_config.items():
            if key not in data:
                data[key] = value
                
        super().__init__(**data)
        if self.exchange_env == "prod-india":
            self.api_url = "https://api.india.delta.exchange"
            self.ws_url = "wss://socket.india.delta.exchange"
        elif self.exchange_env == "testnet-india":
            self.api_url = "https://cdn-ind.testnet.deltaex.org"
            self.ws_url = "wss://socket-ind.testnet.deltaex.org"
        else:
            raise ValueError(f"Unsupported exchange_env={self.exchange_env}")

        if self.base_url:
            self.api_url = self.base_url.rstrip("/")