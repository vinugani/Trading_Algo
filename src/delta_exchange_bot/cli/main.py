import argparse
import logging
from pathlib import Path

from delta_exchange_bot.core.engine import TradingEngine
from delta_exchange_bot.core.settings import Settings


def _load_settings_from_yaml(config_path: str) -> dict:
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        logging.warning("PyYAML is not installed. --config file will be ignored.")
        return {}

    path = Path(config_path)
    if not path.exists():
        logging.warning("Config file not found: %s. Falling back to env/default settings.", config_path)
        return {}

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        logging.warning("Config file format is invalid for %s. Expected mapping root.", config_path)
        return {}

    app = raw.get("app", {}) if isinstance(raw.get("app"), dict) else {}
    delta = raw.get("delta", {}) if isinstance(raw.get("delta"), dict) else {}
    mapped: dict = {}

    if "mode" in app:
        mapped["mode"] = app["mode"]
    if "exchange_env" in app:
        mapped["exchange_env"] = app["exchange_env"]
    if "base_currency" in app:
        mapped["base_currency"] = app["base_currency"]
    if "order_size" in app:
        mapped["order_size"] = app["order_size"]
    if "max_positions" in app:
        mapped["max_positions"] = app["max_positions"]
    if "trade_frequency_s" in app:
        mapped["trade_frequency_s"] = app["trade_frequency_s"]

    if "api_url" in delta:
        mapped["api_url"] = delta["api_url"]
    if "ws_url" in delta:
        mapped["ws_url"] = delta["ws_url"]
    if "api_key" in delta:
        mapped["api_key"] = delta["api_key"]
    if "api_secret" in delta:
        mapped["api_secret"] = delta["api_secret"]
    if "enable_risk" in delta:
        mapped["enable_risk"] = delta["enable_risk"]
    return mapped


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Delta Exchange Trading Bot")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper")
    parser.add_argument("--config", default="config/default.yml")
    parser.add_argument("--iterations", type=int, default=5, help="Number of cycles to run")
    args = parser.parse_args()

    file_settings = _load_settings_from_yaml(args.config)
    # CLI --mode should always override file mode.
    file_settings["mode"] = args.mode
    settings = Settings(**file_settings)
    engine = TradingEngine(settings)
    try:
        engine.run(max_iterations=args.iterations)
    except KeyboardInterrupt:
        logging.info("Shutting down trading engine")


if __name__ == "__main__":
    main()
