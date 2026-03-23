from delta_exchange_bot.core.settings import Settings


def test_base_url_override_wins_over_exchange_env():
    settings = Settings(
        exchange_env="testnet-india",
        base_url="https://custom.delta.exchange/",
    )
    assert settings.api_url == "https://custom.delta.exchange"


def test_live_mode_loads_live_postgres_database():
    settings = Settings(mode="live")
    assert settings.postgres_dsn.endswith("/trading_live")
