from delta_exchange_bot.api.delta_client import DeltaClient


def test_delta_client_builds_hmac_headers_without_ccxt_dependency():
    client = DeltaClient(
        api_key="key",
        api_secret="secret",
        api_url="https://api.india.delta.exchange",
    )
    headers = client._create_auth_headers("GET", "/v2/orders")

    assert headers["api-key"] == "key"
    assert headers["timestamp"]
    assert headers["signature"]


def test_delta_client_uses_requests_session():
    client = DeltaClient(
        api_key="key",
        api_secret="secret",
        api_url="https://api.india.delta.exchange",
    )
    assert client.session is not None
