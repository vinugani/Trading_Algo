import json

import pytest

from delta_exchange_bot.api.delta_client import DeltaClient, DeltaAPIError


class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code
        self.ok = status_code == 200
        self.text = json.dumps(json_data)

    def json(self):
        return self._json_data


def test_place_order_signing_and_payload(monkeypatch):
    captured = {}

    def fake_request(method, url, json=None, params=None, headers=None, timeout=None):
        captured['method'] = method
        captured['url'] = url
        captured['json'] = json
        captured['params'] = params
        captured['headers'] = headers
        captured['timeout'] = timeout
        return DummyResponse({"success": True, "data": {"order_id": "123"}})

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, 'request', fake_request)

    result = client.place_order(symbol="BTCUSD", side="BUY", size=1.2, price=30000.0)

    assert result["success"] is True
    assert result["data"]["order_id"] == "123"
    assert captured['method'] == 'POST'
    assert captured['url'] == 'https://cdn-ind.testnet.deltaex.org/v2/orders'
    assert captured['json']["product_id"] == "BTCUSD"
    assert captured['json']["limit_price"] == '30000.0'
    assert captured['headers']["api-key"] == "key"
    assert "signature" in captured['headers']


def test_get_ticker_returns_data(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        assert url == 'https://cdn-ind.testnet.deltaex.org/v2/tickers/BTCUSD'
        assert params is None
        assert headers['Content-Type'] == 'application/json'
        return DummyResponse({"success": True, "data": {"price": "30000"}})

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, 'get', fake_get)

    result = client.get_ticker("BTCUSD")

    assert result["success"] is True
    assert result["data"]["price"] == "30000"


def test_error_response_raises(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return DummyResponse({"success": False, "message": "invalid_auth"}, status_code=401)

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, 'get', fake_get)

    with pytest.raises(DeltaAPIError):
        client.get_ticker("BTCUSD")


def test_get_products_alias(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        if url.endswith("/v2/products"):
            return DummyResponse({"success": True, "result": [{"symbol": "BTCUSD"}]})
        raise AssertionError("unexpected url")

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, "get", fake_get)

    out = client.get_products()
    assert out["success"] is True


def test_cancel_order_single_argument_payload(monkeypatch):
    captured = {}

    def fake_request(method, url, json=None, params=None, headers=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        return DummyResponse({"success": True})

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, "request", fake_request)

    out = client.cancel_order("order-123")
    assert out["success"] is True
    assert captured["method"] == "DELETE"
    assert captured["json"] == {"id": "order-123"}


def test_get_positions_accepts_product_id(monkeypatch):
    captured = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse({"success": True, "result": []})

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, "get", fake_get)

    out = client.get_positions(product_id="BTCUSD")
    assert out["success"] is True
    assert captured["url"].endswith("/v2/positions")
    assert captured["params"] == {"product_id": "BTCUSD"}


def test_get_positions_resolves_symbol_to_numeric_product_id(monkeypatch):
    captured = {"positions_params": None}

    def fake_get(url, params=None, headers=None, timeout=None):
        if url.endswith("/v2/products"):
            return DummyResponse({"success": True, "result": [{"symbol": "SOLUSD", "id": 99}]})
        if url.endswith("/v2/positions"):
            captured["positions_params"] = params
            return DummyResponse({"success": True, "result": []})
        raise AssertionError(f"unexpected url {url}")

    client = DeltaClient(api_key="key", api_secret="secret", api_url="https://cdn-ind.testnet.deltaex.org")
    monkeypatch.setattr(client.session, "get", fake_get)

    out = client.get_positions(product_id="SOLUSD")
    assert out["success"] is True
    assert captured["positions_params"] == {"product_id": "99"}
