import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

import requests


def _query_string(query: Optional[Dict[str, Any]]) -> str:
    if not query:
        return ""
    ordered = sorted(query.items())
    return "&".join(f"{k}={v}" for k, v in ordered)


def _body_string(payload: Optional[Dict[str, Any]]) -> str:
    if not payload:
        return ""
    # Must match `requests` JSON serialization format used in actual request body.
    return json.dumps(payload)


class DeltaAPIError(Exception):
    """Raised when Delta API returns an error status."""


class DeltaClient:
    """Delta Exchange REST API client (supports IN production/testnet environments)."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_url: str,
        ws_url: Optional[str] = None,
        timeout: int = 20,
        min_request_interval_s: float = 0.05,
        max_rate_limit_retries: int = 3,
    ):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.api_url = api_url.rstrip("/")
        self.ws_url = ws_url
        self.timeout = timeout
        self.min_request_interval_s = max(0.0, float(min_request_interval_s))
        self.max_rate_limit_retries = max(0, int(max_rate_limit_retries))
        self.session = requests.Session()
        self._last_request_monotonic = 0.0
        self._products_cache_by_symbol: Dict[str, Dict[str, Any]] = {}
        self._products_cache_by_id: Dict[str, Dict[str, Any]] = {}
        self._products_cache_updated_at = 0.0
        self._products_cache_ttl_s = 300.0

    def _create_auth_headers(
        self,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        if not self.api_key or not self.api_secret:
            raise DeltaAPIError("API key/secret are required for authenticated request")

        # Delta India authenticated requests expect epoch seconds.
        ts = str(int(time.time()))
        query_s = _query_string(query)
        body_s = _body_string(payload)
        query_part = f"?{query_s}" if query_s else ""
        signature_data = method + ts + path + query_part + body_s
        signature = hmac.new(self.api_secret, signature_data.encode("utf-8"), hashlib.sha256).hexdigest()

        return {
            "Content-Type": "application/json",
            "api-key": self.api_key,
            "timestamp": ts,
            "signature": signature,
            "User-Agent": "delta-rest-client-v1.0.13",
        }

    def _throttle(self) -> None:
        if self.min_request_interval_s <= 0:
            return
        now = time.monotonic()
        wait_s = self.min_request_interval_s - (now - self._last_request_monotonic)
        if wait_s > 0:
            time.sleep(wait_s)
        self._last_request_monotonic = time.monotonic()

    def _request(
        self,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        url = f"{self.api_url}{path}"
        method_u = method.upper()
        last_error: Optional[DeltaAPIError] = None

        for attempt in range(self.max_rate_limit_retries + 1):
            self._throttle()
            headers = self._create_auth_headers(method_u, path, query, payload) if auth else {
                "Content-Type": "application/json",
                "User-Agent": "delta-rest-client-v1.0.13",
            }
            if method_u == "GET":
                res = self.session.get(url, params=query, headers=headers, timeout=self.timeout)
            else:
                res = self.session.request(method_u, url, json=payload, params=query, headers=headers, timeout=self.timeout)

            if res.status_code == 429:
                retry_after_raw = getattr(res, "headers", {}).get("Retry-After", "1")
                try:
                    retry_after = float(retry_after_raw)
                except (TypeError, ValueError):
                    retry_after = 1.0
                time.sleep(max(0.0, retry_after * (attempt + 1)))
                last_error = DeltaAPIError(f"HTTP 429: {res.text}")
                continue

            if not res.ok:
                raise DeltaAPIError(f"HTTP {res.status_code}: {res.text}")

            out = res.json()
            if isinstance(out, dict) and (out.get("success") is False or out.get("status") == "error"):
                raise DeltaAPIError(out)
            return out

        if last_error is not None:
            raise last_error
        raise DeltaAPIError("Request failed after retries")

    def get_markets(self) -> Dict[str, Any]:
        try:
            return self._request("GET", "/v2/products", auth=False)
        except DeltaAPIError:
            return self._request("GET", "/v2/instruments", auth=False)

    # Compatibility alias for external integrations expecting this method name.
    def get_products(self) -> Dict[str, Any]:
        return self.get_markets()

    def _refresh_products_cache(self, force: bool = False) -> None:
        if not force and (time.time() - self._products_cache_updated_at) < self._products_cache_ttl_s:
            return
        payload = self.get_products()
        rows = []
        if isinstance(payload, dict):
            rows = payload.get("result") or payload.get("data") or []
        if not isinstance(rows, list):
            return
        by_symbol: Dict[str, Dict[str, Any]] = {}
        by_id: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("product_symbol") or row.get("name") or "").upper()
            if symbol:
                by_symbol[symbol] = row
            product_id = row.get("id") or row.get("product_id")
            if product_id is not None:
                by_id[str(product_id)] = row
        if by_symbol or by_id:
            self._products_cache_by_symbol = by_symbol
            self._products_cache_by_id = by_id
            self._products_cache_updated_at = time.time()

    def _get_product_row(self, product: str | int) -> Optional[Dict[str, Any]]:
        key = str(product)
        symbol_key = key.upper()
        row = self._products_cache_by_id.get(key) or self._products_cache_by_symbol.get(symbol_key)
        if row is not None:
            return row
        try:
            self._refresh_products_cache()
        except Exception:
            return None
        return self._products_cache_by_id.get(key) or self._products_cache_by_symbol.get(symbol_key)

    def _resolve_product_id(self, product_id: str | int) -> str:
        product_id_str = str(product_id)
        if product_id_str.isdigit():
            return product_id_str
        row = self._get_product_row(product_id_str)
        if isinstance(row, dict):
            candidate = row.get("id") or row.get("product_id")
            if candidate is not None:
                return str(candidate)
        return product_id_str

    def _normalize_order_size(self, symbol: str, size: float) -> str:
        size_f = float(size)
        if size_f <= 0:
            return "0"

        row = self._get_product_row(symbol)
        if not isinstance(row, dict):
            return str(size_f)

        contract_value_raw = row.get("contract_value")
        contract_type = str(row.get("contract_type") or "").lower()
        try:
            contract_value = float(contract_value_raw) if contract_value_raw is not None else 0.0
        except (TypeError, ValueError):
            contract_value = 0.0

        if contract_value > 0:
            contracts = int(round(size_f / contract_value))
            contracts = max(1, contracts)
            return str(contracts)
        if "futures" in contract_type or "perpetual" in contract_type:
            return str(max(1, int(round(size_f))))
        return str(size_f)

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", f"/v2/tickers/{symbol}", auth=False)

    def get_orderbook(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", f"/v2/l2orderbook/{symbol}", auth=False)

    def get_candles(self, symbol: str, resolution: str, start: int, end: int) -> Dict[str, Any]:
        query = {
            "symbol": symbol,
            "resolution": resolution,
            "start": int(start),
            "end": int(end),
        }
        return self._request("GET", "/v2/history/candles", query=query, auth=False)

    # Compatibility wrapper with optional range.
    def fetch_candles(
        self,
        symbol: str,
        resolution: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> Dict[str, Any]:
        now = int(time.time())
        end_ts = int(end) if end is not None else now
        start_ts = int(start) if start is not None else end_ts - 200 * 60
        return self.get_candles(symbol=symbol, resolution=resolution, start=start_ts, end=end_ts)

    def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float] = None,
        order_type: str = "limit_order",
        time_in_force: Optional[str] = None,
        post_only: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved_product_id = self._resolve_product_id(symbol)
        normalized_size = self._normalize_order_size(symbol, size)
        body = {
            "product_id": int(resolved_product_id) if str(resolved_product_id).isdigit() else resolved_product_id,
            "size": normalized_size,
            "side": side,
            "order_type": order_type,
            "post_only": str(post_only).lower(),
            "reduce_only": str(reduce_only).lower(),
        }
        if price is not None:
            body["limit_price"] = str(price)
        if time_in_force is not None:
            body["time_in_force"] = time_in_force
        if client_order_id:
            body["client_order_id"] = client_order_id

        return self._request("POST", "/v2/orders", payload=body, auth=True)

    def cancel_order(self, *args, **kwargs) -> Dict[str, Any]:
        """Cancel an order.

        Supports both:
        - cancel_order(symbol, order_id)  [legacy]
        - cancel_order(order_id)          [compat]
        - cancel_order(order_id=..., symbol=...)
        """
        symbol: Optional[str] = kwargs.get("symbol")
        order_id: Optional[str] = kwargs.get("order_id")

        if len(args) == 2:
            symbol = str(args[0])
            order_id = str(args[1])
        elif len(args) == 1:
            order_id = str(args[0])

        if not order_id:
            raise DeltaAPIError("order_id is required")

        payload: Dict[str, Any] = {"id": order_id}
        if symbol:
            payload["product_id"] = symbol
        return self._request("DELETE", "/v2/orders", payload=payload, auth=True)

    def get_order(self, order_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/v2/orders/{order_id}", auth=True)

    def list_orders(self, status: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        return self._request("GET", "/v2/orders", query=query, auth=True)

    def get_open_orders(self) -> Dict[str, Any]:
        return self.list_orders(status="open")

    def get_account(self) -> Dict[str, Any]:
        for path in ("/v2/accounts", "/v2/wallet/balances", "/v2/wallet/balance"):
            try:
                return self._request("GET", path, auth=True)
            except DeltaAPIError as exc:
                if "HTTP 404" not in str(exc):
                    raise
        raise DeltaAPIError("No supported account endpoint responded successfully")

    def get_account_balance(self) -> Dict[str, Any]:
        return self.get_account()

    def get_positions(
        self,
        product_id: Optional[str] = None,
        underlying_asset_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if product_id:
            query["product_id"] = self._resolve_product_id(product_id)
        if underlying_asset_symbol:
            query["underlying_asset_symbol"] = underlying_asset_symbol
        return self._request("GET", "/v2/positions", query=query if query else None, auth=True)

    def place_market_order(
        self,
        product_id: str,
        side: str,
        size: float,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.place_order(
            symbol=product_id,
            side=side,
            size=size,
            order_type="market_order",
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )

    def place_limit_order(
        self,
        product_id: str,
        side: str,
        size: float,
        price: float,
        time_in_force: str = "gtc",
        post_only: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.place_order(
            symbol=product_id,
            side=side,
            size=size,
            price=price,
            order_type="limit_order",
            time_in_force=time_in_force,
            post_only=post_only,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )
