from prometheus_client import CollectorRegistry
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from prometheus_client import generate_latest
from prometheus_client import start_http_server


class PrometheusMetricsExporter:
    """Prometheus metrics exporter for live trading telemetry."""

    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()
        self._wins = 0
        self._trades = 0

        self.trade_count = Counter(
            "trade_count",
            "Total number of completed trades",
            registry=self.registry,
        )
        self.win_rate = Gauge(
            "win_rate",
            "Trade win rate in percentage",
            registry=self.registry,
        )
        self.drawdown = Gauge(
            "drawdown",
            "Current drawdown in percentage",
            registry=self.registry,
        )
        self.api_latency = Histogram(
            "api_latency",
            "API request latency in seconds",
            ["endpoint"],
            registry=self.registry,
        )
        self.api_errors = Counter(
            "api_errors",
            "Total API errors by endpoint/category",
            ["endpoint"],
            registry=self.registry,
        )
        self.order_failures = Counter(
            "order_failures",
            "Total failed order placement attempts",
            registry=self.registry,
        )
        self.total_pnl = Gauge(
            "total_pnl",
            "Cumulative realized pnl",
            registry=self.registry,
        )
        self.current_pnl = Gauge(
            "current_pnl",
            "Current pnl value for active session",
            registry=self.registry,
        )

    def start_server(self, port: int = 8000, addr: str = "0.0.0.0") -> None:
        start_http_server(port=port, addr=addr, registry=self.registry)

    def record_trade(self, pnl: float) -> None:
        self._trades += 1
        if pnl > 0:
            self._wins += 1

        self.trade_count.inc()
        self.win_rate.set((self._wins / self._trades) * 100.0 if self._trades > 0 else 0.0)

    def set_drawdown(self, drawdown_pct: float) -> None:
        self.drawdown.set(max(0.0, float(drawdown_pct)))

    def observe_api_latency(self, endpoint: str, latency_seconds: float) -> None:
        safe_latency = max(0.0, float(latency_seconds))
        endpoint_label = endpoint or "unknown"
        self.api_latency.labels(endpoint=endpoint_label).observe(safe_latency)

    def record_api_error(self, endpoint: str) -> None:
        endpoint_label = endpoint or "unknown"
        self.api_errors.labels(endpoint=endpoint_label).inc()

    def set_total_pnl(self, pnl: float) -> None:
        self.total_pnl.set(float(pnl))
        self.current_pnl.set(float(pnl))

    def set_current_pnl(self, pnl: float) -> None:
        self.current_pnl.set(float(pnl))

    def record_order_failure(self) -> None:
        self.order_failures.inc()

    def render_latest(self) -> bytes:
        return generate_latest(self.registry)
