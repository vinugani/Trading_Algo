from prometheus_client import CollectorRegistry

from delta_exchange_bot.monitoring.prometheus_exporter import PrometheusMetricsExporter


def test_trade_count_and_win_rate():
    registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=registry)

    exporter.record_trade(10.0)
    exporter.record_trade(-2.0)
    exporter.record_trade(3.0)

    assert registry.get_sample_value("trade_count_total") == 3.0
    assert registry.get_sample_value("win_rate") == (2 / 3) * 100.0


def test_drawdown_metric():
    registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=registry)

    exporter.set_drawdown(4.2)
    assert registry.get_sample_value("drawdown") == 4.2

    exporter.set_drawdown(-1.0)
    assert registry.get_sample_value("drawdown") == 0.0


def test_api_latency_histogram():
    registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=registry)

    exporter.observe_api_latency("/v2/tickers/BTCUSD", 0.12)
    exporter.observe_api_latency("/v2/tickers/BTCUSD", 0.08)

    count = registry.get_sample_value("api_latency_count", labels={"endpoint": "/v2/tickers/BTCUSD"})
    total = registry.get_sample_value("api_latency_sum", labels={"endpoint": "/v2/tickers/BTCUSD"})

    assert count == 2.0
    assert total == 0.2


def test_api_error_counter():
    registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=registry)

    exporter.record_api_error("/v2/orders")
    exporter.record_api_error("/v2/orders")
    count = registry.get_sample_value("api_errors_total", labels={"endpoint": "/v2/orders"})

    assert count == 2.0


def test_total_pnl_gauge():
    registry = CollectorRegistry()
    exporter = PrometheusMetricsExporter(registry=registry)

    exporter.set_total_pnl(12.5)
    assert registry.get_sample_value("total_pnl") == 12.5
