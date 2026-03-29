"""Unit tests for _position_notional_usd — the fix for the bot-crash bug where
reconciliation incorrectly computed USD notional as (size_contracts × spot_price)
instead of using contract_value from the product catalog.

Delta Exchange India BTCUSD inverse perpetual:
  contract_type = "inverse_perpetual"
  contract_value = 1  (USD per contract)
  → notional = size_contracts × 1  (NOT size_contracts × spot_price)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest


def _make_bot_with_product_row(product_row):
    """Build a minimal ProfessionalTradingBot-like object that only needs
    _position_notional_usd to work.  We patch everything heavy away."""
    from delta_exchange_bot.core.settings import Settings

    settings = Settings(
        mode="paper",
        api_key="k",
        api_secret="s",
        trade_symbols=["BTCUSD"],
    )

    mock_client = MagicMock()
    mock_client._get_product_row.return_value = product_row

    with patch("delta_exchange_bot.cli.professional_bot.DeltaClient", return_value=mock_client), \
         patch("delta_exchange_bot.cli.professional_bot.DatabaseManager"), \
         patch("delta_exchange_bot.cli.professional_bot.OrderExecutionEngine"), \
         patch("delta_exchange_bot.cli.professional_bot.PrometheusMetricsExporter"), \
         patch("delta_exchange_bot.cli.professional_bot.RealtimeMarketDataService"), \
         patch("delta_exchange_bot.cli.professional_bot.SafetyController"):
        from delta_exchange_bot.cli.professional_bot import ProfessionalTradingBot
        bot = ProfessionalTradingBot(settings)
        bot.client = mock_client
        return bot


class TestPositionNotionalUsd:
    """Tests for ProfessionalTradingBot._position_notional_usd."""

    def test_inverse_perpetual_uses_contract_value_not_price(self):
        """BTCUSD inverse: 1 contract × $1/contract = $1, NOT 1 × $66520."""
        bot = _make_bot_with_product_row({
            "contract_type": "inverse_perpetual",
            "contract_value": 1.0,
        })
        notional = bot._position_notional_usd("BTCUSD", size_contracts=1.0, entry_price=66520.0)
        assert notional == pytest.approx(1.0), (
            f"Expected $1.00 for 1 inverse contract, got ${notional:.2f}. "
            "If this returns $66520, the crash bug is present."
        )

    def test_inverse_perpetual_scales_with_contracts(self):
        """Multiple contracts still multiply only by contract_value."""
        bot = _make_bot_with_product_row({
            "contract_type": "inverse_perpetual",
            "contract_value": 1.0,
        })
        notional = bot._position_notional_usd("ETHUSD", size_contracts=5.0, entry_price=2000.0)
        assert notional == pytest.approx(5.0)

    def test_linear_perpetual_multiplies_by_price(self):
        """Linear perp: 10 contracts × 0.001 BTC/contract × $60000 = $600."""
        bot = _make_bot_with_product_row({
            "contract_type": "perpetual",
            "contract_value": 0.001,
        })
        notional = bot._position_notional_usd("BTCUSDT", size_contracts=10.0, entry_price=60000.0)
        assert notional == pytest.approx(600.0)

    def test_fallback_when_no_product_row(self):
        """No product metadata → fallback to size × price."""
        bot = _make_bot_with_product_row(None)
        notional = bot._position_notional_usd("BTCUSD", size_contracts=1.0, entry_price=66520.0)
        assert notional == pytest.approx(66520.0)

    def test_fallback_when_contract_value_zero(self):
        """contract_value = 0 → fallback to size × price."""
        bot = _make_bot_with_product_row({
            "contract_type": "inverse_perpetual",
            "contract_value": 0.0,
        })
        notional = bot._position_notional_usd("BTCUSD", size_contracts=1.0, entry_price=66520.0)
        assert notional == pytest.approx(66520.0)

    def test_zero_size_returns_zero(self):
        bot = _make_bot_with_product_row({"contract_type": "inverse_perpetual", "contract_value": 1.0})
        assert bot._position_notional_usd("BTCUSD", 0.0, 66520.0) == 0.0

    def test_zero_price_returns_zero(self):
        bot = _make_bot_with_product_row({"contract_type": "inverse_perpetual", "contract_value": 1.0})
        assert bot._position_notional_usd("BTCUSD", 1.0, 0.0) == 0.0

    def test_safety_gate_passes_for_inverse_contract(self):
        """Confirm the safety gate no longer fires for a normal 1-contract position
        on a ~$100 account (max_leverage=10 → max_allowed=$1010).

        Before the fix: synced_notional = 1 × 66520 = $66520 > $1010 → CRASH.
        After the fix:  synced_notional = 1 × 1     = $1     < $1010 → OK.
        """
        bot = _make_bot_with_product_row({
            "contract_type": "inverse_perpetual",
            "contract_value": 1.0,
        })
        notional = bot._position_notional_usd("BTCUSD", size_contracts=1.0, entry_price=66520.0)
        account_equity = 101.12
        max_leverage = 10.0
        max_allowed = account_equity * max_leverage
        assert notional <= max_allowed, (
            f"Safety gate would fire: notional={notional:.2f} > max_allowed={max_allowed:.2f}"
        )
