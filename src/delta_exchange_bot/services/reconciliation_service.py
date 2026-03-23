import asyncio
import logging
from typing import Dict, Any, List

from delta_exchange_bot.api.delta_client import DeltaClient
from delta_exchange_bot.persistence.db import DatabaseManager
from delta_exchange_bot.persistence.models import PositionSide

logger = logging.getLogger(__name__)

class ReconciliationService:
    """
    Background service to ensure local DB and exchange state are in sync.
    """
    def __init__(
        self, 
        api: DeltaClient, 
        db: DatabaseManager, 
        symbols: List[str],
        interval_s: int = 300 # 5 minutes
    ):
        self.api = api
        self.db = db
        self.symbols = symbols
        self.interval_s = interval_s
        self._is_running = False

    async def start(self):
        self._is_running = True
        logger.info(f"ReconciliationService started with {self.interval_s}s interval.")
        while self._is_running:
            try:
                await self.reconcile()
            except Exception as e:
                logger.error(f"Reconciliation loop error: {e}")
            await asyncio.sleep(self.interval_s)

    def stop(self):
        self._is_running = False

    async def reconcile(self):
        logger.info("Starting reconciliation audit...")
        
        # 1. Fetch live positions from Delta
        try:
            exchange_positions_raw = self.api.get_positions()
            exchange_positions_data = exchange_positions_raw.get("result", [])
            # Map by symbol for easy comparison
            exchange_pos = {p["symbol"]: p for p in exchange_positions_data if float(p.get("size", 0)) != 0}
        except Exception as e:
            logger.error(f"Failed to fetch positions from exchange: {e}")
            return

        # 2. Audit each tracked symbol
        for symbol in self.symbols:
            local_pos = self.db.get_active_position(symbol)
            ex_pos = exchange_pos.get(symbol)
            
            if ex_pos and not local_pos:
                logger.warning(f"DISCREPANCY: Position for {symbol} exists on exchange but not in DB. Syncing...")
                # We don't have the original trade_id, so we'll create a synthetic one
                self.db.update_position({
                    "symbol": symbol,
                    "trade_id": f"recon-{symbol}",
                    "side": ex_pos["side"],
                    "size": abs(float(ex_pos["size"])),
                    "avg_entry_price": float(ex_pos["avg_entry_price"])
                })
                
            elif local_pos and not ex_pos:
                logger.warning(f"DISCREPANCY: Position for {symbol} exists in DB but not on exchange. Closing in DB...")
                self.db.close_position(symbol)
                # Update logical trade if possible
                self.db.close_trade(local_pos["trade_id"], exit_price=0.0) # Price unknown
                
            elif local_pos and ex_pos:
                # Check for size/side mismatch
                ex_size = abs(float(ex_pos["size"]))
                if abs(local_pos["size"] - ex_size) > 1e-6 or local_pos["side"] != ex_pos["side"].lower():
                    logger.warning(f"DISCREPANCY: Position mismatch for {symbol}. Local: {local_pos['size']} {local_pos['side']}, Exchange: {ex_size} {ex_pos['side']}. Updating DB...")
                    self.db.update_position({
                        "symbol": symbol,
                        "trade_id": local_pos["trade_id"],
                        "side": ex_pos["side"],
                        "size": ex_size,
                        "avg_entry_price": float(ex_pos["avg_entry_price"])
                    })
        
        logger.info("Reconciliation audit completed.")
