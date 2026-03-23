import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Production-grade Risk Management Engine.
    Handles:
    - Position sizing based on risk-per-trade
    - Daily loss limits (Kill Switch)
    - Max concurrent positions
    - Minimum signal confidence validation
    """
    
    def __init__(
        self, 
        max_positions: int = 3, 
        max_risk_per_trade: float = 0.02, # 2% max risk
        max_daily_loss_pct: float = 0.05, # 5% daily kill switch
        min_confidence: float = 0.6
    ):
        self.max_positions = max_positions
        self.max_risk_per_trade = max_risk_per_trade
        self.max_daily_loss_pct = max_daily_loss_pct
        self.min_confidence = min_confidence
        
        self.daily_pnl = 0.0
        self.starting_balance = 0.0

    def set_daily_baseline(self, balance: float):
        """Should be called at the start of each day or session."""
        self.starting_balance = balance
        self.daily_pnl = 0.0

    def calculate_position_size(
        self, 
        balance: float, 
        entry_price: float, 
        stop_loss: float, 
        risk_pct: Optional[float] = None
    ) -> float:
        """
        Calculates position size based on risk amount.
        Formula: size = (balance * risk_pct) / abs(entry_price - stop_loss)
        """
        if abs(entry_price - stop_loss) < 1e-8:
            return 0.0
            
        risk_allowance = (risk_pct or self.max_risk_per_trade) * balance
        risk_per_unit = abs(entry_price - stop_loss)
        
        size = risk_allowance / risk_per_unit
        
        # Limit total position value to a reasonable fraction of capital (e.g., 20% max notional)
        # to prevent over-leveraging on tight stops.
        max_notional = balance * 0.5 # Max 50% capital in one trade
        if (size * entry_price) > max_notional:
            size = max_notional / entry_price
            
        return round(size, 8)

    def check_kill_switch(self, current_balance: float) -> bool:
        """Returns True if the daily loss limit has been hit."""
        if self.starting_balance <= 0:
            return False
            
        total_loss_pct = (self.starting_balance - current_balance) / self.starting_balance
        if total_loss_pct >= self.max_daily_loss_pct:
            logger.critical(f"KILL SWITCH TRIGGERED: Daily loss {total_loss_pct*100:.2f}% hit limit {self.max_daily_loss_pct*100:.2f}%")
            return True
        return False

    def assess_signal(
        self, 
        signal: dict, 
        current_positions: int, 
        balance: float
    ) -> dict:
        """
        Comprehensive pre-trade validation.
        Returns check results and calculated size.
        """
        status = {
            "allowed": False,
            "reason": "",
            "size": 0.0
        }
        
        # 1. Confidence Check
        if signal.get("confidence", 0) < self.min_confidence:
            status["reason"] = f"Low confidence: {signal.get('confidence')}"
            return status
            
        # 2. Daily Loss Check
        if self.check_kill_switch(balance):
            status["reason"] = "Daily loss limit hit"
            return status
            
        # 3. Position Limit Check
        if current_positions >= self.max_positions:
            status["reason"] = f"Max positions reached: {current_positions}"
            return status
            
        # 4. SL Check
        if not signal.get("stop_loss"):
            status["reason"] = "Mandatory Stop-Loss missing"
            return status
            
        # 5. Position Sizing
        size = self.calculate_position_size(
            balance=balance,
            entry_price=signal["price"],
            stop_loss=signal["stop_loss"]
        )
        
        if size <= 0:
            status["reason"] = "Calculated size is zero"
            return status
            
        status["allowed"] = True
        status["size"] = size
        return status
