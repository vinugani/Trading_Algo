from __future__ import annotations
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from delta_exchange_bot.strategy.base import Signal, Strategy

class EnhancedRSIScalping(Strategy):
    """
    Production-Grade RSI Scalping Strategy.
    Filters:
    - Trend: Price must be above/below EMA 50.
    - Volume: Volume must be above 20-period SMA.
    - Volatility: ATR used for stop-loss distance.
    - RSI: Standard overbought/oversold.
    """
    
    def __init__(
        self,
        rsi_period: int = 14,
        ema_period: int = 50,
        vol_sma_period: int = 20,
        atr_period: int = 14,
        rsi_overbought: float = 70.0,
        rsi_oversold: float = 30.0,
        min_confidence: float = 0.6
    ):
        self.rsi_period = rsi_period
        self.ema_period = ema_period
        self.vol_sma_period = vol_sma_period
        self.atr_period = atr_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.min_confidence = min_confidence

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # EMA Trend
        df['ema'] = df['close'].rolling(window=self.ema_period).mean()
        
        # Volume SMA
        df['vol_sma'] = df['volume'].rolling(window=self.vol_sma_period).mean()
        
        # ATR (Simplified)
        df['tr'] = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift(1)),
                abs(df['low'] - df['close'].shift(1))
            )
        )
        df['atr'] = df['tr'].rolling(window=self.atr_period).mean()
        
        return df

    def generate(self, market_data: Dict[str, Dict]) -> List[Signal]:
        signals = []
        
        for symbol, data in market_data.items():
            # In production, market_data should contain OHLCV DataFrames
            df = data.get("df")
            if df is None or len(df) < self.ema_period:
                continue
            
            df = self._calculate_indicators(df)
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            price = latest['close']
            rsi = latest['rsi']
            ema = latest['ema']
            vol = latest['volume']
            vol_sma = latest['vol_sma']
            atr = latest['atr']
            
            action = "hold"
            confidence = 0.0
            sl = None
            tp = None
            
            # Condition 1: Long
            # RSI oversold + Price above EMA (Trend) + Volume (> SMA)
            if rsi < self.rsi_oversold and price > ema and vol > vol_sma:
                action = "buy"
                # Scoring: further from oversold + volume strength
                confidence = (self.rsi_oversold - rsi) / self.rsi_oversold + (vol / vol_sma - 1) * 0.2
                sl = price - (atr * 2.0)
                tp = price + (atr * 4.0) # 1:2 RR
                
            # Condition 2: Short
            elif rsi > self.rsi_overbought and price < ema and vol > vol_sma:
                action = "sell"
                confidence = (rsi - self.rsi_overbought) / (100 - self.rsi_overbought) + (vol / vol_sma - 1) * 0.2
                sl = price + (atr * 2.0)
                tp = price - (atr * 4.0) # 1:2 RR
            
            confidence = min(1.0, max(0.0, confidence))
            
            if action != "hold" and confidence >= self.min_confidence:
                signals.append(Signal(
                    symbol=symbol,
                    action=action,
                    confidence=confidence,
                    price=price,
                    stop_loss=sl,
                    take_profit=tp
                ))
            else:
                signals.append(Signal(symbol=symbol, action="hold", confidence=0.0, price=price))
                
        return signals
