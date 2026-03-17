from dataclasses import dataclass
from typing import Dict


@dataclass
class Order:
    order_id: str
    symbol: str
    side: str
    price: float
    size: float
    status: str


class OrderManager:
    def __init__(self):
        self._orders: Dict[str, Order] = {}

    def place_order(self, symbol: str, side: str, size: float, price: float) -> Order:
        order_id = f"{symbol}-{side}-{int(price*100)}-{int(size*1000)}"
        order = Order(order_id=order_id, symbol=symbol, side=side, price=price, size=size, status="open")
        self._orders[order_id] = order
        return order

    def update_order_status(self, order_id: str, status: str):
        if order_id in self._orders:
            self._orders[order_id].status = status

    def get_open_orders(self):
        return [o for o in self._orders.values() if o.status == "open"]
