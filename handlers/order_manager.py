import threading
import time
import math
from handlers.utils import safe_float

class OrderManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.reset()

    def reset(self):
        self.pending_entry_ids = set()
        self.position_exit_orders = {'long': {}, 'short': {}}
        self.open_trades = []
        self.batch_counter = 0

    def place_order(self, symbol, side, qty, price=None, order_type="Market",
                    reduce_only=False, stop_loss_price=None, take_profit_price=None, posSide=None, verbose=True):
        try:
            path = "/api/v5/trade/order"
            q_prec = self.engine.product_info.get('qtyPrecision', 8)
            p_prec = self.engine.product_info.get('pricePrecision', 4)
            body = {"instId": symbol, "tdMode": self.config.get('mode', 'cross'), "side": side.lower(), "ordType": order_type.lower(), "sz": f"{qty:.{q_prec}f}"}
            if self.config.get('okx_pos_mode') == 'long_short_mode' and posSide: body["posSide"] = posSide
            if order_type.lower() == "limit" and price is not None: body["px"] = f"{price:.{p_prec}f}"
            if reduce_only: body["reduceOnly"] = True

            algo_list = []
            algo = {"posSide": body.get("posSide", "net")}
            has_algo = False
            if take_profit_price and safe_float(take_profit_price) > 0:
                algo.update({"tpTriggerPx": str(take_profit_price), "tpOrdPx": "-1", "tpTriggerPxType": "last"})
                has_algo = True
            if stop_loss_price and safe_float(stop_loss_price) > 0:
                algo.update({"slTriggerPx": str(stop_loss_price), "slOrdPx": "-1", "slTriggerPxType": "last"})
                has_algo = True
            if has_algo:
                algo_list.append(algo)
                body["attachAlgoOrds"] = algo_list

            if verbose: self.engine.log(f"Placing {order_type} {side} order for {qty} {symbol}")
            res = self.engine.okx_client.request("POST", path, body_dict=body)
            if res and res.get('code') == '0':
                return res.get('data', [{}])[0]
            return None
        except Exception as e:
            self.engine.log(f"Order fail: {e}", level="error")
            return None

    def initiate_entry_batch(self, initial_limit_price, side, batch_size):
        batch_offset = self.config.get('batch_offset', 0)
        self.batch_counter += 1

        for i in range(batch_size):
            price = initial_limit_price
            if i > 0:
                price = (price - (batch_offset * i)) if side == 'long' else (price + (batch_offset * i))

            if price <= 0: continue

            # Logic for sizing from original code
            leverage = safe_float(self.config.get('leverage', 1), 1.0)
            equity = self.engine.total_equity
            max_allowed = min(float(self.config.get('max_allowed_used', 1000)), equity if equity > 0 else 1000000)

            rate_divisor = max(1, self.config.get('rate_divisor', 1))
            capacity = (max_allowed / rate_divisor) * leverage
            remaining = capacity - self.engine.position_manager.used_amount_notional

            target = self.config.get('target_order_amount', 100)
            if remaining < self.config.get('min_order_amount', 10): break

            trade_amt = min(target, remaining)
            qty_contracts = trade_amt / (price * self.engine.product_info.get('contractSize', 1.0))

            # Precise rounding
            lot_sz = self.engine.product_info.get('qtyStepSize', 1.0)
            qty = math.floor(qty_contracts / lot_sz) * lot_sz

            if qty < self.engine.product_info.get('minOrderQty', 0): continue

            order = self.place_order(self.config['symbol'], "buy" if side == 'long' else "sell", qty, price, order_type="Limit", posSide=side)
            if order:
                self.pending_entry_ids.add(order['ordId'])

    def cancel_order(self, symbol, order_id, reason=None):
        return self.engine.okx_client.request("POST", "/api/v5/trade/cancel-order", body_dict={"instId": symbol, "ordId": order_id})

    def batch_cancel_orders(self, symbol, order_ids):
        if not order_ids: return True
        body = [{"instId": symbol, "ordId": oid} for oid in order_ids]
        return self.engine.okx_client.request("POST", "/api/v5/trade/cancel-batch-orders", body_dict=body)

    def fetch_algo_orders(self, symbol):
        res = self.engine.okx_client.request("GET", "/api/v5/trade/orders-algo-pending", params={"instType": "SWAP", "instId": symbol})
        return res.get('data', []) if res and res.get('code') == '0' else []
