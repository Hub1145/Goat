import threading
import time
from handlers.utils import safe_float

class OrderManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config

        self.pending_entry_ids = set()
        self.position_exit_orders = {'long': {}, 'short': {}}
        self.open_trades = []
        self.lock = threading.Lock()

    def place_order(self, symbol, side, qty, price=None, order_type="Market",
                    time_in_force=None, reduce_only=False,
                    stop_loss_price=None, take_profit_price=None, posSide=None, verbose=True, tdMode=None):
        try:
            path = "/api/v5/trade/order"
            price_precision = self.engine.product_info.get('pricePrecision', 4)
            qty_precision = self.engine.product_info.get('qtyPrecision', 8)
            order_qty_str = f"{qty:.{qty_precision}f}"
            trade_mode = tdMode if tdMode else self.config.get('mode', 'cross')

            body = {"instId": symbol, "tdMode": trade_mode, "side": side.lower(), "ordType": order_type.lower(), "sz": order_qty_str}
            if self.config.get('okx_pos_mode') == 'long_short_mode' and posSide: body["posSide"] = posSide
            if order_type.lower() == "limit" and price is not None: body["px"] = f"{price:.{price_precision}f}"
            if time_in_force: body["timeInForce"] = "GTC" if time_in_force == "GoodTillCancel" else time_in_force
            if reduce_only: body["reduceOnly"] = True

            attach_algo_list = []
            algo_details = {}
            has_algo = False
            if "posSide" in body: algo_details["posSide"] = body["posSide"]
            if take_profit_price and safe_float(take_profit_price) > 0:
                algo_details.update({"tpTriggerPx": str(take_profit_price), "tpOrdPx": "-1", "tpTriggerPxType": "last"})
                has_algo = True
            if stop_loss_price and safe_float(stop_loss_price) > 0:
                algo_details.update({"slTriggerPx": str(stop_loss_price), "slOrdPx": "-1", "slTriggerPxType": "last"})
                has_algo = True
            if has_algo:
                attach_algo_list.append(algo_details)
                body["attachAlgoOrds"] = attach_algo_list

            if verbose: self.engine.log(f"Placing {order_type} {side} order for {order_qty_str} {symbol} at {price}", level="info")
            response = self.engine.okx_client.request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                order_data = response.get('data', [])
                if order_data and order_data[0].get('ordId'):
                    if verbose: self.engine.log(f"[OK] Order placed: OrderID={order_data[0]['ordId']}", level="info")
                    return order_data[0]
            return None
        except Exception as e:
            self.engine.log(f"Exception in order_manager.place_order: {e}", level="error")
            return None

    def cancel_order(self, symbol, order_id, reason=None):
        try:
            path = "/api/v5/trade/cancel-order"
            body = {"instId": symbol, "ordId": order_id}
            if reason: self.engine.log(f"Cancelling order {order_id[:12]} ({reason})...", level="info")
            response = self.engine.okx_client.request("POST", path, body_dict=body)
            return response and response.get('code') in ['0', '51001']
        except Exception as e:
            self.engine.log(f"Exception in order_manager.cancel_order: {e}", level="debug")
            return False

    def batch_cancel_orders(self, symbol, order_ids):
        if not order_ids: return True
        try:
            path = "/api/v5/trade/cancel-batch-orders"
            body = [{"instId": symbol, "ordId": oid} for oid in order_ids]
            response = self.engine.okx_client.request("POST", path, body_dict=body)
            return response and response.get('code') == '0'
        except Exception as e:
            self.engine.log(f"Exception in order_manager.batch_cancel_orders: {e}", level="error")
            return False

    def fetch_algo_orders(self, symbol):
        try:
            path = "/api/v5/trade/orders-algo-pending"
            params = {"instType": "SWAP", "instId": symbol}
            response = self.engine.okx_client.request("GET", path, params=params)
            if response and response.get('code') == '0':
                return response.get('data', [])
        except: pass
        return []
