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

    def _round_to_step(self, value, step):
        if not step or step <= 0: return value
        # Use decimal-safe rounding
        precision = 0
        if '.' in str(step):
            precision = len(str(step).split('.')[-1].rstrip('0'))

        rounded = round(round(value / step) * step, precision)
        # Final check to ensure we don't return 0.0000000000001
        return float(f"{rounded:.{precision}f}")

    def place_order(self, symbol, side, qty, price=None, order_type="Market",
                    reduce_only=False, stop_loss_price=None, take_profit_price=None, posSide=None, verbose=True):
        try:
            path = "/api/v5/trade/order"

            # Apply precision and step size rounding
            q_step = safe_float(self.engine.product_info.get('qtyStepSize', 1.0))
            p_step = safe_float(self.engine.product_info.get('priceTickSize', 0.01))
            q_prec = self.engine.product_info.get('qtyPrecision', 0)
            p_prec = self.engine.product_info.get('pricePrecision', 2)

            qty = self._round_to_step(qty, q_step)
            if qty <= 0:
                self.engine.log(f"Invalid order quantity after rounding: {qty}", level="warning")
                return None

            body = {
                "instId": symbol,
                "tdMode": self.config.get('mode', 'cross'),
                "side": side.lower(),
                "ordType": order_type.lower(),
                "sz": f"{qty:.{q_prec}f}" if q_prec > 0 else str(int(qty))
            }

            if self.config.get('okx_pos_mode') == 'long_short_mode' and posSide:
                body["posSide"] = posSide

            if order_type.lower() == "limit" and price is not None:
                price = self._round_to_step(price, p_step)
                body["px"] = f"{price:.{p_prec}f}"

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
            else:
                msg = res.get('msg') if res else 'No Response'
                code = res.get('code') if res else 'N/A'

                # Extract detailed error from data if available
                detail_msg = ""
                if res and 'data' in res and isinstance(res['data'], list) and len(res['data']) > 0:
                    d = res['data'][0]
                    if 'sMsg' in d:
                        detail_msg = f" | Detail: {d.get('sMsg')} (sCode: {d.get('sCode')})"

                # Log more details for non-zero codes to help debugging
                self.engine.log(f"Order failed: {msg}{detail_msg} (Code: {code}). Request: sz={body.get('sz')}, px={body.get('px', 'MKT')}, side={body.get('side')}, algo={bool(body.get('attachAlgoOrds'))}", level="error")
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

            leverage = safe_float(self.config.get('leverage', 1), 1.0)
            equity = self.engine.total_equity
            max_allowed = min(float(self.config.get('max_allowed_used', 1000)), equity if equity > 0 else 1000000)

            rate_divisor = max(1, self.config.get('rate_divisor', 1))
            capacity = (max_allowed / rate_divisor) * leverage
            remaining = capacity - self.engine.position_manager.used_amount_notional

            target = self.config.get('target_order_amount', 100)
            if remaining < self.config.get('min_order_amount', 10): break

            trade_amt = min(target, remaining)
            qty = trade_amt / (price * self.engine.product_info.get('contractSize', 1.0))

            if qty < safe_float(self.engine.product_info.get('minOrderQty', 0)): continue

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
        # instType is required. instId is optional but recommended.
        params = {"instType": "SWAP", "instId": symbol}
        res = self.engine.okx_client.request("GET", "/api/v5/trade/orders-algo-pending", params=params)

        # If 400 with code 51000, it might be an issue with instId/instType combination
        if res and res.get('code') == '51000':
             # Try without instId, just instType
             res = self.engine.okx_client.request("GET", "/api/v5/trade/orders-algo-pending", params={"instType": "SWAP"})

        return res.get('data', []) if res and res.get('code') == '0' else []

    def place_position_tpsl(self, side, entry_price):
        if not entry_price: return

        # Respect 'null' or 0 as 'disabled' as requested
        tp_offset_cfg = self.config.get('tp_price_offset')
        sl_offset_cfg = self.config.get('sl_price_offset')

        tp_offset = safe_float(tp_offset_cfg) if tp_offset_cfg is not None else 0
        sl_offset = safe_float(sl_offset_cfg) if sl_offset_cfg is not None else 0

        tp_price = 0.0
        sl_price = 0.0
        p_prec = self.engine.product_info.get('pricePrecision', 2)

        if side == 'long':
            if tp_offset > 0: tp_price = round(entry_price + tp_offset, p_prec)
            if sl_offset > 0: sl_price = round(entry_price - sl_offset, p_prec)
        else:
            if tp_offset > 0: tp_price = round(entry_price - tp_offset, p_prec)
            if sl_offset > 0: sl_price = round(entry_price + sl_offset, p_prec)

        if tp_price > 0 or sl_price > 0:
            qty = abs(self.engine.position_qty[side])
            if qty > 0:
                self.engine.log(f"Placing dynamic TP/SL for {side} position: TP={tp_price}, SL={sl_price}")
                # Cancel existing first to prevent duplicates
                self.cancel_all_algo_orders(self.config['symbol'])

                if tp_price > 0:
                    body = {
                        "instId": self.config['symbol'], "tdMode": self.config.get('mode', 'cross'),
                        "side": "sell" if side == "long" else "buy", "posSide": side,
                        "ordType": "conditional", "sz": str(qty),
                        "tpTriggerPx": str(tp_price), "tpOrdPx": "-1"
                    }
                    self.engine.okx_client.request("POST", "/api/v5/trade/order-algo", body_dict=body)
                if sl_price > 0:
                    body = {
                        "instId": self.config['symbol'], "tdMode": self.config.get('mode', 'cross'),
                        "side": "sell" if side == "long" else "buy", "posSide": side,
                        "ordType": "conditional", "sz": str(qty),
                        "slTriggerPx": str(sl_price), "slOrdPx": "-1"
                    }
                    self.engine.okx_client.request("POST", "/api/v5/trade/order-algo", body_dict=body)

    def cancel_all_algo_orders(self, symbol):
        algos = self.fetch_algo_orders(symbol)
        if algos:
            body = [{"instId": symbol, "algoId": a['algoId']} for a in algos]
            return self.engine.okx_client.request("POST", "/api/v5/trade/cancel-algos", body_dict=body)
        return True

    def batch_modify_tpsl(self, symbol):
        self.engine.log("Executing Batch Modify TP/SL for all positions")
        for side, in_pos in self.engine.in_position.items():
            if in_pos:
                entry = self.engine.position_entry_price[side]
                self.place_position_tpsl(side, entry)

    def sync_open_orders(self, symbol):
        res = self.engine.okx_client.request("GET", "/api/v5/trade/orders-pending", params={"instType": "SWAP", "instId": symbol})
        if res and res.get('code') == '0':
            raw_orders = res.get('data', [])
            formatted = []
            now_ms = time.time() * 1000
            limit = self.config.get('cancel_unfilled_seconds', 30)

            current_ids = set()
            for o in raw_orders:
                oid = o.get('ordId')
                current_ids.add(oid)
                c_time = safe_float(o.get('cTime'))
                time_left = None
                if c_time > 0:
                    elapsed = (now_ms - c_time) / 1000
                    time_left = max(0, int(limit - elapsed))

                # Map OKX fields to dashboard fields
                formatted.append({
                    'id': oid,
                    'type': o.get('side', '').upper(),
                    'posSide': o.get('posSide'),
                    'entry_spot_price': safe_float(o.get('px')),
                    'stake': abs(safe_float(o.get('sz'))) * safe_float(o.get('px')) * safe_float(self.engine.product_info.get('contractSize', 1.0)),
                    'tp_price': safe_float(o.get('tpTriggerPx')),
                    'sl_price': safe_float(o.get('slTriggerPx')),
                    'time_left': time_left,
                    'ordId': oid,
                    'cTime': c_time
                })
            self.open_trades = formatted
            with self.engine.lock:
                self.pending_entry_ids &= current_ids
        return self.open_trades

    def check_unfilled_timeouts(self):
        limit = self.config.get('cancel_unfilled_seconds', 0)
        now_ms = time.time() * 1000
        mkt = self.engine.latest_trade_price

        cancel_tp_below = self.config.get('cancel_on_tp_price_below_market')
        cancel_tp_above = self.config.get('cancel_on_tp_price_above_market')
        cancel_ent_below = self.config.get('cancel_on_entry_price_below_market')
        cancel_ent_above = self.config.get('cancel_on_entry_price_above_market')

        to_cancel = []
        reasons = []

        for o in self.open_trades:
            # We generally only auto-cancel ENTRY orders based on these conditions
            if o['ordId'] not in self.pending_entry_ids: continue

            # 1. Time-based cancel
            c_time = o.get('cTime')
            if limit > 0 and c_time and (now_ms - c_time) > (limit * 1000):
                to_cancel.append(o['ordId'])
                reasons.append(f"Timeout ({limit}s)")
                continue

            # 2. Condition-based cancel
            if mkt <= 0: continue

            ent = o.get('entry_spot_price', 0)
            tp = o.get('tp_price', 0)
            if tp <= 0:
                side = 'long' if o.get('type') == 'BUY' else 'short'
                tp_off = safe_float(self.config.get('tp_price_offset'))
                if tp_off > 0:
                    tp = (ent + tp_off) if side == 'long' else (ent - tp_off)

            if cancel_ent_below and ent > 0 and ent < mkt:
                to_cancel.append(o['ordId']); reasons.append(f"Entry {ent} < Market {mkt}")
            elif cancel_ent_above and ent > 0 and ent > mkt:
                to_cancel.append(o['ordId']); reasons.append(f"Entry {ent} > Market {mkt}")
            elif cancel_tp_below and tp > 0 and tp < mkt:
                to_cancel.append(o['ordId']); reasons.append(f"TP {tp} < Market {mkt}")
            elif cancel_tp_above and tp > 0 and tp > mkt:
                to_cancel.append(o['ordId']); reasons.append(f"TP {tp} > Market {mkt}")

        if to_cancel:
            for i, oid in enumerate(to_cancel):
                self.engine.log(f"Auto-canceling order {oid}: {reasons[i]}", level="info")
            self.batch_cancel_orders(self.config['symbol'], to_cancel)
            with self.engine.lock:
                self.pending_entry_ids -= set(to_cancel)
