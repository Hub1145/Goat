import math
from handlers.utils import safe_float

class AutoCalManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0
        self.auto_add_step_count = 0
        self.last_add_price = 0.0

    def calculate_need_add_metrics(self):
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0

        mkt = self.engine.latest_trade_price
        if mkt <= 0: return

        for side in ['long', 'short']:
            if self.engine.in_position[side]:
                entry = self.engine.position_entry_price[side]
                notional = self.engine.position_manager.position_notional[side]
                if entry <= 0 or notional <= 0: continue

                rec = self.config.get('add_pos_recovery_percent', 0.6) / 100.0
                is_against = (side == 'long' and mkt <= entry) or (side == 'short' and mkt >= entry)

                if is_against:
                    if side == 'long':
                        target_be = min(mkt * (1 + rec), entry - 1e-8)
                        if (denom := target_be - mkt) > 0:
                            self.need_add_usdt_above_zero += notional * (entry - target_be) / denom
                    else:
                        target_be = max(mkt * (1 - rec), entry + 1e-8)
                        if (denom := mkt - target_be) > 0:
                            self.need_add_usdt_above_zero += notional * (target_be - entry) / denom

                    mult = self.config.get('add_pos_profit_multiplier', 1.5)
                    fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                    target_pnl = notional * fee_pct * (mult + 2)
                    qty = notional / entry
                    target_avg = (entry - (target_pnl / qty)) if side == 'long' else (entry + (target_pnl / qty))

                    if side == 'long':
                        if (denom_p := target_avg - mkt) > 0:
                            self.need_add_usdt_profit_target += notional * (entry - target_avg) / denom_p
                    else:
                        if (denom_p := mkt - target_avg) > 0:
                            self.need_add_usdt_profit_target += notional * (target_avg - entry) / denom_p

    def check_auto_exit(self, net_pnl, unrealized_pnl):
        notional = self.engine.cached_pos_notional
        if notional <= 0: return False, ""
        if self.config.get('use_add_pos_above_zero') and net_pnl >= 0: return True, "Mode 1"
        if self.config.get('use_add_pos_profit_target'):
            mult = self.config.get('add_pos_profit_multiplier', 1.5)
            fee = notional * (self.config.get('trade_fee_percentage', 0.08) / 100.0)
            if unrealized_pnl >= fee * (mult + 2): return True, "Mode 2"
        # Standard features...
        return False, ""

    def check_auto_margin(self):
        if not self.config.get('use_auto_margin'): return
        for side in ['long', 'short']:
            if self.engine.in_position[side]:
                pos = self.engine.position_manager.position_details.get(side, {})
                liqp = self.engine.position_manager.position_liq[side]
                sl = self.engine.current_stop_loss[side]
                if pos.get('mgnMode') == 'isolated' and liqp > 0 and sl > 0:
                    if (side == 'long' and liqp >= sl) or (side == 'short' and liqp <= sl):
                        amt = abs(sl - liqp) + self.config.get('auto_margin_offset', 30.0)
                        self.engine.okx_client.request("POST", "/api/v5/account/position/margin-balance", body_dict={"instId": self.config['symbol'], "posSide": pos.get('posSide', 'net'), "type": "add", "amt": str(round(amt, 2))})

    def check_auto_add(self):
        if not any(self.config.get(k) for k in ['use_add_pos_auto_cal', 'use_add_pos_above_zero', 'use_add_pos_profit_target']): return
        side = 'long' if self.engine.in_position['long'] else ('short' if self.engine.in_position['short'] else None)
        if not side: return
        mkt = self.engine.latest_trade_price
        if self.last_add_price == 0: self.last_add_price = self.engine.position_entry_price[side]
        if not mkt: return
        gap = self.config.get('add_pos_gap_threshold', 5.0) + (self.auto_add_step_count * self.config.get('add_pos_gap_offset', 0.0))
        if ((self.last_add_price - mkt) if side == 'long' else (mkt - self.last_add_price)) >= gap:
            self.last_add_price = mkt
            self._execute_add(side, mkt)

    def _execute_add(self, side, price):
        if self.auto_add_step_count >= self.config.get('add_pos_max_count', 10): return
        pct = (self.config.get('add_pos_size_pct', 5.0) + (self.auto_add_step_count * self.config.get('add_pos_size_pct_offset', 0.0))) / 100.0
        sz = (self.engine.position_manager.position_notional[side] * pct) / (price * self.engine.product_info.get('contractSize', 1.0))
        if self.engine.order_manager.place_order(self.config['symbol'], "buy" if side == "long" else "sell", sz, order_type="Market", posSide=side):
            self.auto_add_step_count += 1
