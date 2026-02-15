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
                qty = abs(self.engine.position_qty[side])
                if entry <= 0 or qty <= 0: continue

                initial_notional = qty * entry
                notional = qty * mkt

                rec = self.config.get('add_pos_recovery_percent', 0.6) / 100.0
                fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                mult = self.config.get('add_pos_profit_multiplier', 1.5)

                # Minimum margin to cover fees (Entry + Exit)
                K_zero = fee_pct * 2
                # Target margin for profit
                K_profit = fee_pct * (mult + 2)

                if rec > K_zero:
                    # Mode 1: To make PnL always above 0
                    if side == 'long':
                        val_zero = (initial_notional - notional * (1 + rec - K_zero)) / (rec - K_zero)
                    else:
                        val_zero = (initial_notional - notional * (1 - rec + K_zero)) / (K_zero - rec)
                        # Re-check Short Zero:
                        # qM(rec-K) = -(initial - notional(1-rec+K)) = -initial + notional(1-rec+K)
                        # val_zero = (notional * (1 - rec + K_zero) - initial_notional) / (rec - K_zero)

                    if side == 'short': # Correcting Short formula to be positive
                        val_zero = (notional * (1 - rec + K_zero) - initial_notional) / (rec - K_zero)

                    if val_zero > 0:
                        self.need_add_usdt_above_zero += val_zero

                if rec > K_profit:
                    # Mode 2: To reach Profit Target & Close
                    target_pnl = initial_notional * K_profit # Desired profit based on current initial cost
                    if side == 'long':
                        val_profit = (target_pnl + initial_notional - notional * (1 + rec - K_profit)) / (rec - K_profit)
                    else:
                        val_profit = (target_pnl - initial_notional + notional * (1 - rec + K_profit)) / (rec - K_profit)

                    if val_profit > 0:
                        self.need_add_usdt_profit_target += val_profit

    def check_auto_exit(self, net_pnl, unrealized_pnl):
        notional = self.engine.cached_pos_notional
        if notional <= 0: return False, ""

        if self.config.get('use_add_pos_above_zero') and net_pnl >= 0:
            return True, "Above Zero Target Met (Mode 1)"

        if self.config.get('use_add_pos_profit_target'):
            mult = self.config.get('add_pos_profit_multiplier', 1.5)
            fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
            target = notional * fee_pct * (mult + 2)
            if unrealized_pnl >= target:
                return True, "Profit Target Met (Mode 2)"

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
