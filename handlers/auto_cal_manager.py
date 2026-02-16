import math
import time
from handlers.utils import safe_float

class AutoCalManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0
        self.auto_add_step_count = 0
        self.last_add_price = 0.0
        self.last_order_time = 0

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

                contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))
                initial_notional = qty * entry * contract_size
                notional = qty * mkt * contract_size

                rec = self.config.get('add_pos_recovery_percent', 0.6) / 100.0
                fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                mult = self.config.get('add_pos_profit_multiplier', 1.5)

                current_fees = self.engine.position_manager.current_entry_fees

                # Refined Formula: Incorporate current_entry_fees and exit fees
                K_entry_exit = fee_pct * 2

                # Mode 1: To make PnL always above 0 (Above Zero)
                if rec > K_entry_exit:
                    if side == 'long':
                        val_zero = (current_fees + initial_notional - notional * (1 + rec - K_entry_exit)) / (rec - K_entry_exit)
                    else:
                        val_zero = (current_fees - initial_notional + notional * (1 - rec + K_entry_exit)) / (rec - K_entry_exit)

                    if val_zero > 0:
                        self.need_add_usdt_above_zero += val_zero

                # Mode 2: To reach Profit Target & Close
                # User math: Target UPL = notional * fee_pct * mult
                K_target = fee_pct * mult
                if rec > K_target:
                    # Solving for val where: (qty + val/(mkt*csize)) * mkt * csize * (+/-rec) = (notional + val) * K_target
                    # (+/-rec) is just rec here as we use mkt*(1+/-rec)
                    # (notional + val) * rec = (notional + val) * K_target + initial_notional - notional + current_fees
                    # (notional + val) * (rec - K_target) = initial_notional - notional + current_fees
                    if side == 'long':
                        val_profit = (current_fees + initial_notional - notional * (1 + rec - K_target)) / (rec - K_target)
                    else:
                        val_profit = (current_fees - initial_notional + notional * (1 - rec + K_target)) / (rec - K_target)

                    if val_profit > 0:
                        self.need_add_usdt_profit_target += val_profit

    def check_auto_exit(self, net_pnl, unrealized_pnl):
        notional = self.engine.cached_pos_notional
        if notional <= 0: return False, ""

        fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
        used_fees = self.engine.position_manager.current_entry_fees
        size_fees = notional * fee_pct

        # 1. Above Zero (Mode 1)
        if self.config.get('use_add_pos_above_zero') and net_pnl >= 0:
            return True, "Above Zero Target Met (Mode 1)"

        # 2. Profit Target (Mode 2)
        if self.config.get('use_add_pos_profit_target'):
            mult = self.config.get('add_pos_profit_multiplier', 1.5)
            # Match user math: Target = notional * fee_pct * multiplier
            target = notional * fee_pct * mult
            if unrealized_pnl >= target:
                return True, "Profit Target Met (Mode 2)"

        # 3. Auto-Manual Threshold
        if self.config.get('use_pnl_auto_manual'):
            threshold = self.config.get('pnl_auto_manual_threshold', 100.0)
            if unrealized_pnl >= threshold:
                return True, f"Manual PnL Threshold {threshold} Met"

        # 4. Auto-Cal Profit (Based on Entry Fees)
        if self.config.get('use_pnl_auto_cal'):
            times = self.config.get('pnl_auto_cal_times', 1.2)
            if unrealized_pnl >= (used_fees * times):
                return True, f"Auto-Cal Profit Met ({times}x Entry Fees)"

        # 5. Auto-Cal Loss (Based on Entry Fees)
        if self.config.get('use_pnl_auto_cal_loss'):
            times = self.config.get('pnl_auto_cal_loss_times', 15.0)
            if unrealized_pnl <= -(used_fees * times):
                return True, f"Auto-Cal Loss Met ({times}x Entry Fees)"

        # 6. Size Auto-Cal Profit (Based on Current Notional Fee)
        if self.config.get('use_size_auto_cal'):
            times = self.config.get('size_auto_cal_times', 2.0)
            if unrealized_pnl >= (size_fees * times):
                return True, f"Size Auto-Cal Profit Met ({times}x Size Fees)"

        # 7. Size Auto-Cal Loss (Based on Current Notional Fee)
        if self.config.get('use_size_auto_cal_loss'):
            times = self.config.get('size_auto_cal_loss_times', 1.5)
            if unrealized_pnl <= -(size_fees * times):
                return True, f"Size Auto-Cal Loss Met ({times}x Size Fees)"

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

        # Lockout to prevent rapid-fire adds before position sync
        if time.time() - self.last_order_time < 10: return

        side = 'long' if self.engine.in_position['long'] else ('short' if self.engine.in_position['short'] else None)
        if not side:
            self.auto_add_step_count = 0
            self.last_add_price = 0.0
            return

        mkt = self.engine.latest_trade_price
        if not mkt: return

        # Robust initialization of last_add_price
        if self.last_add_price == 0:
            self.last_add_price = self.engine.position_entry_price[side]
            if self.last_add_price == 0: return # Still waiting for sync

        gap_threshold = float(self.config.get('add_pos_gap_threshold', 5.0))
        gap_offset = float(self.config.get('add_pos_gap_offset', 0.0))
        gap = gap_threshold + (self.auto_add_step_count * gap_offset)

        price_diff = (self.last_add_price - mkt) if side == 'long' else (mkt - self.last_add_price)

        if price_diff >= gap:
            self.engine.log(f"Auto-Add Gap Triggered: {side} position, last add {self.last_add_price}, mkt {mkt}, gap {gap:.2f}")
            self.last_add_price = mkt
            self._execute_add(side, mkt)

    def _execute_add(self, side, price):
        max_adds = int(self.config.get('add_pos_max_count', 10))
        if self.auto_add_step_count >= max_adds:
            self.engine.log(f"Auto-Add: Max steps reached ({self.auto_add_step_count}/{max_adds}). Skipping.", level="info")
            return

        current_notional = self.engine.position_manager.position_notional[side]
        # Calculate size based on percentage
        pct_base = float(self.config.get('add_pos_size_pct', 5.0))
        pct_offset = float(self.config.get('add_pos_size_pct_offset', 0.0))
        pct = (pct_base + (self.auto_add_step_count * pct_offset)) / 100.0

        sz_pct_notional = current_notional * pct

        # Calculate size based on Need Add metrics if enabled
        # Note: self.need_add_usdt_profit_target and self.need_add_usdt_above_zero are updated in calculate_need_add_metrics
        target_notional = 0.0
        if self.config.get('use_add_pos_profit_target'):
            target_notional = max(target_notional, self.need_add_usdt_profit_target)
        if self.config.get('use_add_pos_above_zero'):
            target_notional = max(target_notional, self.need_add_usdt_above_zero)

        final_notional = max(sz_pct_notional, target_notional)
        self.engine.log(f"Auto-Add Calculation: Current {current_notional:.2f}, Pct {pct*100:.1f}% -> {sz_pct_notional:.2f}. Need-Add target {target_notional:.2f}. Final target {final_notional:.2f}")

        # Limit by remaining capacity
        remaining = self.engine.remaining_amount_notional
        if final_notional > remaining:
            self.engine.log(f"Auto-Add notional {final_notional:.2f} exceeds remaining capacity {remaining:.2f}. Capping.", level="warning")
            final_notional = remaining

        if final_notional < self.config.get('min_order_amount', 10.0):
            self.engine.log(f"Auto-Add notional {final_notional:.2f} is below min_order_amount. Skipping.", level="info")
            return

        sz = final_notional / (price * self.engine.product_info.get('contractSize', 1.0))

        # Apply quantity precision and step size
        lot_sz = safe_float(self.engine.product_info.get('qtyStepSize', 1.0))
        sz = math.floor(sz / lot_sz) * lot_sz

        if sz < safe_float(self.engine.product_info.get('minOrderQty', 0)):
            self.engine.log(f"Auto-Add quantity {sz} is below minOrderQty. Skipping.", level="info")
            return

        if self.engine.order_manager.place_order(self.config['symbol'], "buy" if side == "long" else "sell", sz, order_type="Market", posSide=side):
            self.auto_add_step_count += 1
            self.last_order_time = time.time()
