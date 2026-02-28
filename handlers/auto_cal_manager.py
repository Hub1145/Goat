import math
import time
import threading
from handlers.utils import safe_float

class AutoCalManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.lock = threading.Lock()
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0
        self.auto_add_step_count = 0
        self.last_add_price = 0.0
        self.last_order_time = 0

    def calculate_need_add_metrics(self):
        with self.lock:
            self._calculate_need_add_metrics_internal()

    def _calculate_need_add_metrics_internal(self):
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0

        # Avoid calculation with default/stale product info
        if not self.engine.product_info.get('is_loaded'):
            return

        mkt = self.engine.latest_trade_price
        if mkt <= 0: return

        for side in ['long', 'short']:
            if self.engine.in_position[side]:
                entry = self.engine.position_entry_price[side]
                qty = abs(self.engine.position_qty[side]) # qty is in contracts
                if entry <= 0 or qty <= 0: continue

                # contractSize is multiplier (e.g. 0.1 for BTC).
                # Notional = contracts * entry * size
                contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))

                initial_notional = qty * entry * contract_size
                notional = qty * mkt * contract_size

                # Recovery % (e.g. 0.6 -> 0.006)
                rec_val = self.config.get('add_pos_recovery_percent', 0.6)
                rec = max(0.1, rec_val) / 100.0

                fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                mult = self.config.get('add_pos_profit_multiplier', 1.5)

                # Costs in USDT
                current_fees = self.engine.position_manager.current_entry_fees[side]
                realized_loss = self.engine.position_manager.realized_loss_this_cycle[side]
                costs = current_fees + realized_loss

                K = fee_pct

                # Mode 1: Above Zero (Target Net = 0)
                # Denominator: (mkt * size * (1 - K)) - (mkt * size * (1 + rec))? No.
                # Simplified formula: V = [Costs + Notional_initial - Notional_current*(1+rec-K)] / [rec - 2K]
                denom_zero = rec - 2*K
                if abs(denom_zero) < 0.0001: denom_zero = 0.0001

                if side == 'long':
                    numerator_zero = costs + initial_notional - notional * (1 + rec - K)
                else:
                    numerator_zero = costs + notional * (1 - rec + K) - initial_notional

                val_zero = numerator_zero / denom_zero
                if val_zero > 0:
                    # Correction for "Wrong Dot place":
                    # The formula returns required additional NOTIONAL.
                    self.need_add_usdt_above_zero += val_zero

                # Mode 2: Profit Target & Close
                denom_profit = rec - K * (mult + 2)
                if abs(denom_profit) < 0.0001: denom_profit = 0.0001

                if side == 'long':
                    numerator_profit = costs + initial_notional * (1 + K * mult) - notional * (1 + rec - K)
                else:
                    numerator_profit = costs + notional * (1 - rec + K) - initial_notional * (1 - K * mult)

                val_profit = numerator_profit / denom_profit
                if val_profit > 0:
                    self.need_add_usdt_profit_target += val_profit

    def check_auto_exit(self, net_pnl, unrealized_pnl):
        notional = self.engine.cached_pos_notional
        if notional <= 0: return False, ""

        fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
        # Use aggregate fees for thresholds
        used_fees = sum(self.engine.position_manager.current_entry_fees.values())
        size_fees = notional * fee_pct

        # 1. Above Zero (Mode 1)
        if self.config.get('use_add_pos_above_zero') and net_pnl >= 0:
            return True, "Above Zero Target Met (Mode 1)"

        # 2. Profit Target (Mode 2)
        if self.config.get('use_add_pos_profit_target'):
            mult = self.config.get('add_pos_profit_multiplier', 1.5)
            # Match user expectation: Net Profit = One-way Fee * Multiplier
            # So Target Unrealized PnL = Total Cycle Fees + Estimated Exit Fee + (One-way Fee * Multiplier)
            one_way_fee = notional * fee_pct
            total_cycle_fees = sum(self.engine.position_manager.current_entry_fees.values())
            target = total_cycle_fees + one_way_fee + (one_way_fee * mult)

            if unrealized_pnl >= target:
                return True, f"Profit Target Met (Mode 2: Net > {one_way_fee * mult:.2f})"

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
        with self.lock:
            if not any(self.config.get(k) for k in ['use_add_pos_auto_cal', 'use_add_pos_above_zero', 'use_add_pos_profit_target']): return

            # Lockout to prevent rapid-fire adds before position sync
            if time.time() - self.last_order_time < 10: return

            mkt = self.engine.latest_trade_price
            if not mkt: return

            any_in_pos = False
            for side in ['long', 'short']:
                if self.engine.in_position[side]:
                    any_in_pos = True
                    # Robust initialization of last_add_price
                    if self.last_add_price == 0:
                        self.last_add_price = self.engine.position_entry_price[side]
                        if self.last_add_price == 0: continue

                    gap_threshold = float(self.config.get('add_pos_gap_threshold', 5.0))
                    gap_offset = float(self.config.get('add_pos_gap_offset', 0.0))
                    gap = gap_threshold + (self.auto_add_step_count * gap_offset)

                    price_diff = (self.last_add_price - mkt) if side == 'long' else (mkt - self.last_add_price)

                    if price_diff >= gap:
                        self.engine.log(f"Auto-Add Gap Triggered: {side} position, last add {self.last_add_price}, mkt {mkt}, gap {gap:.2f}")
                        if self._execute_add(side, mkt):
                            self.last_add_price = mkt
                            break # Only one add per check loop to maintain sanity

            if not any_in_pos:
                self.auto_add_step_count = 0
                self.last_add_price = 0.0

    def _execute_add(self, side, price):
        # IMPORTANT: Auto-Cal recovery orders bypass budget and min order amount restrictions
        is_recovery = False
        target_notional = 0.0
        if self.config.get('use_add_pos_profit_target') and self.need_add_usdt_profit_target > 0:
            target_notional = max(target_notional, self.need_add_usdt_profit_target)
            is_recovery = True
        if self.config.get('use_add_pos_above_zero') and self.need_add_usdt_above_zero > 0:
            target_notional = max(target_notional, self.need_add_usdt_above_zero)
            is_recovery = True

        max_adds = int(self.config.get('add_pos_max_count', 10))
        if self.auto_add_step_count >= max_adds:
            self.engine.log(f"Auto-Add: Max steps reached ({self.auto_add_step_count}/{max_adds}). Skipping.", level="info")
            return False

        current_notional = self.engine.position_manager.position_notional[side]
        # Calculate size based on percentage
        pct_base = float(self.config.get('add_pos_size_pct', 5.0))
        pct_offset = float(self.config.get('add_pos_size_pct_offset', 0.0))
        pct = (pct_base + (self.auto_add_step_count * pct_offset)) / 100.0

        sz_pct_notional = current_notional * pct
        final_notional = max(sz_pct_notional, target_notional)

        self.engine.log(f"Auto-Add Calc: Current {current_notional:.2f}, Pct {pct*100:.1f}% -> {sz_pct_notional:.2f}. Recovery Target {target_notional:.2f}. Final {final_notional:.2f}")

        if not is_recovery:
            # Standard Auto-Add (Percentage based only) follows restrictions
            remaining = self.engine.remaining_amount_notional
            if final_notional > remaining:
                self.engine.log(f"Auto-Add notional {final_notional:.2f} exceeds remaining capacity {remaining:.2f}. Capping.", level="warning")
                final_notional = remaining

            if final_notional < self.config.get('min_order_amount', 10.0):
                self.engine.log(f"Auto-Add notional {final_notional:.2f} below min_order_amount. Skipping.", level="info")
                return False
        else:
            self.engine.log("Auto-Cal Recovery Order: Bypassing budget and min-order restrictions.", level="info")

        contract_multiplier = safe_float(self.engine.product_info.get('contractSize', 1.0))
        sz = final_notional / (price * contract_multiplier)

        # Apply quantity precision and step size
        lot_sz = safe_float(self.engine.product_info.get('qtyStepSize', 1.0))
        sz = round(math.floor(sz / lot_sz) * lot_sz, 8)

        if sz < safe_float(self.engine.product_info.get('minOrderQty', 0)):
            self.engine.log(f"Auto-Add quantity {sz} is below minOrderQty (Target Notional {final_notional:.2f}). Skipping.", level="info")
            return False

        tp, sl = self.engine.order_manager._calculate_tpsl_prices(side, price)

        # Step 2 Exit Offset Override (Relative to New Average Entry)
        step2 = safe_float(self.config.get('add_pos_step2_offset'), 0)
        if step2 > 0:
            p_prec = self.engine.product_info.get('pricePrecision', 2)
            entry = self.engine.position_entry_price[side]
            qty = abs(self.engine.position_qty[side])
            # Estimate new average entry
            new_total_qty = qty + sz
            if new_total_qty > 0:
                new_avg_entry = ((qty * entry) + (sz * price)) / new_total_qty
                if side == 'long': tp = round(new_avg_entry + step2, p_prec)
                else: tp = round(new_avg_entry - step2, p_prec)
                self.engine.log(f"Auto-Add Step 2: New Avg Entry Est {new_avg_entry:.4f}, TP set at {tp:.4f} (Offset {step2})")

        if self.engine.order_manager.place_order(self.config['symbol'], "buy" if side == "long" else "sell", sz,
                                                 order_type="Market", posSide=side, take_profit_price=tp, stop_loss_price=sl):
            self.auto_add_step_count += 1
            self.last_order_time = time.time()
            return True
        return False
