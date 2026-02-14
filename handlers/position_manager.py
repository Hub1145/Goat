import threading
from handlers.utils import safe_float

class PositionManager:
    def __init__(self, engine):
        self.engine = engine # Orchestrator
        self.config = engine.config

        self.in_position = {'long': False, 'short': False}
        self.position_qty = {'long': 0.0, 'short': 0.0}
        self.position_entry_price = {'long': 0.0, 'short': 0.0}
        self.position_liq = {'long': 0.0, 'short': 0.0}
        self.position_details = {'long': {}, 'short': {}}

        self.session_baseline_qty = {'long': 0.0, 'short': 0.0}
        self.account_balance = 0.0
        self.total_balance = 0.0
        self.total_equity = 0.0
        self.available_balance = 0.0

        self.cached_active_positions_count = 0
        self.cached_pos_notional = 0.0
        self.cached_unrealized_pnl = 0.0
        self.used_amount_notional = 0.0

        self.lock = threading.Lock()

    def process_positions(self, positions_data, is_snapshot=True):
        temp_active_count = 0
        temp_pos_notional = 0.0
        temp_unrealized_pnl = 0.0
        temp_used_notional = 0.0

        with self.lock:
            prev_qtys = {k: v for k, v in self.position_qty.items()}
            target_symbol = self.config['symbol'].strip().upper()
            found_sides = set()
            contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))
            if contract_size <= 0: contract_size = 1.0

            for pos in positions_data:
                pos_inst_id = pos.get('instId', '').strip().upper()
                if pos_inst_id == target_symbol:
                    qty_raw = safe_float(pos.get('pos'))
                    if qty_raw == 0 and is_snapshot: continue

                    current_mkt_price = self.engine.latest_trade_price if self.engine.latest_trade_price else safe_float(pos.get('avgPx'))
                    pos_sz_notional = abs(qty_raw) * current_mkt_price * contract_size

                    raw_side = pos.get('posSide', 'net')
                    side_key = self._map_side(raw_side)

                    if qty_raw != 0:
                        found_sides.add(side_key)
                        if self.engine.is_running:
                            session_qty = max(0, abs(qty_raw * contract_size) - self.session_baseline_qty.get(side_key, 0.0))
                            temp_used_notional += session_qty * current_mkt_price
                        temp_pos_notional += pos_sz_notional
                        temp_unrealized_pnl += safe_float(pos.get('upl', '0'))
                        temp_active_count += 1
                        new_qty = qty_raw * contract_size

                        if abs(new_qty - prev_qtys.get(side_key, 0.0)) > 1e-6:
                            self.engine.log(f"Position update [{side_key.upper()}]: {prev_qtys.get(side_key, 0.0)} -> {new_qty}. Syncing TP/SL...", level="debug")
                            self.engine._should_update_tpsl = True
                            if abs(new_qty) > abs(prev_qtys.get(side_key, 0.0)):
                                self.engine.total_trades_count += 1

                        if self.engine.current_take_profit[side_key] == 0 or self.engine.current_stop_loss[side_key] == 0:
                             self.engine._should_update_tpsl = True

                        self.in_position[side_key] = True
                        self.position_entry_price[side_key] = safe_float(pos.get('avgPx'))
                        if abs(self.position_entry_price[side_key] - prev_qtys.get(side_key, 0.0)) > 1e-6:
                            self.engine.last_add_price = self.position_entry_price[side_key]
                        self.position_qty[side_key] = new_qty
                        self.position_liq[side_key] = safe_float(pos.get('liqp', '0'))
                        self.position_details[side_key] = pos
                    else:
                        if side_key in found_sides: found_sides.remove(side_key)

            # Close detection logic
            for s in ['long', 'short']:
                should_close = False
                if is_snapshot:
                    if s not in found_sides and self.in_position[s]: should_close = True
                else:
                    for pos in positions_data:
                        if self._map_side(pos.get('posSide', 'net')) == s and safe_float(pos.get('pos')) == 0:
                            should_close = True
                            break

                if should_close:
                    self._handle_closure(s)

            self.cached_active_positions_count = temp_active_count
            self.cached_pos_notional = temp_pos_notional
            self.cached_unrealized_pnl = temp_unrealized_pnl
            self.used_amount_notional = temp_used_notional

    def _handle_closure(self, side):
        close_reason = "Exchange Hit (TP/SL/Manual)"
        if self.engine.authoritative_exit_in_progress:
            close_reason = "Authoritative Exit (Bot Target/Emergency)"
        else:
            mkt = self.engine.latest_trade_price
            tp = self.engine.current_take_profit[side]
            sl = self.engine.current_stop_loss[side]
            if tp > 0 and abs(mkt - tp) / tp < 0.001:
                close_reason = "Mode 2 Profit Target Reached" if self.config.get('use_add_pos_profit_target') else "Exchange Hit (Take Profit)"
            elif sl > 0 and abs(mkt - sl) / sl < 0.001:
                close_reason = "Exchange Hit (Stop Loss)"

        self.engine.log("=" * 60, level="info")
        self.engine.log(f"[DONE] Close Position: {side.upper()} {self.config['symbol']} | Reason: {close_reason}", level="info")
        self.engine.log("=" * 60, level="info")

        self.in_position[side] = False
        self.position_entry_price[side] = 0.0
        self.position_qty[side] = 0.0
        self.position_liq[side] = 0.0
        self.position_details[side] = {}
        self.engine.current_take_profit[side] = 0.0
        self.engine.current_stop_loss[side] = 0.0
        self.engine._should_update_tpsl = True

    def _map_side(self, raw_side):
        if raw_side == 'short': return 'short'
        if raw_side == 'long': return 'long'
        side_key = self.config.get('direction', 'long')
        return 'long' if side_key == 'both' else side_key

    def update_realtime_metrics(self, current_price):
        if not current_price: return
        with self.lock:
            temp_upl = 0.0
            temp_notional = 0.0
            contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))
            for side in ['long', 'short']:
                if self.in_position[side]:
                    qty = abs(self.position_qty[side])
                    entry = self.position_entry_price[side]
                    if side == 'long':
                        temp_upl += (current_price - entry) * qty
                    else:
                        temp_upl += (entry - current_price) * qty
                    temp_notional += qty * current_price
            self.cached_unrealized_pnl = temp_upl
            self.cached_pos_notional = temp_notional
