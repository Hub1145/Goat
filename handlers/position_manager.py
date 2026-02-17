import threading
from handlers.utils import safe_float

class PositionManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.reset_session_metrics()
        self.reset()

    def reset_session_metrics(self):
        self.net_trade_profit = 0.0
        self.total_trade_profit = 0.0
        self.total_trade_loss = 0.0
        self.total_fees = 0.0

    def reset(self):
        self.pending_fill_logs = {} # ordId -> {pnl, fee, time}
        self.in_position = {'long': False, 'short': False}
        self.position_qty = {'long': 0.0, 'short': 0.0}
        self.position_entry_price = {'long': 0.0, 'short': 0.0}
        self.position_liq = {'long': 0.0, 'short': 0.0}
        self.position_details = {'long': {}, 'short': {}}
        self.position_notional = {'long': 0.0, 'short': 0.0}
        self.session_baseline_qty = {'long': 0.0, 'short': 0.0}
        self.baseline_initialized = False
        self.cached_active_positions_count = 0
        self.cached_pos_notional = 0.0
        self.cached_unrealized_pnl = 0.0
        self.used_amount_notional = 0.0
        self.current_entry_fees = 0.0 # Fees paid for CURRENT active position(s)

    def process_positions(self, positions_data, is_snapshot=True):
        temp_active_count = 0
        temp_pos_notional = 0.0
        temp_unrealized_pnl = 0.0
        temp_used_notional = 0.0
        target_symbol = self.config['symbol'].strip().upper()
        found_sides = set()
        contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))

        with self.engine.lock:
            if not self.baseline_initialized and is_snapshot:
                for pos in positions_data:
                    if pos.get('instId', '').strip().upper() == target_symbol:
                        q = abs(safe_float(pos.get('pos')))
                        if q > 0:
                            s_key = self._map_side(pos.get('posSide', 'net'), qty=safe_float(pos.get('pos')))
                            self.session_baseline_qty[s_key] = q
                self.baseline_initialized = True
                self.engine.log(f"Baseline initialized: Long={self.session_baseline_qty['long']}, Short={self.session_baseline_qty['short']} contracts (Manual positions ignored in used margin)")

            prev_qtys = {k: v for k, v in self.position_qty.items()}
            for pos in positions_data:
                if pos.get('instId', '').strip().upper() == target_symbol:
                    qty_raw = safe_float(pos.get('pos'))
                    if qty_raw == 0 and is_snapshot: continue
                    side_key = self._map_side(pos.get('posSide', 'net'), qty=qty_raw)
                    if qty_raw != 0:
                        found_sides.add(side_key)
                        mkt_px = self.engine.latest_trade_price if self.engine.latest_trade_price else safe_float(pos.get('avgPx'))
                        side_notional = abs(qty_raw) * mkt_px * contract_size
                        self.position_notional[side_key] = side_notional
                        temp_pos_notional += side_notional
                        temp_unrealized_pnl += safe_float(pos.get('upl', '0'))
                        temp_active_count += 1

                        # Session margin tracking (consistent whether running or stopped)
                        session_qty = max(0, abs(qty_raw) - self.session_baseline_qty.get(side_key, 0.0))
                        temp_used_notional += session_qty * mkt_px * contract_size

                        new_qty = qty_raw
                        if abs(new_qty - prev_qtys.get(side_key, 0.0)) > 1e-6:
                            if abs(new_qty) > abs(prev_qtys.get(side_key, 0.0)):
                                self.engine.total_trades_count += 1

                        self.in_position[side_key] = True
                        self.position_entry_price[side_key] = safe_float(pos.get('avgPx'))
                        self.position_qty[side_key] = new_qty
                        self.position_liq[side_key] = safe_float(pos.get('liqp', '0'))
                        self.position_details[side_key] = pos

            for s in ['long', 'short']:
                if is_snapshot:
                    if s not in found_sides and self.in_position[s]: self._handle_closure(s)
                else:
                    for pos in positions_data:
                        if self._map_side(pos.get('posSide', 'net'), qty=safe_float(pos.get('pos'))) == s and safe_float(pos.get('pos')) == 0:
                            self._handle_closure(s)
                            break

            self.cached_active_positions_count = temp_active_count
            self.cached_pos_notional = temp_pos_notional
            self.cached_unrealized_pnl = temp_unrealized_pnl
            self.used_amount_notional = temp_used_notional

    def _handle_closure(self, s):
        self.engine.log(f"Position Detected Closed: {s.upper()}", level="info")
        self.in_position[s] = False
        self.position_qty[s] = 0.0
        self.position_entry_price[s] = 0.0
        self.position_notional[s] = 0.0
        self.position_details[s] = {}
        self.engine.current_take_profit[s] = 0.0
        self.engine.current_stop_loss[s] = 0.0
        # If no positions left, clear current entry fees
        if not any(self.in_position.values()):
            self.current_entry_fees = 0.0

    def update_realtime_metrics(self, current_price):
        if not current_price: return
        temp_upl = 0.0
        temp_notional = 0.0
        contract_size = safe_float(self.engine.product_info.get('contractSize', 1.0))
        for side in ['long', 'short']:
            if self.in_position[side]:
                qty = abs(self.position_qty[side])
                entry = self.position_entry_price[side]
                if side == 'long': temp_upl += (current_price - entry) * qty * contract_size
                else: temp_upl += (entry - current_price) * qty * contract_size
                side_notional = qty * current_price * contract_size
                self.position_notional[side] = side_notional
                temp_notional += side_notional
        self.cached_unrealized_pnl = temp_upl
        self.cached_pos_notional = temp_notional

    def _map_side(self, raw_side, qty=0):
        if raw_side == 'short': return 'short'
        if raw_side == 'long': return 'long'
        if raw_side == 'net' or not raw_side:
            if qty > 0: return 'long'
            if qty < 0: return 'short'
        side_key = self.config.get('direction', 'long')
        return 'long' if side_key == 'both' else side_key

    def add_fee(self, fee):
        self.total_fees += abs(fee)
        # We also add to current_entry_fees, but we should clear it when position closes
        self.current_entry_fees += abs(fee)

    def sync_positions(self):
        target_symbol = self.config['symbol'].strip().upper()
        res = self.engine.okx_client.request("GET", "/api/v5/account/positions", params={"instId": target_symbol})
        if res and res.get('code') == '0':
            self.process_positions(res.get('data', []), is_snapshot=True)

    def add_realized_pnl(self, ord_id, pnl, fee):
        net = pnl + fee
        if net > 0: self.total_trade_profit += net
        else: self.total_trade_loss += abs(net)
        self.net_trade_profit = self.total_trade_profit - self.total_trade_loss

        # Aggregate fill logs to avoid spamming
        if ord_id not in self.pending_fill_logs:
            self.pending_fill_logs[ord_id] = {'pnl': 0.0, 'fee': 0.0, 'time': time.time()}

        self.pending_fill_logs[ord_id]['pnl'] += pnl
        self.pending_fill_logs[ord_id]['fee'] += fee
        self.pending_fill_logs[ord_id]['time'] = time.time()

    def flush_fill_logs(self, force=False):
        now = time.time()
        to_remove = []
        for ord_id, data in self.pending_fill_logs.items():
            if force or (now - data['time'] > 1.0):
                pnl, fee = data['pnl'], data['fee']
                net = pnl + fee
                self.engine.log(f"Trade Closed (Order {ord_id}) - Realized PnL: {pnl:.4f}, Fee: {fee:.4f}, Net: {net:.4f}", level="info")
                to_remove.append(ord_id)
        for oid in to_remove:
            del self.pending_fill_logs[oid]
