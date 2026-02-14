import threading
import time
import logging
import json
from datetime import datetime, timezone
from collections import deque

from handlers.okx_client import OKXClient
from handlers.utils import safe_float, safe_int
from handlers.websocket_handler import WebSocketHandler
from handlers.account_manager import AccountManager
from handlers.position_manager import PositionManager
from handlers.order_manager import OrderManager
from handlers.auto_cal_manager import AutoCalManager

class TradingBotEngine:
    def __init__(self, config_file, emit_func):
        self.config_file = config_file
        self.emit = emit_func
        self.config = self._load_config()
        self.is_running = False
        self.stop_event = threading.Event()
        self.console_logs = deque(maxlen=200)
        self.product_info = {}
        self.latest_trade_price = 0.0
        self.total_trades_count = 0
        self.last_emit_time = 0
        self.monitoring_tick = 0
        self.current_take_profit = {'long': 0.0, 'short': 0.0}
        self.current_stop_loss = {'long': 0.0, 'short': 0.0}
        self._should_update_tpsl = False
        self.last_add_price = 0.0
        self.authoritative_exit_in_progress = False
        self.exit_lock = threading.Lock()

        # Metrics (will be updated by handlers)
        self.total_equity = 0.0
        self.account_balance = 0.0
        self.available_balance = 0.0
        self.net_trade_profit = 0.0
        self.total_trade_profit = 0.0
        self.total_trade_loss = 0.0

        # Handlers
        self.okx_client = OKXClient(self.log, self.config)
        self.account_manager = AccountManager(self)
        self.position_manager = PositionManager(self)
        self.order_manager = OrderManager(self)
        self.auto_cal_manager = AutoCalManager(self)
        self.ws_handler = WebSocketHandler(self.log, self.config, self.okx_client, self._on_ws_message)

        self.mgmt_thread = None

    def _load_config(self):
        with open(self.config_file, 'r') as f: return json.load(f)

    def log(self, message, level='info'):
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        self.console_logs.append(log_entry)
        self.emit('console_log', log_entry)
        if level == 'info': logging.info(message)
        elif level == 'error': logging.error(message)

    @property
    def in_position(self): return self.position_manager.in_position
    @property
    def position_qty(self): return self.position_manager.position_qty
    @property
    def position_entry_price(self): return self.position_manager.position_entry_price
    @property
    def cached_pos_notional(self): return self.position_manager.cached_pos_notional
    @property
    def cached_unrealized_pnl(self): return self.position_manager.cached_unrealized_pnl
    @property
    def open_trades(self): return self.order_manager.open_trades
    @property
    def used_amount_notional(self): return self.position_manager.used_amount_notional
    @property
    def remaining_amount_notional(self): return max(0.0, self.config.get('max_allowed_used', 0.0) - self.used_amount_notional)
    @property
    def max_allowed_display(self): return self.config.get('max_allowed_used', 0.0)
    @property
    def max_amount_display(self): return self.max_allowed_display
    @property
    def net_profit(self): return self.cached_unrealized_pnl
    @property
    def daily_reports(self): return self.account_manager.daily_reports
    @property
    def need_add_usdt_profit_target(self): return self.auto_cal_manager.need_add_usdt_profit_target
    @property
    def need_add_usdt_above_zero(self): return self.auto_cal_manager.need_add_usdt_above_zero
    @property
    def trade_fees(self): return 0.0
    @property
    def cumulative_margin_used(self): return 0.0

    def start(self, passive_monitoring=False):
        if not passive_monitoring: self.is_running = True
        self.stop_event.clear()
        self.account_manager.sync_server_time()
        self.account_manager.fetch_product_info(self.config['symbol'])
        self.ws_handler.start()
        if not self.mgmt_thread or not self.mgmt_thread.is_alive():
            self.mgmt_thread = threading.Thread(target=self._mgmt_loop, daemon=True)
            self.mgmt_thread.start()
        self.log(f"Bot started (Mode: {'Passive' if passive_monitoring else 'Active'})")

    def stop(self):
        self.is_running = False
        self.log("Bot trading stopped (Passive monitoring active)")

    def stop_bot(self):
        self.stop_event.set()
        self.is_running = False
        self.ws_handler.stop()
        self.log("Bot completely shut down")

    def _mgmt_loop(self):
        while not self.stop_event.is_set():
            try:
                self.monitoring_tick += 1
                if self.monitoring_tick % 10 == 0: self.account_manager.sync_account_data()

                # Auto Features (Run always as requested)
                self.auto_cal_manager.calculate_need_add_metrics()
                self.auto_cal_manager.check_auto_add()

                # Periodic TP/SL Sync
                if self.monitoring_tick % 5 == 0:
                    algos = self.order_manager.fetch_algo_orders(self.config['symbol'])
                    for a in algos:
                        side = self.position_manager._map_side(a.get('posSide', 'net'))
                        sl = safe_float(a.get('slTriggerPx'))
                        if sl > 0: self.current_stop_loss[side] = sl
                        tp = safe_float(a.get('tpTriggerPx'))
                        if tp > 0: self.current_take_profit[side] = tp

                self.account_manager.check_daily_report()
                if time.time() - self.last_emit_time >= 1.5:
                    self._emit_socket_updates()
                    self.last_emit_time = time.time()
            except Exception as e: self.log(f"Error in mgmt loop: {e}", level="error")
            time.sleep(1)

    def _on_ws_message(self, msg, is_private):
        if 'data' in msg:
            channel = msg.get('arg', {}).get('channel', '')
            data = msg.get('data', [])
            if channel == 'tickers' and data:
                price = safe_float(data[0].get('last'))
                if price > 0:
                    self.latest_trade_price = price
                    self.position_manager.update_realtime_metrics(price)
                    self._emit_socket_updates(throttle=True)
            elif channel == 'positions' and data:
                self.position_manager.process_positions(data)
                self._emit_socket_updates()
            elif channel == 'account' and data:
                for d in data[0].get('details', []):
                    if d.get('ccy') == 'USDT':
                        self.account_balance = safe_float(d.get('bal'))
                        self.total_equity = safe_float(data[0].get('totalEq'))
                        self.available_balance = safe_float(d.get('availBal'))
                self._emit_socket_updates()

    def _emit_socket_updates(self, throttle=False):
        if throttle and time.time() - self.last_emit_time < 0.2: return
        self.last_emit_time = time.time()
        payload = {
            'total_trades': self.total_trades_count, 'total_capital': self.total_equity,
            'total_balance': self.account_balance, 'available_balance': self.available_balance,
            'used_amount': self.used_amount_notional, 'remaining_amount': self.remaining_amount_notional,
            'net_profit': self.net_profit, 'in_position': self.in_position,
            'position_qty': self.position_qty, 'position_entry_price': self.position_entry_price,
            'daily_reports': self.daily_reports, 'need_add_usdt': self.need_add_usdt_profit_target,
            'need_add_above_zero': self.need_add_usdt_above_zero, 'running': self.is_running,
            'trade_fees': self.trade_fees, 'net_trade_profit': self.net_trade_profit,
            'total_trade_profit': self.total_trade_profit, 'total_trade_loss': self.total_trade_loss,
            'current_take_profit': self.current_take_profit, 'current_stop_loss': self.current_stop_loss
        }
        self.emit('account_update', payload)
        self.emit('bot_status', {'running': self.is_running})
        self.emit('trades_update', {'trades': self.open_trades})

    def emergency_sl(self):
        self.log("🚨 EMERGENCY SL", level="warning")
        for side, in_pos in self.in_position.items():
            if in_pos:
                self.order_manager.place_order(self.config['symbol'], "sell" if side == "long" else "buy", abs(self.position_qty[side]), order_type="Market", posSide=side)

    def batch_modify_tpsl(self): self.log("Batch Modify TP/SL triggered")
    def batch_cancel_orders(self): self.log("Batch Cancel Orders triggered")
    def test_api_credentials(self): return self.okx_client.sync_server_time()
    def apply_live_config_update(self, new_config):
        self.config = new_config
        for h in [self.okx_client, self.account_manager, self.position_manager, self.order_manager, self.auto_cal_manager, self.ws_handler]:
            h.config = new_config
        return {'success': True}
