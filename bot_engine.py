import json
import time
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import websocket # The 'websocket-client' package provides the 'websocket' module
import ta
import threading
from collections import deque
import os # Added for file path operations
import logging.handlers
import requests
import hashlib
import hmac
import base64
import math
import _thread

# OKX API configuration defaults (now managed per instance)
OKX_REST_API_BASE_URL = "https://www.okx.com"

# Rate Limiter Class - Token Bucket Algorithm
class RateLimiter:
    """
    Token bucket rate limiter to prevent API rate limit errors.
    Supports different rate limits for different endpoint categories.
    """
    def __init__(self):
        self.locks = {}
        self.buckets = {}
        
        # Define rate limits per endpoint category (requests per second)
        # OKX limits: ~20-60 req/2s depending on endpoint
        self.limits = {
            'account': {'rate': 3, 'capacity': 6},      # Account endpoints: 3 req/s, burst 6
            'trade': {'rate': 3, 'capacity': 6},        # Trade endpoints: 3 req/s, burst 6
            'market': {'rate': 10, 'capacity': 20},     # Market data: 10 req/s, burst 20
            'public': {'rate': 10, 'capacity': 20},     # Public endpoints: 10 req/s, burst 20
            'default': {'rate': 5, 'capacity': 10}      # Default: 5 req/s, burst 10
        }
        
        # Initialize buckets
        for category in self.limits:
            self.locks[category] = threading.Lock()
            self.buckets[category] = {
                'tokens': self.limits[category]['capacity'],
                'last_update': time.time()
            }
    
    def _get_category(self, path):
        """Determine endpoint category from API path"""
        if '/account/' in path:
            return 'account'
        elif '/trade/' in path:
            return 'trade'
        elif '/market/' in path:
            return 'market'
        elif '/public/' in path:
            return 'public'
        else:
            return 'default'
    
    def acquire(self, path, tokens=1):
        """
        Acquire tokens before making a request.
        Blocks if insufficient tokens available.
        """
        category = self._get_category(path)
        lock = self.locks[category]
        
        with lock:
            while True:
                now = time.time()
                bucket = self.buckets[category]
                limit = self.limits[category]
                
                # Refill tokens based on time elapsed
                time_passed = now - bucket['last_update']
                bucket['tokens'] = min(
                    limit['capacity'],
                    bucket['tokens'] + time_passed * limit['rate']
                )
                bucket['last_update'] = now
                
                # Check if we have enough tokens
                if bucket['tokens'] >= tokens:
                    bucket['tokens'] -= tokens
                    return
                
                # Calculate wait time for next token
                tokens_needed = tokens - bucket['tokens']
                wait_time = tokens_needed / limit['rate']
                time.sleep(min(wait_time, 0.5))  # Sleep max 0.5s at a time

# product_info structure template (now managed per instance)
product_info_TEMPLATE = {
    "pricePrecision": None,
    "qtyPrecision": None,
    "priceTickSize": None,
    "minOrderQty": None,
    "contractSize": None,
}

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default

# Top-level signature helper (kept simple, takes params)
def generate_okx_signature(api_secret, timestamp, method, request_path, body_str=''):
    """Generate HMAC SHA256 signature for OKX API."""
    message = str(timestamp) + method.upper() + request_path + body_str
    hashed = hmac.new(api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(hashed.digest()).decode('utf-8')
    return signature
    return signature

class TradingBotEngine:
    def __init__(self, config_path, emit_callback):
        self.config_path = config_path
        self.emit = emit_callback
        
        self.console_logs = deque(maxlen=500)
        self.config = self._load_config()

        # Configure logging
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG) # Root captures everything

        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 1. Console Handler (Always INFO and above)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)

        # 2. info.log (Always INFO and above, used for downloads, trimmed at 5MB)
        try:
            info_handler = logging.handlers.RotatingFileHandler(
                'info.log', maxBytes=5*1024*1024, backupCount=1, encoding='utf-8'
            )
            info_handler.setFormatter(formatter)
            info_handler.setLevel(logging.INFO)
            root_logger.addHandler(info_handler)
        except Exception as e:
            print(f"Error setting up info.log: {e}")

        # 3. debug.log (Capture DEBUG level only if configured)
        if self.config.get('log_level', 'info').lower() == 'debug':
            try:
                debug_handler = logging.handlers.RotatingFileHandler(
                    'debug.log', maxBytes=10*1024*1024, backupCount=1, encoding='utf-8'
                )
                debug_handler.setFormatter(formatter)
                debug_handler.setLevel(logging.DEBUG)
                root_logger.addHandler(debug_handler)
                logging.info('Logger initialized. Writing DEBUG logs to debug.log')
            except Exception as e:
                print(f"Error setting up debug.log: {e}")
        else:
            logging.info('Logger initialized. info.log active for monitoring/downloads.')

        self._apply_api_credentials()

        self.ws = None
        self.ws_public = None
        self.ws_private = None
        self.ws_thread = None
        self.is_running = False
        self.stop_event = threading.Event()
        self.bot_start_time = int(time.time() * 1000) # Track start time in ms
        
        self.current_balance = 0.0
        self.open_trades = []
        self.is_bot_initialized = threading.Event()
        
        # OKX specific variables (from example bot)
        self.historical_data_store = {}
        self.data_lock = threading.Lock()
        self.trade_data_lock = threading.Lock()
        self.latest_trade_price = None
        self.latest_trade_timestamp = None
        self.last_price_update_time = time.time() # High-precision timestamp of last price arrival
        self.account_balance = 0.0
        self.available_balance = 0.0
        self.total_balance = 0.0 # Tracking OKX 'bal' (Actual Total Balance)
        self.total_equity = 0.0
        self.effective_wallet_balance = 0.0 # Base for Real-time Equity (Equity - PnL)
        self.account_info_lock = threading.Lock()
        self.net_profit = 0.0 # Track actual PnL
        self.net_profit_after_fees = 0.0 # Internal fee-adjusted PnL
    
        # Financial Display Metrics
        self.max_allowed_display = 0.0
        self.max_amount_display = 0.0
        self.remaining_amount_notional = 0.0
        self.trade_fees = 0.0
        
        # New State Variables for Client Logic
        self.total_capital_2nd = 0.0
        self.cumulative_margin_used = 0.0  # Session-only, resets on restart
        self.last_add_price = 0.0 # Tracks price of last entry/add for Gap Trigger
        self.auto_add_step_count = 0 # Tracks if we are on Step 1 (Market) or Step 2 (Limit)

        
        # Refactored for Dual-Direction Support
        self.in_position = {'long': False, 'short': False}
        self.position_entry_price = {'long': 0.0, 'short': 0.0}
        self.position_qty = {'long': 0.0, 'short': 0.0}
        self.position_liq = {'long': 0.0, 'short': 0.0}
        self.position_details = {'long': {}, 'short': {}} # Store raw OKX position data (lever, etc.)
        self.current_stop_loss = {'long': 0.0, 'short': 0.0}
        self.current_take_profit = {'long': 0.0, 'short': 0.0}
        self.position_exit_orders = {'long': {}, 'short': {}} # { 'long': {'tp': id, 'sl': id}, ... }
        self.entry_reduced_tp_flag = {'long': False, 'short': False}
        self.session_baseline_qty = {'long': 0.0, 'short': 0.0} # Baseline for session-only notional tracking
        
        self.batch_counter = 0 # Track batches for logging
        self.monitoring_tick = 0 # Track monitoring cycles
        self.used_amount_notional = 0.0
        self.position_lock = threading.Lock()
        self.pending_entry_ids = [] # List to track multiple pending entry orders
        self.pending_entry_order_id = None # Kept for backward compatibility/single tracking if needed
        self.pending_entry_order_details = {} # Now will store details per order ID in a dict
        self.entry_sl_price = 0.0 # This might need migration too if we have concurrent entries? 
                                  # Entries are usually batch-based and transient. 
        self.sl_hit_triggered = False
        self.sl_hit_lock = threading.Lock()
        self.entry_order_with_sl = None
        self.entry_order_sl_lock = threading.Lock()
        self.tp_hit_triggered = False
        self.tp_hit_lock = threading.Lock()
        self.bot_startup_complete = False
        self._should_update_tpsl = False
        self.last_applied_creds_hash = None # To detect credential/testnet changes
        self.last_emit_time = 0.0 # Throttling for real-time WS updates


        self.ws_subscriptions_ready = threading.Event()
        self.pending_subscriptions = set()
        
        self.total_trades_count = 0 # Persistent counter for individual fills
        self.credentials_invalid = False
        
        # Session-level realized profit tracking
        self.total_trade_profit = 0.0  # Cumulative profit from winning trades
        self.total_trade_loss = 0.0    # Cumulative loss from losing trades
        self.net_trade_profit = 0.0    # Net realized profit (profit - loss)
    
        # Instance-specific OKX State
        self.server_time_offset = 0
        self.okx_api_key = ""
        self.okx_api_secret = ""
        self.okx_passphrase = ""
        self.okx_simulated_trading_header = {}
        self.product_info = {
            "pricePrecision": None,
            "qtyPrecision": None,
            "priceTickSize": None,
            "minOrderQty": None,
            "contractSize": None,
        }.copy()
        self.okx_rest_api_base_url = "https://www.okx.com" # Assuming this was a global constant

        self.confirmed_subscriptions = set()
        
        # Initialize persistent analytics
        self.analytics_path = "analytics.json"
        self.total_trade_profit = 0.0
        self.total_trade_loss = 0.0
        self.net_trade_profit = 0.0
        self.daily_reports = []
        self._load_analytics()

        # Concurrency Guards for Authoritative Exit
        self.exit_lock = threading.Lock()
        self.authoritative_exit_in_progress = False

        # Initialize rate limiter for API request throttling (RESTORED)
        self.rate_limiter = RateLimiter()

        self.intervals = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
            '12h': 43200, '1d': 86400, '1w': 604800, '1M': 2592000
        }
        
    def log(self, message, level='info', to_file=False, filename=None):
        # Map levels to numerical priorities
        LEVEL_MAP = {
            'debug': 10,
            'info': 20,
            'warning': 30,
            'error': 40,
            'critical': 50
        }
        
        # Get configured log level from config
        configured_level_str = self.config.get('log_level', 'info').lower()
        configured_level = LEVEL_MAP.get(configured_level_str, 20)
        current_level = LEVEL_MAP.get(level.lower(), 20)
        
        # If the level of this message is lower than the configured level, skip it
        if current_level < configured_level:
            return
            
        # Suppress non-critical logs if credentials are known to be invalid
        if self.credentials_invalid and level.lower() != 'critical':
            return

        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        
        # Always append to console_logs for internal history if it passed the filter
        self.console_logs.append(log_entry)
        
        # Emit to the frontend
        self.emit('console_log', log_entry)
        
        # Write to standard logger (Console/File handling now managed at root level)
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        elif level == 'debug':
            logging.debug(message)
        elif level == 'critical':
            logging.critical(message)
    
    def check_credentials(self):
        """Verifies if the current API credentials are valid and configured."""
        self._apply_api_credentials()
        
        if not self.okx_api_key or not self.okx_api_secret or not self.okx_passphrase:
            return False, "API Key, Secret, or Passphrase missing for selected mode."
        
        try:
            path = "/api/v5/account/balance"
            params = {"ccy": "USDT"}
            # Use max_retries=1 to fail quickly if invalid
            response = self._okx_request("GET", path, params=params, max_retries=1)
            
            if response and response.get('code') == '0':
                return True, "Credentials valid."
            elif response and response.get('code') == '50110': # Invalid API key
                return False, "Invalid API credentials."
            elif response and response.get('msg'):
                return False, f"API Error: {response.get('msg')}"
            else:
                return False, "Unknown API error during validation."
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def start(self, passive_monitoring=False):
        if self.is_running and not passive_monitoring:
            self.log('Bot is already trading', 'warning')
            return
        
        if not passive_monitoring:
            self.log('Bot starting trading logic...', 'info')
            # Reset session-based trade metrics for a clean start
            self.total_trade_profit = 0.0
            self.total_trade_loss = 0.0
            self.net_trade_profit = 0.0
            self.total_trades_count = 0
            self.log('Session trade metrics reset.', 'info')
        else:
            self.log('Bot starting background monitoring...', 'info')
        
        # 0. Apply Credentials
        force_ws_restart = False
        old_creds_hash = self.last_applied_creds_hash
        self._apply_api_credentials()
        
        if old_creds_hash and old_creds_hash != self.last_applied_creds_hash:
            self.log("Sensitive configuration change detected (API Keys or Environment). Forcing WebSocket reconnection...", level="info")
            force_ws_restart = True

        # 0.1 Perform initial credential validity check
        if not self.okx_api_key or not self.okx_api_secret or not self.okx_passphrase:
            self.log("⚠️ API Credentials not configured for the selected mode.", "error")
            self.credentials_invalid = True
            if not passive_monitoring:
                self.emit('error', {'message': 'API Credentials not configured.'})
                self.is_running = False
            return

        self.log("Verifying API credentials...", level="debug")
        valid, msg = self.check_credentials()
        if not valid:
            if any(err in msg.lower() for err in ['invalid', 'credentials', 'key', 'secret', 'passphrase', '401']):
                self.credentials_invalid = True
            
            if not self.credentials_invalid:
                 self.log(f"⚠️ API Connection/Verification Failed: {msg}", "error")
            else:
                 self.log(f"⚠️ API Credentials Verification Failed: {msg}", "error")

            if not passive_monitoring:
                self.emit('error', {'message': f'API Credentials Error: {msg}'})
                self.is_running = False
            return

        # New initialization sequence for OKX
        if not self._sync_server_time():
            self.log("Failed to synchronize server time. Please check network connection or API.", 'error')
            if not passive_monitoring: self.is_running = False
            self.emit('bot_status', {'running': False})
            return
        
        if not self._fetch_product_info(self.config['symbol']):
            self.log("Failed to fetch product info. Exiting.", 'error')
            if not passive_monitoring: self.is_running = False
            self.emit('bot_status', {'running': False})
            return
 
        if not passive_monitoring:
            # 0.2 Record Session Baseline Quantities to ignore pre-existing manual positions
            self.fetch_account_data_sync() # Ensure metrics are fresh before start
            with self.position_lock:
                self.session_baseline_qty = {k: v for k, v in self.position_qty.items()}
                self.used_amount_notional = 0.0
                self.remaining_amount_notional = 0.0
            self.log(f"Session baseline recorded: LONG={self.session_baseline_qty['long']}, SHORT={self.session_baseline_qty['short']}", level="debug")

            # 1. Position Mode Sync
            target_pos_mode = self.config.get('okx_pos_mode', 'net_mode')
            if not self._okx_set_position_mode(target_pos_mode):
                 self.log("Failed to verify/set position mode. Exiting.", 'error')
                 self.is_running = False
                 self.emit('bot_status', {'running': False})
                 return

            # 2. Leverage Sync
            lev_val = self.config.get('leverage', 20)
            symbol = self.config['symbol']
            lev_success = False
            if target_pos_mode == 'long_short_mode':
                l_ok = self._okx_set_leverage(symbol, lev_val, pos_side="long")
                s_ok = self._okx_set_leverage(symbol, lev_val, pos_side="short")
                lev_success = l_ok and s_ok
            else:
                lev_success = self._okx_set_leverage(symbol, lev_val, pos_side="net")

            if not lev_success:
                self.log("Failed to set leverage. Exiting.", 'error')
                self.is_running = False
                self.emit('bot_status', {'running': False})
                return
        
            self.log("Checking for and closing any existing open positions...", level="info")
            self._check_and_close_any_open_position()

        if force_ws_restart:
            self.log("Sensitive configuration change: Performing clean shutdown and thread synchronization...", level="info")
            self.stop_event.set() # Signal all threads to stop
            
            try:
                if self.ws_public: self.ws_public.close()
                if self.ws_private: self.ws_private.close()
            except Exception as e:
                self.log(f"Error closing WS for restart: {e}", level="debug")

            # Join the old threads to ensure they are dead before starting new ones
            if getattr(self, 'ws_thread', None) and self.ws_thread.is_alive():
                self.log("Joining old WebSocket thread...", level="debug")
                self.ws_thread.join(timeout=5.0)
            
            if getattr(self, 'mgmt_thread', None) and self.mgmt_thread.is_alive():
                self.log("Joining old management thread...", level="debug")
                self.mgmt_thread.join(timeout=2.0)
            
            self.stop_event.clear() # Reset for the new session
            self.log("Shutdown complete. Ready for new configuration.", level="info")

        # Mark as running ONLY after all initialization is complete
        if not passive_monitoring:
            self.is_running = True
        
        self.bot_start_time = int(time.time() * 1000)

        # Check if threads are already running (Normal start case without cred change)
        if getattr(self, 'ws_thread', None) and self.ws_thread.is_alive():
            if self.ws_public and getattr(self, 'subscribed_symbol', None) != self.config.get('symbol'):
                 self.log(f"Symbol changed to {self.config.get('symbol')}, restarting WebSocket...", level="info")
                 try:
                     self.ws_public.close(); self.ws_private.close()
                 except: pass
            else:
                 self.log("WebSocket and Management threads are already active. Re-applied any credential changes.", level="debug")
                 return
        
        self.log('Bot initialized. Starting live connection threads...', 'info')
        self.stop_event.clear() # Ensure it's clear
        self.ws_thread = threading.Thread(target=self._initialize_websocket_and_start_main_loop, daemon=True)
        self.ws_thread.start()

        if not getattr(self, 'mgmt_thread', None) or not self.mgmt_thread.is_alive():
            self.mgmt_thread = threading.Thread(target=self._unified_management_loop, daemon=True)
            self.mgmt_thread.start()
    
    def stop(self):
        if not self.is_running:
            self.log('Bot trading is not active', 'warning')
            return
        
        self.is_running = False
        self.log('Bot trading logic paused. Background monitoring remains active.', 'info')
        
        # Reset session capacity metrics on stop as per user request
        with self.position_lock:
            self.used_amount_notional = 0.0
            self.remaining_amount_notional = 0.0
            self.trade_fees = 0.0
            self.session_baseline_qty = {'long': 0.0, 'short': 0.0}

        self.emit('bot_status', {'running': False})

    def shutdown(self):
        """Truly stops all threads and connections."""
        self.is_running = False
        self.stop_event.set()
        if self.ws_public or self.ws_private:
            try:
                self.ws_public.close(); self.ws_private.close()
            except:
                pass
        self.log('Bot fully shut down.', 'info')
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                # Ensure new config parameters have default values if not present
                config.setdefault('max_allowed_used', 1000.0)
                config.setdefault('cancel_on_tp_price_below_market', True)
                config.setdefault('cancel_on_entry_price_below_market', True)
                config.setdefault('cancel_on_tp_price_above_market', True)
                config.setdefault('cancel_on_entry_price_above_market', True)
                config.setdefault('websocket_timeframes', ['1m', '5m']) # Add default for websocket_timeframes
                config.setdefault('direction', 'long')
                config.setdefault('mode', 'cross') # Changed default mode to 'cross'
                config.setdefault('tp_amount', 0.5)
                config.setdefault('sl_amount', 1.0)
                config.setdefault('trigger_price', 'last')
                config.setdefault('tp_mode', 'limit')
                config.setdefault('tp_type', 'oco')
                config.setdefault('use_candlestick_conditions', True) # New parameter
                config.setdefault('okx_demo_api_key', '')
                config.setdefault('okx_demo_api_secret', '')
                config.setdefault('okx_demo_api_passphrase', '')
                config.setdefault('use_chg_open_close', False)
                config.setdefault('min_chg_open_close', 0)
                config.setdefault('max_chg_open_close', 0)
                config.setdefault('use_chg_high_low', False)
                config.setdefault('min_chg_high_low', 0)
                config.setdefault('max_chg_high_low', 0)
                config.setdefault('use_chg_high_close', False)
                config.setdefault('min_chg_high_close', 0)
                config.setdefault('max_chg_high_close', 0)
                config.setdefault('candlestick_timeframe', '1m')
                config.setdefault('log_level', 'info') # New: Default for log level
                config.setdefault('use_auto_margin', False)
                config.setdefault('auto_margin_offset', 30.0)
                config.setdefault('use_add_pos_auto_cal', False)
                config.setdefault('add_pos_recovery_percent', 0.6)
                config.setdefault('add_pos_profit_multiplier', 1.5)
                config.setdefault('use_add_pos_above_zero', False)
                config.setdefault('use_add_pos_profit_target', False)
                return config
        except FileNotFoundError:
            self.log(f"Config file not found: {self.config_path}", 'error')
            raise
        except json.JSONDecodeError as e:
            self.log(f"Error decoding config file {self.config_path}: {e}", 'error')
            raise
        except Exception as e:
            self.log(f"An unexpected error occurred while loading config: {e}", 'error')
            raise

    # ================================================================================
    # OKX API Helper Functions (Adapted as methods)
    # ================================================================================

    def _apply_api_credentials(self):
        """Applies configured API credentials to instance variables."""
        self.credentials_invalid = False
        use_dev = self.config.get('use_developer_api', False)
        use_demo = self.config.get('use_testnet', False)

        if use_dev:
            if use_demo:
                self.okx_api_key = self.config.get('dev_demo_api_key', '')
                self.okx_api_secret = self.config.get('dev_demo_api_secret', '')
                self.okx_passphrase = self.config.get('dev_demo_api_passphrase', '')
            else:
                self.okx_api_key = self.config.get('dev_api_key', '')
                self.okx_api_secret = self.config.get('dev_api_secret', '')
                self.okx_passphrase = self.config.get('dev_passphrase', '')
        else:
            if use_demo:
                self.okx_api_key = self.config.get('okx_demo_api_key', '')
                self.okx_api_secret = self.config.get('okx_demo_api_secret', '')
                self.okx_passphrase = self.config.get('okx_demo_api_passphrase', '')
            else:
                self.okx_api_key = self.config.get('okx_api_key', '')
                self.okx_api_secret = self.config.get('okx_api_secret', '')
                self.okx_passphrase = self.config.get('okx_passphrase', '')

        if use_demo:
            self.okx_simulated_trading_header = {'x-simulated-trading': '1'}
        else:
            self.okx_simulated_trading_header = {}
            
        # Update last applied hash for sensitive config change detection
        sensitivity_str = f"{use_dev}:{use_demo}:{self.okx_api_key}:{self.okx_api_secret}:{self.okx_passphrase}"
        self.last_applied_creds_hash = hashlib.md5(sensitivity_str.encode()).hexdigest()

        self.log(f"API Credentials Applied: {'Developer' if use_dev else 'User'} | {'Demo' if use_demo else 'Live'}", level="debug")

    def _save_config(self):
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            self.log(f"Config saved to {self.config_path}", level="debug")
        except Exception as e:
            self.log(f"Error saving config: {e}", level="error")

    def _okx_request(self, method, path, params=None, body_dict=None, max_retries=3):
        if self.credentials_invalid:
            return None
            
        local_dt = datetime.now(timezone.utc)
        adjusted_dt = local_dt + timedelta(milliseconds=self.server_time_offset)
        timestamp = adjusted_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        body_str = ''
        if body_dict:
            body_str = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

        request_path_for_signing = path
        final_url = f"{self.okx_rest_api_base_url}{path}" 

        if params and method.upper() == 'GET':
            query_string = '?' + '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
            request_path_for_signing += query_string
            final_url += query_string

        signature = generate_okx_signature(self.okx_api_secret, timestamp, method, request_path_for_signing, body_str)

        headers = {
            "OK-ACCESS-KEY": self.okx_api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.okx_passphrase,
            "Content-Type": "application/json"
        }

        headers.update(self.okx_simulated_trading_header)

        for attempt in range(max_retries):
            if self.credentials_invalid:
                return None

            try:
                # Acquire rate limit token before making request
                self.rate_limiter.acquire(path)
                
                req_func = getattr(requests, method.lower(), None)
                if not req_func:
                    self.log(f"Unsupported HTTP method: {method}", level="error")
                    return None

                kwargs = {'headers': headers, 'timeout': 15}

                if body_dict and method.upper() in ['POST', 'PUT', 'DELETE']:
                    kwargs['data'] = body_str

                self.log(f"{method} {path} (Attempt {attempt + 1}/{max_retries})", level="debug")
                response = req_func(final_url, **kwargs)

                if response.status_code != 200:
                    try:
                        error_json = response.json()
                        okx_error_code = error_json.get('code')
                        
                        # Check for invalid credential error codes
                        if okx_error_code in ['50110', '50111', '50113'] or response.status_code == 401:
                            if not self.credentials_invalid:
                                self.log(f"CRITICAL: Invalid API credentials detected (Status={response.status_code}, Code={okx_error_code}). Suppressing further API errors.", level="critical")
                                self.credentials_invalid = True
                            return error_json

                        if not self.credentials_invalid:
                            self.log(f"API Error: Status={response.status_code}, Code={okx_error_code}, Msg={error_json.get('msg')}. Full Response: {error_json}", level="error")
                        
                        if okx_error_code:
                            return error_json
                    except json.JSONDecodeError:
                        if not self.credentials_invalid:
                            self.log(f"API Error: Status={response.status_code}, Response: {response.text}", level="error")

                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                try:
                    json_response = response.json()
                    if json_response.get('code') != '0':
                        self.log(f"OKX API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}. Full Response: {json_response}", level="debug")
                    self.log(f"DEBUG: Full OKX API Response for {method} {path}: {json_response}", level="debug") # Log full response
                    return json_response
                except json.JSONDecodeError:
                    self.log(f"Failed to decode JSON for {method} {path}. Status: {response.status_code}, Resp: {response.text}", level="error")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

            except requests.exceptions.Timeout:
                self.log(f"API request timeout (Attempt {attempt + 1}/{max_retries})", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if e.response is not None else "N/A"
                err_text = e.response.text[:200] if e.response is not None else 'No response text'
                self.log(f"OKX API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except Exception as e:
                self.log(f"Unexpected error during OKX API request ({method} {path}): {e}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        return None

    def _fetch_historical_data_okx(self, symbol, timeframe, start_ts_ms, end_ts_ms):
        try:
            path = "/api/v5/market/history-candles"

            okx_timeframe_map = {
                '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
                '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
            }
            okx_timeframe = okx_timeframe_map.get(timeframe)

            if not okx_timeframe:
                self.log(f"Invalid timeframe for OKX: {timeframe}", level="error")
                return []

            all_data = []
            max_candles_limit = 100

            current_before_ms = end_ts_ms

            self.log(f"Fetching historical data for {symbol} ({timeframe})", level="debug")

            while True:
                params = {
                    "instId": symbol,
                    "bar": okx_timeframe,
                    "limit": str(max_candles_limit),
                    "before": str(current_before_ms)
                }

                response = self._okx_request("GET", path, params=params)
                
                if response and response.get('code') == '0':
                    rows = response.get('data', [])
                    if rows:
                        self.log(f"Fetched {len(rows)} candles for {timeframe}", level="debug")
                        parsed_klines = []
                        for kline in rows:
                            try:
                                parsed_klines.append([
                                    int(kline[0]),
                                    float(kline[1]),
                                    float(kline[2]),
                                    float(kline[3]),
                                    float(kline[4]),
                                    float(kline[5])
                                ])
                            except (ValueError, TypeError, IndexError) as e:
                                self.log(f"Error parsing OKX kline: {kline} - {e}", level="error")
                                continue
                        
                        all_data.extend(parsed_klines)
                        
                        oldest_ts = int(rows[-1][0])
                        current_before_ms = oldest_ts

                        if oldest_ts <= start_ts_ms or len(rows) < max_candles_limit:
                            break 
                    else:
                        break 

                    time.sleep(0.3)
                else:
                    self.log(f"Error fetching OKX klines: {response}", level="error")
                    return []
            
            final_data = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            if not final_data.empty:
                final_data = final_data.drop_duplicates(subset=['Timestamp'])
                final_data = final_data[final_data['Timestamp'] >= start_ts_ms]
                final_data = final_data.sort_values(by='Timestamp', ascending=True)
                return final_data.values.tolist()
            else:
                return []
        except Exception as e:
            self.log(f"Exception in _fetch_historical_data_okx: {e}", level="error")
            return []

    def _sync_server_time(self):
        """Synchronizes server time and updates instance offset."""
        try:
            response = requests.get(f"{self.okx_rest_api_base_url}/api/v5/public/time", timeout=5)
            response.raise_for_status()
            json_response = response.json()
            if json_response.get('code') == '0' and json_response.get('data'):
                server_timestamp_ms = int(json_response['data'][0]['ts'])
                local_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                self.server_time_offset = server_timestamp_ms - local_timestamp_ms
                self.log(f"OKX server time synchronized. Offset: {self.server_time_offset}ms", level="info")
                return True
            else:
                self.log(f"Failed to get OKX server time: {json_response.get('msg', 'Unknown error')}", level="error")
                return False
        except requests.exceptions.RequestException as e:
            self.log(f"Error fetching OKX server time: {e}", level="error")
            return False
        except Exception as e:
            self.log(f"Unexpected error in _sync_server_time: {e}", level="error")
            return False

    def _fetch_product_info(self, target_symbol):
        try:
            path = "/api/v5/public/instruments"
            params = {"instType": "SWAP", "instId": target_symbol}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                product_data = None
                if isinstance(response.get('data'), list):
                    for item in response['data']:
                        if item.get('instId') == target_symbol:
                            product_data = item
                            break
                elif isinstance(response.get('data'), dict) and response.get('data').get('instId') == target_symbol:
                    product_data = response.get('data')

                if not product_data:
                    self.log(f"Product {target_symbol} not found in OKX instruments response.", level="error")
                    return False

                self.product_info['priceTickSize'] = safe_float(product_data.get('tickSz'))
                self.product_info['qtyPrecision'] = int(np.abs(np.log10(safe_float(product_data.get('lotSz'))))) if safe_float(product_data.get('lotSz')) > 0 else 0
                self.product_info['pricePrecision'] = int(np.abs(np.log10(safe_float(product_data.get('tickSz'))))) if safe_float(product_data.get('tickSz')) > 0 else 0
                self.product_info['qtyStepSize'] = safe_float(product_data.get('lotSz'))
                self.product_info['minOrderQty'] = safe_float(product_data.get('minSz'))

                self.product_info['contractSize'] = safe_float(product_data.get('ctVal', '1'), 1.0)

                self.log(f"Product specifications for {target_symbol} initialized.", level="debug")
                return True
            else:
                self.log(f"Failed to fetch product info for {target_symbol} (code: {response.get('code') if response else 'N/A'}, msg: {response.get('msg') if response else 'N/A'})", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in fetch_product_info: {e}", level="error")
            return False

    def _okx_set_leverage(self, symbol, leverage_val, pos_side="net"):
        try:
            path = "/api/v5/account/set-leverage"
            body = {
                "instId": symbol,
                "lever": str(int(leverage_val)),
                "mgnMode": self.config.get('mode', 'cross'), # Use mode from config
                "posSide": pos_side
            }

            self.log(f"Setting leverage to {leverage_val}x for {symbol} ({pos_side})", level="debug")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"[OK] Leverage set to {leverage_val}x for {symbol} ({pos_side})", level="info")
                return True
            else:
                self.log(f"Failed to set leverage for {symbol}: {response.get('msg') if response else 'No response'}", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in okx_set_leverage: {e}", level="error")
            return False

    def _okx_set_position_mode(self, mode_val):
        try:
            # 1. First, check CURRENT position mode to avoid unnecessary errors
            path_get = "/api/v5/account/config"
            get_response = self._okx_request("GET", path_get)
            
            if get_response and get_response.get('code') == '0':
                current_mode = get_response['data'][0].get('posMode')
                if current_mode == mode_val:
                    self.log(f"[OK] Position mode already confirmed: {mode_val}", level="debug")
                    return True
                else:
                    self.log(f"Position mode mismatch (Current: {current_mode}, Target: {mode_val}). Attempting update...", level="info")
            
            # 2. Update if needed
            # Mode options: 'net_mode' (One-way) or 'long_short_mode' (Hedge)
            path_set = "/api/v5/account/set-position-mode"
            body = {"posMode": mode_val}
            
            self.log(f"Setting account position mode to {mode_val}... (Requires 0 positions/orders)", level="debug")
            response = self._okx_request("POST", path_set, body_dict=body)
            
            if response and response.get('code') == '0':
                self.log(f"[OK] Position mode set to {mode_val}", level="info")
                return True
            elif response and response.get('code') == '51000': # Already in this mode (backup check)
                self.log(f"[OK] Position mode already confirmed: {mode_val}", level="debug")
                return True
            else:
                self.log(f"Failed to set position mode: {response.get('msg') if response else 'No response'}", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_set_position_mode: {e}", level="error")
            return False

    def _get_ws_url(self, ws_type="public"):
        # Dynamic URL: Production vs Demo - Split for Public/Private separation
        is_testnet = self.config.get('use_testnet', False)
        base = "wspap.okx.com:8443" if is_testnet else "ws.okx.com:8443"
        return f"wss://{base}/ws/v5/{ws_type}"

    def _on_websocket_message(self, ws, message):
        # ws passed here could be ws_public or ws_private
        is_private = (ws == self.ws_private)
        try:
            msg = json.loads(message)
            # self.log(f"DEBUG: _on_websocket_message received parsed message: {msg}", level="debug")

            # Handle event messages (subscribe/login)
            if 'event' in msg:
                if msg['event'] == 'subscribe':
                    arg = msg.get('arg', {})
                    prefix = "private" if is_private else "public"
                    channel_id = f"{prefix}:{arg.get('channel')}:{arg.get('instId') if arg.get('instId') else ''}"
                    self.log(f"Subscription confirmed for {channel_id}", level="debug")
                    self.confirmed_subscriptions.add(channel_id)
                    if self.pending_subscriptions == self.confirmed_subscriptions:
                        self.log("All WebSocket subscriptions are ready.", level="debug")
                        self.ws_subscriptions_ready.set()
                elif msg['event'] == 'login':
                    if msg.get('code') == '0':
                        self.log("WebSocket Login Successful.", level="info")
                        self._send_websocket_subscriptions(ws_type="private")
                    else:
                        self.log(f"WebSocket Login Failed: {msg.get('msg')}", level="error")
                elif msg['event'] == 'channel-conn-count':
                    # Status message from OKX, ignore to reduce noise
                    pass
                else: # Log other event messages
                    self.log(f"Received non-subscribe event message: {msg}", level="warning")
                # Do NOT return here, allow further processing if it's a data message that also has an event.
            
            if 'data' in msg:
                channel = msg.get('arg', {}).get('channel', '')
                data = msg.get('data', [])

                if channel == 'trades' and data:
                    with self.trade_data_lock:
                        self.latest_trade_timestamp = int(data[-1].get('ts'))
                        self.latest_trade_price = safe_float(data[-1].get('px'))
                        self.last_price_update_time = time.time()

                elif channel == 'tickers' and data:
                    # Process ticker data to update latest_trade_price
                    # The `last` field from ticker data represents the current price
                    self.latest_trade_price = safe_float(data[0].get('last'))
                    self.last_price_update_time = time.time()
                    
                    # Trigger real-time metric update if in position for instant UI feedback
                    has_pos = False
                    with self.position_lock:
                        if any(self.in_position.values()):
                            has_pos = True
                    
                    if has_pos:
                        # Trigger a fast re-calculation of PnL and emit
                        self._update_realtime_metrics_from_price()

                elif channel == 'account' and data:
                    # Real-time balance updates
                    with self.account_info_lock:
                        for detail in data[0].get('details', []):
                            if detail.get('ccy') == 'USDT':
                                self.account_balance = safe_float(detail.get('bal'))
                                self.total_balance = safe_float(detail.get('bal'))
                                self.total_equity = safe_float(data[0].get('totalEq')) if data[0].get('totalEq') else safe_float(detail.get('eq'))
                                
                                # Update Effective Wallet Balance base whenever exchange gives fresh Equity/PnL
                                with self.position_lock:
                                    self.effective_wallet_balance = self.total_equity - getattr(self, 'cached_unrealized_pnl', 0.0)
                                self.available_balance = safe_float(detail.get('availBal'))
                                
                                self.log(f"Real-time Balance Update: {self.total_equity} USDT", level="debug")
                                break
                    # Emit update to frontend
                    self._emit_socket_updates()

                elif channel == 'positions' and data:
                    self.log(f"Real-time Position Update received ({len(data)} items)", level="debug")
                    self._process_account_positions(data, is_snapshot=False)

                # ---------------------------------------------------------
                # REAL-TIME UI UPDATES (TRANSITIONAL)
                # ---------------------------------------------------------
                # Triggered on every price tick to keep the dashboard responsive
                if self.latest_trade_price:
                    now = time.time()
                    # Throttle emissions to max 2 per second to prevent UI flooding
                    if now - self.last_emit_time >= 0.5:
                        self.last_emit_time = now
                        self.emit('price_update', {'price': self.latest_trade_price, 'symbol': self.config['symbol']})
                # ---------------------------------------------------------

        except json.JSONDecodeError:
            self.log(f"DEBUG: Non-JSON WebSocket message received: {message[:500]}", level="debug")
        except Exception as e:
            self.log(f"Exception in on_websocket_message: {e}", level="error")

    def _on_websocket_open(self, ws):
        if ws == self.ws_private:
            self.log("OKX PRIVATE WebSocket opened. Logging in...", level="info")
            self._login_websocket()
        else:
            self.log("OKX PUBLIC WebSocket opened. Subscribing...", level="info")
            self._send_websocket_subscriptions(ws_type="public")

    def _login_websocket(self):
        try:
            timestamp = str(int(time.time()))
            method = "GET"
            path = "/users/self/verify"
            
            message = timestamp + method + path
            signature = hmac.new(self.okx_api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
            signature_base64 = base64.b64encode(signature).decode('utf-8')
            
            login_payload = {
                "op": "login",
                "args": [{
                    "apiKey": self.okx_api_key,
                    "passphrase": self.okx_passphrase,
                    "timestamp": timestamp,
                    "sign": signature_base64
                }]
            }
            self.ws_private.send(json.dumps(login_payload))
        except Exception as e:
            self.log(f"WebSocket login error: {e}", level="error")

    def _send_websocket_subscriptions(self, ws_type="public"):
        ws = self.ws_private if ws_type == "private" else self.ws_public
        if not ws: return

        self.subscribed_symbol = self.config['symbol']
        
        if ws_type == "public":
            channels = [
                {"channel": "trades", "instId": self.subscribed_symbol},
                {"channel": "tickers", "instId": self.subscribed_symbol}
            ]
        else:
            channels = [
                {"channel": "account"}, 
                {"channel": "positions", "instType": "ANY"}
            ]
        
        subscription_payload = {
            "op": "subscribe",
            "args": channels
        }
        self.log(f"WS ({ws_type}) Sending subscriptions: {json.dumps(subscription_payload)}", level="debug")
        ws.send(json.dumps(subscription_payload))
        self.pending_subscriptions.update({f"{ws_type}:{arg['channel']}:{arg.get('instId', '')}" for arg in channels})
    def _on_websocket_error(self, ws_app, error):
        self.log(f"OKX WebSocket error: {error}", level="error")

    def _on_websocket_close(self, ws_app, close_status_code, close_msg):
        self.log(f"OKX WebSocket closed. Status: {close_status_code}, Msg: {close_msg}", level="debug")
        # No longer spawning a new thread here. 
        # The reconnection is now handled by the loop in _initialize_websocket_and_start_main_loop.

    def _fetch_initial_historical_data(self, symbol, timeframe, start_date_str, end_date_str):
        with self.data_lock:
            try:
                start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                start_ts_ms = int(start_dt.timestamp() * 1000)
                end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                end_ts_ms = int(end_dt.timestamp() * 1000)

                raw_data = self._fetch_historical_data_okx(symbol, timeframe, start_ts_ms, end_ts_ms)

                if raw_data:
                    df = pd.DataFrame(raw_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)

                    if df.empty:
                        self.log(f"No valid data for {timeframe}", level="error")
                        return False

                    invalid_rows = df[(df['Low'] > df['High']) |
                                    (df['Open'] < df['Low']) | (df['Open'] > df['High']) |
                                    (df['Close'] < df['Low']) | (df['Close'] > df['High'])]

                    if not invalid_rows.empty:
                        self.log(f"WARNING: Found {len(invalid_rows)} invalid OHLC rows", level="warning")
                        df = df[(df['Low'] <= df['High'])]

                    df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
                    df = df.set_index('Datetime')
                    df = df[~df.index.duplicated(keep='first')]
                    df = df.sort_index()

                    self.historical_data_store[timeframe] = df

                    self.log(f"Loaded {len(df)} candles for {timeframe}", level="debug")
                    return True
                else:
                    self.log(f"Failed to fetch data for {timeframe}", level="error")
                    return False
            except Exception as e:
                self.log(f"Exception in _fetch_initial_historical_data: {e}", level="error")
                return False

    def _okx_place_order(self, symbol, side, qty, price=None, order_type="Market",
                        time_in_force=None, reduce_only=False,
                        stop_loss_price=None, take_profit_price=None, posSide=None, verbose=True, tdMode=None):
        try:
            path = "/api/v5/trade/order"
            price_precision = self.product_info.get('pricePrecision', 4)
            qty_precision = self.product_info.get('qtyPrecision', 8)

            order_qty_str = f"{qty:.{qty_precision}f}"
            
            # Use provided tdMode or default to config
            trade_mode = tdMode if tdMode else self.config.get('mode', 'cross')

            body = {
                "instId": symbol,
                "tdMode": trade_mode,
                "side": side.lower(),
                "ordType": order_type.lower(),
                "sz": order_qty_str,
            }

            if (self.config.get('hedge_mode', False) or self.config.get('okx_pos_mode') == 'long_short_mode') and posSide:
                body["posSide"] = posSide

            if order_type.lower() == "limit" and price is not None:
                body["px"] = f"{price:.{price_precision}f}"

            if time_in_force:
                if time_in_force == "GoodTillCancel":
                    body["timeInForce"] = "GTC"
                else:
                    body["timeInForce"] = time_in_force

            if reduce_only:
                body["reduceOnly"] = True

            # Attach TP/SL via attachAlgoOrds (Correct V5 Structure)
            attach_algo_list = []
            algo_details = {}
            has_algo = False
            
            # Ensure posSide is passed to algo if present in parent order (Critical for Long/Short mode)
            if "posSide" in body:
                algo_details["posSide"] = body["posSide"]

            if take_profit_price and safe_float(take_profit_price) > 0:
                algo_details["tpTriggerPx"] = str(take_profit_price)
                algo_details["tpOrdPx"] = "-1" # Market TP
                algo_details["tpTriggerPxType"] = "last"
                has_algo = True

            if stop_loss_price and safe_float(stop_loss_price) > 0:
                algo_details["slTriggerPx"] = str(stop_loss_price)
                algo_details["slOrdPx"] = "-1" # Market SL
                algo_details["slTriggerPxType"] = "last"
                has_algo = True
                
            if has_algo:
                attach_algo_list.append(algo_details)
                body["attachAlgoOrds"] = attach_algo_list

            self.log(f"DEBUG: Order placement request body: {body}", level="debug")
            if verbose:
                self.log(f"Placing {order_type} {side} order for {order_qty_str} {symbol} at {price}", level="info")
            
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                order_data = response.get('data', [])
                if order_data and order_data[0].get('ordId'):
                    if verbose:
                        self.log(f"[OK] Order placed: OrderID={order_data[0]['ordId']}", level="info")
                    
                    # Trigger immediate account refresh for UI responsiveness
                    def _update_after_reconnect():
                        try:
                            self._sync_account_data()
                            self._emit_socket_updates()
                        except Exception as e:
                            self.log(f"Error in reconnect update: {e}", level="error")
                    threading.Thread(target=_update_after_reconnect, daemon=True).start()
                    return order_data[0]
                else:
                    self.log(f"[FAIL] Order placement failed: No order ID in response. Response: {response}", level="error")
                    return None
            else:
                error_msg = response.get('msg', 'Unknown error') if response else 'No response'
                self.log(f"[FAIL] Order placement failed: {error_msg}. Response: {response}", level="error")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_order: {e}", level="error")
            return None

    def _okx_place_algo_order(self, body, verbose=True):
        try:
            path = "/api/v5/trade/order-algo"
            if verbose:
                self.log(f"Placing algo order", level="info")
            response = self._okx_request("POST", path, body_dict=body)
            if response and response.get('code') == '0':
                data = response.get('data', [])
                if data and (data[0].get('algoId') or data[0].get('ordId')):
                    if verbose:
                        self.log(f"[OK] Algo order placed", level="info")
                    return data[0]
                else:
                    self.log(f"[FAIL] Algo order placed but no algoId/ordId returned: {response}", level="error")
                    return None
            else:
                self.log(f"[FAIL] Algo order failed: {response}", level="debug")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_algo_order: {e}", level="debug")
            return None

    def _okx_cancel_order(self, symbol, order_id, reason=None):
        try:
            path = "/api/v5/trade/cancel-order"
            body = {
                "instId": symbol,
                "ordId": order_id,
            }

            log_msg = f"Cancelling OKX order {order_id[:12]}..."
            if reason:
                log_msg = f"Cancelling OKX order {order_id[:12]} ({reason})..."
            self.log(log_msg, level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"[OK] Order cancelled", level="info")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Order already filled/cancelled (OK)", level="info")
                return True
            else:
                self.log(f"Failed to cancel order (OK, continuing): {response.get('msg') if response else 'No response'}", level="debug")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_order: {e}", level="debug")
            return False

    def _okx_cancel_algo_order(self, symbol, algo_id):
        try:
            # Use the modern plural endpoint for canceling algo orders
            path = "/api/v5/trade/cancel-algos"
            body = [{
                "instId": symbol,
                "algoId": algo_id,
            }]

            self.log(f"Cancelling OKX algo order {str(algo_id)[:12]}...", level="debug")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"[OK] Algo order cancelled", level="debug")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Algo order already filled/cancelled (OK)", level="debug")
                return True
            else:
                self.log(f"Failed to cancel algo order (OK, continuing): {response.get('msg') if response else 'No response'}", level="debug")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_algo_order: {e}", level="error")
            return False

    def _close_all_entry_orders(self):
        try:
            self.log("Attempting to close unfilled linear entry orders...", level="info")

            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') == '0':
                self.log("No orders found or API error (OK if no orders)", level="info")
                return True

            orders = response.get('data', [])
            cancelled_count = 0

            for order in orders:
                try:
                    order_id = order.get('ordId')
                    status = order.get('state')
                    side = order.get('side')
                    if side == 'buy' and status not in ['filled', 'canceled', 'rejected']:
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error processing OKX order: {e}", level="error")

            if cancelled_count > 0:
                self.log(f"[OK] Closed {cancelled_count} unfilled linear entry orders", level="info")
            else:
                self.log(f"No unfilled linear entry orders to close (OK)", level="info")

            return True
        except Exception as e:
            self.log(f"Exception in _close_all_entry_orders: {e} (continuing)", level="error")
            return True

    def _handle_tp_hit(self, side='long'):
        with self.tp_hit_lock:
            self.tp_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log(f"[TARGET] TP HIT ({side.upper()}) - EXECUTING PROTOCOL", level="info")
            self.log("=" * 80, level="info")

            self.log("Step 1: Closing unfilled entry orders...", level="info")
            self._close_all_entry_orders()

            time.sleep(1)

            self.log(f"Step 2: Checking {side.upper()} OKX position status...", level="info")
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            position_still_open = False
            open_qty = 0.0

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol'] and pos.get('posSide', 'net') == side:
                        pos_qty_raw = safe_float(pos.get('pos', '0'))
                        if abs(pos_qty_raw) > 0:
                            position_still_open = True
                            open_qty = abs(pos_qty_raw)
                            self.log(f"OKX {side.upper()} position still open: {open_qty} (partial fill)", level="info")
                            break

            if position_still_open and open_qty > 0:
                self.log("Step 3: Waiting 3 seconds for liquidity...", level="info")
                time.sleep(3)

                self.log(f"Step 4: Market closing remaining {side.upper()} position...", level="info")
                close_side = "Sell" if side == 'long' else "Buy"
                exit_order_response = self._okx_place_order(self.config['symbol'], close_side, open_qty, order_type="Market", reduce_only=True, posSide=side)

                if exit_order_response and exit_order_response.get('ordId'):
                    self.log(f"[OK] Market close order placed for {open_qty} {self.config['symbol']} ({side})", level="info")
                    self.log(f"[DONE] Close Position (TP Partial): {side.upper()} {self.config['symbol']} | Qty: {open_qty}", level="info")
                
                time.sleep(1)
                self._cancel_all_exit_orders_and_reset(f"TP hit - {side} closed", side=side)
            else:
                self.log(f"OKX {side.upper()} position fully closed or not found. No market close needed.", level="info")
                self.log(f"[DONE] Close Position (TP): {side.upper()} {self.config['symbol']}", level="info")
                self._cancel_all_exit_orders_and_reset(f"TP hit - {side} fully closed", side=side)

            with self.tp_hit_lock:
                self.tp_hit_triggered = False

            self.log(f"[OK] {side.upper()} TP HIT PROTOCOL COMPLETE", level="info")

        except Exception as e:
            self.log(f"Exception in _handle_tp_hit ({side}): {e}", level="error")
            with self.tp_hit_lock:
                self.tp_hit_triggered = False

    def _handle_eod_exit(self):
        try:
            self.log("=" * 80, level="info")
            self.log("🕐 EOD EXIT TRIGGERED (OKX)", level="info")
            self.log("=" * 80, level="info")

            self.log("Step 1: Closing all open OKX positions...", level="info")
            try:
                path = "/api/v5/account/positions"
                params = {"instType": "SWAP", "instId": self.config['symbol']}
                response = self._okx_request("GET", path, params=params)

                if response and response.get('code') == '0':
                    positions = response.get('data', [])
                    for pos in positions:
                        if pos.get('instId') == self.config['symbol']:
                            size_rv = safe_float(pos.get('pos', 0))
                            if abs(size_rv) > 0:
                                pos_side = pos.get('posSide', 'net')
                                close_side = "Sell" if size_rv > 0 else "Buy"
                                
                                self.log(f"Found active {pos_side} position: {size_rv} - closing...", level="info")
                                exit_order_response = self._okx_place_order(self.config['symbol'], close_side, abs(size_rv), order_type="Market", reduce_only=True, posSide=pos_side)
                                if exit_order_response and exit_order_response.get('ordId'):
                                    self.log(f"[OK] {pos_side.upper()} close order placed", level="info")
                                else:
                                    self.log(f"⚠ {pos_side.upper()} close failed (OK if closed)", level="warning")
                                time.sleep(0.5)
                else:
                    self.log("No OKX positions found or API error (OK)", level="info")
            except Exception as e:
                self.log(f"Error closing OKX positions: {e} (OK, continuing)", level="warning")

            self.log("Step 2: Closing unfilled entry orders...", level="info")
            try:
                self._close_all_entry_orders()
            except Exception as e:
                self.log(f"Error closing entry orders: {e} (OK, continuing)", level="warning")

            time.sleep(0.5)

            self.log("Step 3: Force cancelling all remaining OKX orders...", level="info")
            try:
                path = "/api/v5/trade/cancel-all-after"
                body = {"timeOut": "0", "instType": "SWAP"}
                response = self._okx_request("POST", path, body_dict=body)
                if response and response.get('code') == '0':
                    self.log(f"[OK] All OKX orders cancelled", level="info")
                else:
                    self.log(f"⚠ All OKX orders cancel response: {response} (OK)", level="warning")
            except Exception as e:
                self.log(f"Error force cancelling OKX orders: {e} (OK, continuing)", level="error")

            self.log("=" * 80, level="info")
            self.log("[OK] EOD EXIT COMPLETE (OKX)", level="info")
            self.log("=" * 80, level="info")

            self._cancel_all_exit_orders_and_reset("EOD Exit")

        except Exception as e:
            self.log(f"Exception in _handle_eod_exit (OKX): {e} (continuing)", level="error")
            self._cancel_all_exit_orders_and_reset("EOD Exit - forced")

    def _handle_order_update(self, orders_data):
        with self.position_lock:
            current_pending_id = self.pending_entry_order_id
            is_in_pos = self.in_position
            active_exit_orders = dict(self.position_exit_orders)
            tracked_qty = self.position_qty

    def _handle_order_update(self, orders_data):
        with self.position_lock:
             # Snapshot current states for directional mapping 
             active_exit_ids = {
                 'long': self.position_exit_orders.get('long', {}),
                 'short': self.position_exit_orders.get('short', {})
             }
             pending_entry_ids = list(self.pending_entry_ids)
             
        for order in orders_data:
            if not isinstance(order, dict): continue

            order_id = order.get('ordId') or order.get('algoId')
            status = order.get('state')
            symbol = order.get('instId')
            pos_side = order.get('posSide', 'net')
            
            # Map side for processing
            side_key = 'long'
            if pos_side == 'short': side_key = 'short'
            elif pos_side == 'net':
                # Map net based on order contents or current config if ambiguous
                side_key = self.config.get('direction', 'long')
                if side_key == 'both': side_key = 'long'

            if symbol != self.config['symbol']: continue

            # 1. SL HIT
            if order_id == active_exit_ids[side_key].get('sl') and status in ['filled', 'partially_filled']:
                with self.sl_hit_lock:
                    if not self.sl_hit_triggered:
                        self.sl_hit_triggered = True
                        threading.Timer(0.1, lambda s=side_key: self._handle_sl_hit(side=s)).start()
                return

            # 2. ENTRY FILLED
            if order_id in pending_entry_ids:
                cum_qty = safe_float(order.get('accFillSz', 0))
                with self.position_lock:
                    if order_id in self.pending_entry_order_details:
                        self.pending_entry_order_details[order_id]['status'] = status
                        self.pending_entry_order_details[order_id]['cum_qty'] = cum_qty

                if status in ['filled', 'partially_filled'] or cum_qty > 0:
                    self.log(f"🎉 ENTRY FILLED [{side_key.upper()}]: {cum_qty} {self.config['symbol']}", level="info")
                    if status == 'filled':
                        threading.Timer(2.0, lambda oid=order_id: self._confirm_and_set_active_position(oid)).start()
                    else:
                        threading.Timer(5.0, lambda oid=order_id: self._confirm_and_set_active_position(oid)).start()
                    return
                elif status in ['canceled', 'failed']:
                    self._reset_entry_state(f"Entry order {status}")
                    return

            # 3. TP HIT
            if order_id == active_exit_ids[side_key].get('tp') and status in ['filled', 'partially_filled']:
                with self.tp_hit_lock:
                    if not self.tp_hit_triggered:
                        self.tp_hit_triggered = True
                        threading.Timer(0.1, lambda s=side_key: self._handle_tp_hit(side=s)).start()
                return

    def _detect_sl_from_position_update(self, positions_msg):
        # Scan positions message for closures
        for pos in positions_msg:
            if pos.get('instId') == self.config['symbol']:
                pos_side = pos.get('posSide', 'net')
                side_key = 'long'
                if pos_side == 'short': side_key = 'short'
                elif pos_side == 'net':
                    side_key = self.config.get('direction', 'long')
                    if side_key == 'both': side_key = 'long'

                size_rv = safe_float(pos.get('pos', 0))
                
                with self.position_lock:
                    was_in = self.in_position[side_key]
                    exp_qty = self.position_qty[side_key]

                if was_in and size_rv == 0 and abs(exp_qty) > 0:
                    self.log(f"🛑 SL DETECTED [{side_key.upper()}] via WebSocket Position Update!", level="info")
                    with self.sl_hit_lock:
                        if not self.sl_hit_triggered:
                            self.sl_hit_triggered = True
                            threading.Timer(0.1, lambda s=side_key: self._handle_sl_hit(side=s)).start()


    def _handle_sl_hit(self, side='long'):
        with self.sl_hit_lock:
            self.sl_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log(f"🛑 STOP LOSS HIT ({side.upper()}) - EXECUTING CLEANUP", level="info")
            self.log("=" * 80, level="info")

            self.log(f"{side.upper()} position already closed by exchange SL", level="info")

            try:
                self._close_all_entry_orders()
            except: pass

            time.sleep(0.5)

            self.log(f"Cancelling {side.upper()} TP order and resetting state...", level="info")
            self.log(f"[DONE] Close Position (SL): {side.upper()} {self.config['symbol']}", level="info")
            self._cancel_all_exit_orders_and_reset(f"SL hit - {side} closed by exchange", side=side)
            
            # Trigger immediate account refresh for UI responsiveness
            def _update_after_init():
                try:
                    self._sync_account_data()
                    self._emit_socket_updates()
                except Exception as e:
                    self.log(f"Error in init update: {e}", level="error")
            threading.Thread(target=_update_after_init, daemon=True).start()

            with self.sl_hit_lock:
                self.sl_hit_triggered = False
            self.log(f"[OK] {side.upper()} SL CLEANUP COMPLETE", level="info")
        except Exception as e:
            self.log(f"Exception in _handle_sl_hit ({side}): {e}", level="error")
            self._cancel_all_exit_orders_and_reset(f"SL hit - {side} forced reset", side=side)
            with self.sl_hit_lock:
                self.sl_hit_triggered = False

    def _confirm_and_set_active_position(self, filled_order_id):
        try:
            self.log(f"Confirming OKX position for filled order ID: {filled_order_id}", level="debug")

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)
            self.log(f"DEBUG: Response from /api/v5/account/positions: {response}", level="debug")

            entry_confirmed = False
            actual_entry_price = 0.0
            actual_qty = 0.0
            actual_side = None
            found_pos_side = None

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                self.log(f"DEBUG: Positions data from OKX: {positions}", level="debug")
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        pos_qty_str = pos.get('pos', '0')
                        size_val = safe_float(pos_qty_str)
                        if abs(size_val) > 0:
                            avg_entry_price_rv = safe_float(pos.get('avgPx', 0))
                            actual_entry_price = avg_entry_price_rv
                            actual_qty = size_val
                            entry_confirmed = True
                            # Determine actual side if 'net'
                            found_pos_side = pos.get('posSide')
                            if found_pos_side == 'net' or not found_pos_side:
                                actual_side = 'short' if size_val < 0 else 'long'
                            else:
                                actual_side = found_pos_side
                            
                            self.log(f"DEBUG: Confirmed active {actual_side} position - Entry Price: {actual_entry_price}, Quantity: {actual_qty}", level="debug")
                            break

            if not entry_confirmed or actual_entry_price <= 0:
                self.log("CRITICAL: Could not confirm OKX position or invalid entry price!", level="error")
                self.log(f"DEBUG: entry_confirmed: {entry_confirmed}, actual_entry_price: {actual_entry_price}", level="debug")
                return


            tp_price = 0.0
            sl_price = 0.0
            tp_off = self.config.get('tp_price_offset', 0)
            sl_off = self.config.get('sl_price_offset', 0)

            if actual_side == 'long':
                if tp_off and safe_float(tp_off) > 0:
                    tp_price = actual_entry_price + safe_float(tp_off)
                else:
                    self.log(f"Confirm Pos: TP offset is null or 0 for {actual_side.upper()}. Skipping TP calc.", level="info")

                if sl_off and safe_float(sl_off) > 0:
                    sl_price = actual_entry_price - safe_float(sl_off)
                else:
                    self.log(f"Confirm Pos: SL offset is null or 0 for {actual_side.upper()}. Skipping SL calc.", level="info")
                exit_order_side = "sell"
            else: # short
                if tp_off and safe_float(tp_off) > 0:
                    tp_price = actual_entry_price - safe_float(tp_off)
                else:
                    self.log(f"Confirm Pos: TP offset is null or 0 for {actual_side.upper()}. Skipping TP calc.", level="info")

                if sl_off and safe_float(sl_off) > 0:
                    sl_price = actual_entry_price + safe_float(sl_off)
                else:
                    self.log(f"Confirm Pos: SL offset is null or 0 for {actual_side.upper()}. Skipping SL calc.", level="info")
                exit_order_side = "buy"
            
            with self.position_lock:
                self.in_position[actual_side] = True
                self.position_entry_price[actual_side] = actual_entry_price
                self.position_qty[actual_side] = actual_qty
                self.current_take_profit[actual_side] = tp_price
                self.current_stop_loss[actual_side] = sl_price
                self.pending_entry_order_id = None
                self.position_exit_orders[actual_side] = {}

                # Emit position update for this side
                self.emit('position_update', {
                    'in_position': self.in_position[actual_side],
                    'position_entry_price': self.position_entry_price[actual_side],
                    'position_qty': self.position_qty[actual_side],
                    'current_take_profit': self.current_take_profit[actual_side],
                    'current_stop_loss': self.current_stop_loss[actual_side],
                    'side': actual_side 
                })

            self.log(f"OKX {actual_side.upper()} POSITION OPENED", level="info")
            self.log(f"Entry: ${actual_entry_price:.2f} | Qty: {actual_qty}", level="info")
            self.log(f"TP: ${tp_price:.2f} | SL: ${sl_price:.2f}", level="info")

            # Check for existing TP/SL orders (Atomic Fallback Check)
            existing_tp = False
            existing_sl = False
            try:
                algo_path = "/api/v5/trade/orders-algo-pending"
                algo_params = {"instId": self.config['symbol'], "ordType": "conditional"}
                algo_res = self._okx_request("GET", algo_path, params=algo_params)
                if algo_res and algo_res.get('code') == '0':
                    for ord in algo_res.get('data', []):
                         # Check if order is for this position side (Long/Short)
                         # OKX 'posSide' in algo order details usually matches 'long'/'short' or 'net'
                         # We check direction: Long Pos -> Sell Order, Short Pos -> Buy Order
                         if actual_side == 'long' and ord['side'] == 'sell':
                             if ord.get('slTriggerPx') and safe_float(ord['slTriggerPx']) > 0: existing_sl = True
                             if ord.get('tpTriggerPx') and safe_float(ord['tpTriggerPx']) > 0: existing_tp = True
                         elif actual_side == 'short' and ord['side'] == 'buy':
                             if ord.get('slTriggerPx') and safe_float(ord['slTriggerPx']) > 0: existing_sl = True
                             if ord.get('tpTriggerPx') and safe_float(ord['tpTriggerPx']) > 0: existing_tp = True
                
                self.log(f"Atomic TP/SL Check: TP={'Found' if existing_tp else 'Missing'}, SL={'Found' if existing_sl else 'Missing'}", level="debug")

            except Exception as e:
                 self.log(f"Failed to check existing algo orders: {e}", level="warning")

            price_precision = self.product_info.get('pricePrecision', 4)
            qty_precision = self.product_info.get('qtyPrecision', 8)

            # Place TP and SL as algo (conditional) orders via /api/v5/trade/order-algo
            # ONLY IF MISSING (Smart Fallback) and IF OFFSET IS CONFIGURED
            if not existing_tp:
                if tp_off and safe_float(tp_off) > 0:
                    tp_body = {
                        "instId": self.config['symbol'],
                        "tdMode": self.config.get('mode', 'cross'),
                        "side": exit_order_side,
                        "posSide": actual_side, 
                        "ordType": "conditional",
                        "sz": f"{(abs(actual_qty) * (self.config.get('tp_amount', 100) / 100)):.{qty_precision}f}",
                        "tpTriggerPx": f"{tp_price:.{price_precision}f}",
                        "tpOrdPx": "-1" if self.config.get('tp_mode', 'market') == 'market' else f"{tp_price:.{price_precision}f}",
                        "reduceOnly": "true"
                    }

                    tp_order = self._okx_place_algo_order(tp_body)
                    if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                        algo_id = tp_order.get('algoId') or tp_order.get('ordId')
                        with self.position_lock:
                            self.position_exit_orders[actual_side]['tp'] = algo_id
                        self.log(f"[OK] TP algo order placed for {actual_side.upper()} at ${tp_price:.2f}", level="info")
                    else:
                        self.log(f"❌ Failed to place TP algo order: {tp_order}", level="error")
                        self._execute_trade_exit(f"Failed to place TP for {actual_side}", side=actual_side)
                        return
                else:
                    self.log(f"Skipping TP placement for {actual_side.upper()} (No offset configured)", level="info")
            else:
                self.log("TP algo order already exists (Atomic). Skipping redundant placement.", level="info")
                algo_id = tp_order.get('ordId') # Handle the fake/atomic id
                with self.position_lock:
                    self.position_exit_orders[actual_side]['tp'] = algo_id

            if not existing_sl:
                if sl_off and safe_float(sl_off) > 0:
                    sl_body = {
                        "instId": self.config['symbol'],
                        "tdMode": self.config.get('mode', 'cross'),
                        "side": exit_order_side,
                        "posSide": actual_side,
                        "ordType": "conditional",
                        "sz": f"{(abs(actual_qty) * (self.config.get('sl_amount', 100) / 100)):.{qty_precision}f}",
                        "slTriggerPx": f"{sl_price:.{price_precision}f}",
                        "slOrdPx": "-1", # market
                        "reduceOnly": "true"
                    }

                    sl_order = self._okx_place_algo_order(sl_body)
                    if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                        algo_id = sl_order.get('algoId') or sl_order.get('ordId')
                        with self.position_lock:
                            self.position_exit_orders[actual_side]['sl'] = algo_id
                        self.log(f"[OK] SL algo order placed for {actual_side.upper()} at ${sl_price:.2f}", level="info")
                    else:
                        self.log(f"❌ Failed to place SL algo order: {sl_order}", level="error")
                        self._execute_trade_exit(f"Failed to place SL for {actual_side}", side=actual_side)
                        return
                else:
                    self.log(f"Skipping SL placement for {actual_side.upper()} (No offset configured)", level="info")
            else:
                 self.log("SL algo order already exists (Atomic). Skipping redundant placement.", level="info")
                 algo_id = sl_order.get('ordId')
                 with self.position_lock:
                     self.position_exit_orders[actual_side]['sl'] = algo_id

        except Exception as e:
            self.log(f"Exception in _confirm_and_set_active_position (OKX): {e}", level="error")


    def _execute_trade_exit(self, reason, side=None):
        """
        Authoritative Account Reset: Fetches ALL positions and orders directly from OKX
        and closes/cancels everything to leave NOTHING behind.
        """
        with self.exit_lock:
            if self.authoritative_exit_in_progress:
                self.log(f"Join: Authoritative exit already in progress. Ignoring trigger: {reason}", level="debug")
                return
            self.authoritative_exit_in_progress = True

        try:
            target_symbol = self.config['symbol']
            self.log(f"=== EMERGENCY EXIT === Reason: {reason} | Symbol: {target_symbol}", level="info")

            # 1. Fetch CURRENT positions directly from exchange
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": target_symbol}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                positions_data = response.get('data', [])
                for pos in positions_data:
                    # Filter for our symbol just in case, though OKX params should handle it
                    if pos.get('instId') == target_symbol:
                        pos_qty = safe_float(pos.get('pos', '0'))
                        pos_side_raw = pos.get('posSide', 'net')
                        mgn_mode = pos.get('mgnMode') # Extract margin mode (cross/isolated)
                        
                        if abs(pos_qty) > 0:
                            # Record realized PnL before closing
                            unrealized_pnl = safe_float(pos.get('upl', '0'))
                            if unrealized_pnl > 0:
                                self.total_trade_profit += unrealized_pnl
                            else:
                                self.total_trade_loss += abs(unrealized_pnl)
                            self.net_trade_profit = self.total_trade_profit - self.total_trade_loss
                            
                            # Determine close side (If qty > 0 [Long], Sell. If qty < 0 [Short], Buy)
                            close_side = "Sell" if pos_qty > 0 else "Buy"
                            
                            self.log(f"Force closing {pos_side_raw.upper()} position: {abs(pos_qty)} {target_symbol} @ Market (Mode: {mgn_mode})", level="info")
                            # Pass mgn_mode as tdMode to ensure we address the position in the correct margin account
                            exit_order = self._okx_place_order(target_symbol, close_side, abs(pos_qty), order_type="Market", reduce_only=True, posSide=pos_side_raw, tdMode=mgn_mode)
                            
                            if exit_order and exit_order.get('ordId'):
                                self.log(f"[OK] Position closed. Order ID: {exit_order.get('ordId')}", level="info")
                                self.log(f"[DONE] Close Position (Auth): {pos_side_raw.upper()} {target_symbol} | Reason: {reason}", level="info")
                                self.log(f"[PROFIT] Realized PnL: ${unrealized_pnl:.2f} | Session Total: ${self.net_trade_profit:.2f}", level="info")
                            else:
                                self.log(f"⚠️ Market exit for {pos_side_raw.upper()} failed or rejected.", level="warning")


            # 2. Batch Cancel ALL pending orders for this symbol (Limit & Algo)
            # We call batch_cancel_orders which performs the exchange-wide sweep
            self.batch_cancel_orders()

            # 3. Synchronize internal state to avoid ghost tracking
            with self.position_lock:
                self.in_position = {'long': False, 'short': False}
                self.position_qty = {'long': 0.0, 'short': 0.0}
                self.position_entry_price = {'long': 0.0, 'short': 0.0}
                self.position_exit_orders = {'long': {}, 'short': {}}
                self.pending_entry_ids = []
                self.pending_entry_order_details = {}

        except Exception as e:
            self.log(f"CRITICAL ERROR in _execute_trade_exit: {e}", level="error")
        finally:
            with self.exit_lock:
                self.authoritative_exit_in_progress = False
            self.log("=== EMERGENCY EXIT COMPLETE === Account cleared for symbol.", level="info")

    def _check_auto_add_position_step(self, current_price, current_side, remaining_budget):
        # Log condition check (User Request)
        # Only log if monitoring tick allows to avoid spam, but client wants VISIBILITY.
        if self.monitoring_tick % 6 == 0:
            self.log("Check Add Position Condition")

        # GAP-BASED AUTO-ADD LOGIC
        # Trigger: Market Price is [GAP] more than Average Entry Price.
        # Sizing: Step 1 = 1x Price, Step 2 = 2x Price (if scaling enabled).
        
        base_gap = self.config.get('add_pos_gap_threshold', 5.0)
        gap_offset = self.config.get('add_pos_gap_offset', 0.0)
        gap_threshold = base_gap + (self.auto_add_step_count * gap_offset)
        gap_offset = self.config.get('add_pos_gap_offset', 0.0)
        size_pct_offset = self.config.get('add_pos_size_pct_offset', 0.0)

        # Apply offsets for subsequent steps
        if self.auto_add_step_count > 0:
            gap_threshold += gap_offset
        
        # 1. Get Average Entry Price
        avg_entry = 0.0
        with self.position_lock:
            if current_side == 'long':
                avg_entry = self.position_entry_price.get('long', 0.0)
            else:
                avg_entry = self.position_entry_price.get('short', 0.0)
        
        if avg_entry <= 0: return # No position to add to

        # 2. Check Gap (Strict Loss Condition for Averaging Down)
        # User Requirement: "For short: When PnL is loss... if market > entry gap 5... add orders"
        # This means we ONLY trigger if price moves AGAINST the position.

        gap_magnitude = 0.0
        is_loss_gap = False

        if current_side == 'long':
            # Long: Loss if Market < Entry. Gap = Entry - Market
            if current_price < avg_entry:
                gap_magnitude = avg_entry - current_price
                is_loss_gap = True
        else: # short
            # Short: Loss if Market > Entry. Gap = Market - Entry
            if current_price > avg_entry:
                gap_magnitude = current_price - avg_entry
                is_loss_gap = True

        # If it's a profit gap (or flat), we do NOT add.
        if not is_loss_gap:
            # self.log(f"[DEBUG] Position in Profit or Zero Gap. No Averaging Down.", level="debug")
            return

        if gap_magnitude < gap_threshold:
             # self.log(f"[DEBUG] Gap {gap_magnitude:.2f} < {gap_threshold}. No Add.", level="debug")
             return # No gap trigger

        # 3. Mode 1 Specific Check: Stop adding if PnL already near zero
        # Requirement: "Run max 6 loops to make PnL near 0" - stop when goal achieved
        if self.config.get('use_add_pos_above_zero', False):
            trade_fee_pct = self.config.get('trade_fee_percentage', 0.07)
            
            # Calculate current position size for fee calculation
            current_total_notional = 0.0
            with self.position_lock:
                if current_side == 'long':
                    current_total_notional = self.okx_position_notional.get('long', 0)
                else:
                    current_total_notional = self.okx_position_notional.get('short', 0)
            
            if current_total_notional > 0:
                current_size_fee = current_total_notional * (trade_fee_pct / 100.0)
                near_zero_threshold = max(1.0, current_size_fee * 0.1)
                
                # If PnL is already near zero, don't add more
                if self.net_profit >= -near_zero_threshold:
                    self.log(f"Mode 1 Check PnL near zero (${self.net_profit:.2f}). No more adding needed.", level="info")
                    return

        # 4. Check Max Loops
        max_loops = self.config.get('add_pos_max_count', 10)
        if self.auto_add_step_count >= max_loops:
             self.log(f"Auto-Add Check SKIPPED: Max Loops Reached ({self.auto_add_step_count} >= {max_loops})", level="warning")
             return

        # 5. Calculate Add Amount (Percentage of Current Size)
        # Requirement: "Order amount is the Newest Size Amount 30%"
        size_pct_base = self.config.get('add_pos_size_pct', 30.0)
        size_pct = (size_pct_base + (self.auto_add_step_count * size_pct_offset)) / 100.0
        
        # We need CURRENT TOTAL SIZE (Notional)
        current_total_notional = 0.0
        with self.position_lock:
             if current_side == 'long':
                 # Calculate from currently tracked position
                 current_total_notional = abs(safe_float(self.position_details.get('long', {}).get('notionalUsd', 0))) 
                 # Or use okx_pos_notional passed in?
             else:
                 current_total_notional = abs(safe_float(self.position_details.get('short', {}).get('notionalUsd', 0)))
        
        # Fallback if position detail is missing but we are here (shouldn't happen much)
        if current_total_notional == 0:
             # Try getting from open trades? No, this is triggered when we HAVE a position.
             return

        add_notional = current_total_notional * size_pct
        
        step_count = self.auto_add_step_count + 1 # 1-based current step
        
        # LOGGING CRITICAL STEPS FOR USER VISIBILITY
        self.log(f"=== AUTO-ADD TRIGGERED (Step {step_count}) ===", level="warning")
        self.log(f"1. Gap Check: Market {current_price:.2f} vs AvgEntry {avg_entry:.2f} | Gap {gap_magnitude:.2f} > Threshold {gap_threshold}", level="info")
        self.log(f"2. Sizing: Current Size ${current_total_notional:.2f} x {size_pct*100:.1f}% = ${add_notional:.2f}", level="info")
        
        # 5. Calculate Margin to Deduct from Capital 2nd
        # "Order amount is the AFTER leverage... minus amount from total capital 2nd"
        # Be careful: "Order amount" usually means Notional. 
        # But user says: "divide leverage 100=4.5, minus 4.5 from total capital 2nd".
        # So Capital 2nd tracks MARGIN ("Real Money Used"), not Notional.
        
        # Get leverage
        current_leverage = 1.0
        with self.position_lock:
            if current_side == 'long':
                current_leverage = self.position_details.get('long', {}).get('lever', 1.0)
            else:
                current_leverage = self.position_details.get('short', {}).get('lever', 1.0)
        current_lever_float = safe_float(current_leverage, 1.0)
        if current_lever_float <= 0: current_lever_float = 1.0

        margin_cost = add_notional / current_lever_float
        
        # CLIENT REQUIREMENT: NO budget check for Auto-Add
        # "All order amount is NOT minus from remaining but from total capital"
        # Orders should place as long as gap condition is met
        # if margin_cost > remaining_budget:
        #      self.log(f"Auto-Add Check SKIPPED: Required Margin ${margin_cost:.2f} > Budget ${remaining_budget:.2f}", level="warning")
        #      return

        # Execute Market Order
        target_side = "Buy" if current_side == 'long' else "Sell"
        
        # Qty = Notional / Price / ContractSize
        contract_size = safe_float(self.product_info.get('contractSize', 1.0))
        if contract_size <= 0: contract_size = 1.0
        
        qty_contracts = add_notional / (current_price * contract_size)
        
        # Format Qty
        qty_precision = safe_int(self.product_info.get('lotSzPrecision', '0'))
        is_integer_qty = self.product_info.get('lotSz', '1') == '1' and '.' not in self.product_info.get('lotSz', '1')
        
        if is_integer_qty:
            qty_str = str(max(1, int(qty_contracts)))
        else:
            min_sz = safe_float(self.product_info.get('minSz', 0.001))
            qty_str = f"{max(min_sz, qty_contracts):.{qty_precision}f}"
            
        self.log(f"Auto-Add Check Triggering Gap Add (Step {step_count}). Gap: {gap_magnitude:.2f} > {gap_threshold}. Cost: ${margin_cost:.2f} (Notional: ${add_notional:.2f}, Qty: {qty_str})", level="warning")
        
        order_response = self._okx_place_order(
            self.config['symbol'],
            target_side,
            safe_float(qty_str),
            order_type="Market",
            verbose=True
        )

        if order_response and order_response.get('ordId'):
            self.log(f"[OK] Auto-Add Step {step_count} Executed. Order: {order_response.get('ordId')}", level="info")
            # Update State
            self.last_add_price = current_price
            self.cumulative_margin_used += margin_cost # Track Margin (session-only)
            self.auto_add_step_count += 1
            
            # Cooldown
            time.sleep(1)
            
            # Step 2: Ensure Exit Orders are updated immediately
            threading.Thread(target=self._update_exit_orders, args=(current_side,), daemon=True).start()
        else:
            self.log(f"[FAIL] Auto-Add Step {step_count} Failed: {order_response}", level="error")

    def _update_exit_orders(self, side):
        """
        Step 2 of Auto-Add: Calculate new avg price and place Limit Exit (TP).
        """
        try:
            time.sleep(2) # Wait for fill
            
            # 1. Fetch latest position data
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)
             
            if response and response.get('code') == '0':
                positions_data = response.get('data', [])
                target_pos = None
                for pos in positions_data:
                    pos_side = pos.get('posSide', 'net')
                    if side == 'long' and (pos_side == 'long' or (pos_side == 'net' and float(pos['pos']) > 0)):
                        target_pos = pos
                        break
                    elif side == 'short' and (pos_side == 'short' or (pos_side == 'net' and float(pos['pos']) < 0)):
                        target_pos = pos
                        break
                
                if target_pos:
                    avg_px = safe_float(target_pos.get('avgPx'))
                    # Calculate TP Price
                    # Mode 1: Break Even (AvP + Fee/Size?) -> Just AvP for now or small profit
                    # Mode 2: Profit Multiplier
                    
                    tp_price = 0.0
                    if side == 'long':
                        step2_offset = safe_float(self.config.get('add_pos_step2_offset', 0.0))
                        
                        if step2_offset > 0:
                            tp_price = avg_px + step2_offset
                        else:
                            # Mode 2: Profit Multiplier (Improved with fee accounting)
                            profit_mult = self.config.get('add_pos_profit_multiplier', 1.5)
                            trade_fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                            
                            # Price = EntryPrice * (1 + Fee) / (1 - Fee * (1 + Multiplier))
                            # This ensures we cover entry fee, exit fee, and hit profit target
                            denom = 1 - (trade_fee_pct * (1 + profit_mult))
                            if denom > 0:
                                tp_price = avg_px * (1 + trade_fee_pct) / denom
                            else:
                                tp_price = avg_px * 1.005 # Fallback to 0.5% profit
                        
                    else: # Short
                        step2_offset = safe_float(self.config.get('add_pos_step2_offset', 0.0))
                        
                        if step2_offset > 0:
                            tp_price = avg_px - step2_offset
                        else:
                            profit_mult = self.config.get('add_pos_profit_multiplier', 1.5)
                            trade_fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                            
                            # Price = EntryPrice * (1 - Fee) / (1 + Fee * (1 + Multiplier))
                            denom = 1 + (trade_fee_pct * (1 + profit_mult))
                            tp_price = avg_px * (1 - trade_fee_pct) / denom

                # Sanity check
                if tp_price > 0:
                    self.log(f"=== AUTO-ADD STEP 2 (CLOSE) ===", level="info")
                    if safe_float(self.config.get('add_pos_step2_offset', 0.0)) > 0:
                         self.log(f"Logic used: Fixed Offset (${safe_float(self.config.get('add_pos_step2_offset', 0.0))})", level="info")
                    else:
                         self.log(f"Logic used: Profit Multiplier ({self.config.get('add_pos_profit_multiplier', 1.5)}x Fees)", level="info")
                    
                    self.log(f"New Avg Entry: {avg_px} -> Setting Limit Exit at {tp_price:.4f}", level="info")
                    
                    # Sync internal state so closure detection knows this is a Mode 2 exit
                    with self.position_lock:
                        self.current_take_profit[side] = tp_price

                    # Place Limit Close
                    close_side = "Sell" if side == 'long' else "Buy"
                    qty = abs(safe_float(target_pos.get('pos')))
                    
                    # Reduce Only to strictly close
                    self._okx_place_order(
                        self.config['symbol'],
                        close_side,
                        qty,
                        price=tp_price,
                        order_type="Limit",
                        reduce_only=True,
                        verbose=True
                    )
                else:
                     self.log(f"Auto-Add Check Calculated TP Price Invalid: {tp_price}", level="error")

        except Exception as e:
            self.log(f"Error in _update_exit_orders: {e}", level="error")

    def _cancel_all_exit_orders_and_reset(self, reason, side=None):
        # Determine sides to reset
        sides_to_reset = [side] if side else ['long', 'short']
        
        with self.position_lock:
            for s in sides_to_reset:
                orders_to_cancel = list(self.position_exit_orders[s].values())

                self.in_position[s] = False
                self.position_entry_price[s] = 0.0
                self.position_qty[s] = 0.0
                self.current_take_profit[s] = 0.0
                self.current_stop_loss[s] = 0.0
                self.position_exit_orders[s] = {}
                self.entry_reduced_tp_flag[s] = False

                for order_id in orders_to_cancel:
                    if order_id:
                        try:
                            # Note: Usually already cancelled by execute_trade_exit, but safe to retry
                            self._okx_cancel_algo_order(self.config['symbol'], order_id)
                        except: pass

        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None

        self.log("=" * 80, level="info")
        self.log(f"STATE RESET [{side.upper() if side else 'ALL'}] - Reason: {reason}", level="info")
        self.log("=" * 80, level="info")
    def _check_and_close_any_open_position(self):
        try:
            self.log("Checking for any open OKX positions to close...", level="debug")
            path = "/api/v5/account/positions"
            # BROADEN: Remove instId filter to be more robust against exchange-side filtering issues
            params = {"instType": "SWAP"}
            response = self._okx_request("GET", path, params=params)

            any_closed = False
            if response and response.get('code') == '0':
                positions = response.get('data', [])
                target_symbol = self.config['symbol'].strip().upper()
                
                # Trace log all symbols found if target is missing
                found_symbols = [p.get('instId') for p in positions]
                self.log(f"Position check found symbols: {found_symbols}", level="debug")

                for pos in positions:
                    pos_inst_id = pos.get('instId', '').strip().upper()
                    if pos_inst_id == target_symbol:
                        size_rv = safe_float(pos.get('pos', 0))
                        if abs(size_rv) > 0:
                            # Detect posSide and margin mode. TRUST THE EXCHANGE DATA.
                            pos_side = pos.get('posSide')
                            if not pos_side:
                                pos_side = 'net'
                                
                            mgn_mode = pos.get('mgnMode')

                            self.log(f"⚠️ Found open {pos_side} position: {size_rv} {self.config['symbol']} (Mode: {mgn_mode})", level="warning")
                            
                            # If size_rv is negative (short), we must BUY to close. This applies to Net mode too (negative size = short).
                            close_side = "Buy" if size_rv < 0 else "Sell"
                            
                            self.log(f"Closing {abs(size_rv)} {self.config['symbol']} with market {close_side} order (posSide: {pos_side})", level="info")
                            # Use explicit tdMode and posSide from the position data
                            close_order = self._okx_place_order(self.config['symbol'], close_side, abs(size_rv), order_type="Market", reduce_only=True, posSide=pos_side, tdMode=mgn_mode)
                            if close_order and close_order.get('ordId'):
                                self.log(f"[OK] Position close order placed: {close_order.get('ordId')}", level="info")
                                self.log(f"[DONE] Close Position (Manual): {pos_side.upper()} {self.config['symbol']} | Qty: {abs(size_rv)}", level="info")
                                any_closed = True
                            else:
                                self.log(f"❌ Failed to place close order for {pos_side} position", level="error")

            if not any_closed:
                self.log("No open OKX positions found to close.", level="info")
            return any_closed
        except Exception as e:
            self.log(f"Exception in _check_and_close_any_open_position (OKX): {e}", level="error")
            return False

    def _reset_entry_state(self, reason):
        with self.position_lock:
            self.pending_entry_order_id = None
            self.entry_reduced_tp_flag = False
            self.pending_entry_order_details = {}
        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None
        self.log(f"Entry state reset. Reason: {reason}", level="info")


        self.log("=" * 80, level="info")
        self.log(f"POSITION CLOSED - Reason: {reason}", level="info")
        self.log("=" * 80, level="info")

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    self._okx_cancel_algo_order(self.config['symbol'], order_id)
                    time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error cancelling order: {e} (OK, continuing)", level="error")

        # Account information is no longer updated in real-time via private WebSocket.

    def _get_latest_data_and_indicators(self):
        try:
            with self.trade_data_lock: # Use trade_data_lock for latest_trade_price
                current_price = self.latest_trade_price
                if current_price is None:
                    if self.is_running:
                        self.log(f"Could not get current market price from WebSocket. Waiting for data.", level="warning")
                    return None
                
                # Check price age for logging/diagnostics
                price_age = time.time() - self.last_price_update_time
                if price_age > 1.0:
                    self.log(f"Price data is {price_age:.1f}s old. Checking connection...", level="debug")

            return {
                'current_price': current_price,
                'price_age': price_age
            }

        except Exception as e:
            self.log(f"Exception in _get_latest_data_and_indicators: {e}", level="error")
            return None

    def _check_candlestick_conditions(self, market_data):
        # Fetch the latest completed candle for the primary timeframe (e.g., '1m')
        # This assumes you have historical data being updated.
        timeframe = self.config.get('candlestick_timeframe', '1m')
        with self.data_lock:
            df = self.historical_data_store.get(timeframe)
            if df is None or df.empty:
                self.log(f"No historical data for {timeframe} to check candlestick conditions.", "warning")
                return True, "No Data (Default Pass)" # Default to true if data is not available to not block trades

            latest_candle = df.iloc[-1]
            o = latest_candle['Open']
            h = latest_candle['High']
            l = latest_candle['Low']
            c = latest_candle['Close']

        status_parts = []
        
        # Check Open-Close Change
        oc_pass = True
        if self.config.get('use_chg_open_close'):
            chg_open_close = abs(o - c)
            min_chg = self.config.get('min_chg_open_close', 0)
            max_chg = self.config.get('max_chg_open_close', 0)
            if not (min_chg <= chg_open_close <= max_chg):
                 oc_pass = False
            status_parts.append(f"open-close={'Passed' if oc_pass else 'Fail'}")

        # Check High-Low Change
        hl_pass = True
        if self.config.get('use_chg_high_low'):
            chg_high_low = h - l
            min_chg = self.config.get('min_chg_high_low', 0)
            max_chg = self.config.get('max_chg_high_low', 0)
            if not (min_chg <= chg_high_low <= max_chg):
                hl_pass = False
            status_parts.append(f"High-Low={'Passed' if hl_pass else 'Fail'}")

        # Check High-Close Change
        hc_pass = True
        if self.config.get('use_chg_high_close'):
            chg_high_close = abs(h - c)
            min_chg = self.config.get('min_chg_high_close', 0)
            max_chg = self.config.get('max_chg_high_close', 0)
            if not (min_chg <= chg_high_close <= max_chg):
                hc_pass = False
            status_parts.append(f"High-Close={'Passed' if hc_pass else 'Fail'}")

        all_passed = oc_pass and hl_pass and hc_pass
        status_str = "; ".join(status_parts) if status_parts else "Skipped"
        
        return all_passed, status_str

    def _okx_adjust_margin(self, symbol, posSide, amount, type='add'):
        """
        Adjust margin for isolated position.
        """
        path = "/api/v5/account/adj-margin"
        params = {
            "instId": symbol,
            "posSide": posSide,
            "type": type,
            "amt": str(amount)
        }
        res = self._okx_request("POST", path, body=params)
        if res and res.get('code') == '0':
            self.log(f"Successfully {type}ed {amount} margin to {posSide} {symbol}", level="info")
            return True
        else:
            self.log(f"Failed to move margin: {res}", level="error")
            return False

    def _check_entry_conditions(self, market_data, log_prefix=""):
        # Max Amount = Max Allowed Used (USDT)
        # Remaining = (Max Amount * Leverage) - Used Notional
        leverage = float(self.config.get('leverage', 1))
        if leverage <= 0: leverage = 1.0
        
        # Safety Clamp: max_allowed_used must be capped by total_equity (Total Capital)
        max_allowed_config = float(self.config.get('max_allowed_used', 1000.0))
        with self.account_info_lock:
            equity = self.total_equity
        
        max_amount_usdt = max_allowed_config
        if equity > 0 and max_allowed_config > equity:
            max_amount_usdt = equity
            if not getattr(self, '_max_allowed_clamped_logged', False):
                self.log(f"Safety Clamp: Max Allowed Used (${max_allowed_config:.2f}) capped by Total Capital (${equity:.2f})", level="warning")
                self._max_allowed_clamped_logged = True
        elif equity > 0 and max_allowed_config <= equity:
            self._max_allowed_clamped_logged = False

        rate_divisor = self.config.get('rate_divisor', 1)
        if rate_divisor <= 0: rate_divisor = 1
        max_amount_per_loop = max_amount_usdt / rate_divisor
        max_notional_capacity = max_amount_per_loop * leverage
        
        min_notional_per_order = self.config.get('min_order_amount', 100)
        
        with self.position_lock:
            # High-Precision Remaining Calculation
            remaining_notional = max_notional_capacity - self.used_amount_notional

        target_amount = self.config.get('target_order_amount', 100)

        # User is responsible for setting Max Allowed within their balance limits
        # Bot focuses only on remaining capacity
        current_price = market_data['current_price']
        direction_mode = self.config.get('direction', 'long')
        long_safety = self.config.get('long_safety_line_price', 0)
        short_safety = self.config.get('short_safety_line_price', float('inf'))
        entry_price_offset = self.config.get('entry_price_offset', 0)

        valid_entries = []
        
        # Possible directions to check
        directions_to_eval = []
        if direction_mode == 'both':
            directions_to_eval = ['long', 'short']
        else:
            directions_to_eval = [direction_mode]

        # Shared Candlestick check (if enabled)
        candlestick_passed = True
        candlestick_msg = "Skipped"
        if self.config.get('use_candlestick_conditions', False):
            candlestick_passed, candlestick_msg = self._check_candlestick_conditions(market_data)

        for d in directions_to_eval:
            passed = False
            signal = 0
            safety_p = 0.0
            limit_p = 0.0
            
            if d == 'long':
                safety_p = long_safety
                passed = (current_price < long_safety)
                signal = 1
                limit_p = current_price - entry_price_offset
            else: # short
                safety_p = short_safety
                passed = (current_price > short_safety)
                signal = -1
                limit_p = current_price + entry_price_offset

            self.log(f"{log_prefix}Entry-1:{d.upper()} Market {current_price:.2f}, Safety:{safety_p}, {'Passed' if passed else 'NOT Passed'}", level="info")
            
            if passed:
                if candlestick_passed:
                    valid_entries.append({'signal': signal, 'limit_price': limit_p, 'side': d})
                    if candlestick_msg != "Skipped":
                         self.log(f"{log_prefix}Entry-2:{candlestick_msg}", level="info")
                else:
                    self.log(f"{log_prefix}Entry-2:Candlestick {candlestick_msg}: NOT Passed", level="info")
        
        # Log final verification for consistency if nothing passed
        if not valid_entries:
             return []

        # Check explicit target/min logs for the first valid one to keep user dashboard tidy
        if remaining_notional < target_amount:
            self.log(f"{log_prefix}Entry-3:Remaining: {remaining_notional:.2f} < Target {target_amount}: Passed (Partial)", level="info")
        else:
            self.log(f"{log_prefix}Entry-3:Remaining: {remaining_notional:.2f} >= Target {target_amount}: Passed", level="info")

        if remaining_notional < min_notional_per_order:
             self.log(f"{log_prefix}Entry-4:Remaining: {remaining_notional:.2f} < Min {min_notional_per_order}: NOT Passed", level="info")
             return []
        
        self.log(f"{log_prefix}Entry-4:Remaining: {remaining_notional:.2f} >= Min {min_notional_per_order}: Passed", level="info")

        return valid_entries

    def _initiate_entry_sequence(self, initial_limit_price, signal, batch_size):
        # NOTE: This function places the batch. It does NOT handle the loop logic. 
        # The loop logic is now in _main_trading_logic.
        
        # We perform a double-check on balance but primary check is in _check_entry_conditions
        with self.account_info_lock:
            current_available_balance = self.available_balance

        batch_offset = self.config['batch_offset']
        self.batch_counter += 1
        
        self.log(f"Place Order Batch {self.batch_counter}", level="info")
        
        for i in range(batch_size):
            current_limit_price = initial_limit_price
            if i > 0: 
                if signal == 1: # Long
                    current_limit_price -= (batch_offset * i)
                else: # Short
                    current_limit_price += (batch_offset * i)

            if current_limit_price <= 0:
                continue

            # Recalculate room for EVERY order to be precise (though less critical if Target is small)
            leverage = float(self.config.get('leverage', 1))
            if leverage <= 0: leverage = 1.0
            
            # Safety Clamp: max_allowed_used must be capped by total_equity (Total Capital)
            max_allowed_config = float(self.config.get('max_allowed_used', 1000.0))
            with self.account_info_lock:
                equity = self.total_equity
            
            max_amount_usdt = max_allowed_config
            if equity > 0 and max_allowed_config > equity:
                max_amount_usdt = equity

            rate_divisor = self.config.get('rate_divisor', 1)
            if rate_divisor <= 0: rate_divisor = 1
            max_amount_per_loop = max_amount_usdt / rate_divisor
            max_notional_capacity = max_amount_per_loop * leverage
            
            with self.position_lock:
                remaining_notional = max_notional_capacity - self.used_amount_notional
            
            target_notional = self.config.get('target_order_amount', 100)
            min_notional = self.config.get('min_order_amount', 100)
            
            if remaining_notional < min_notional:
                self.log(f"Batch {self.batch_counter}-{i+1} skipped: Remaining ({remaining_notional:.2f}) < Min ({min_notional})", level="info")
                break
                
            trade_amount_usdt = min(target_notional, remaining_notional)
        
            # Target contracts based on exact trade_amount_usdt (removed 0.5% buffer)
            qty_base_asset = trade_amount_usdt / current_limit_price
            contract_size = safe_float(self.product_info.get('contractSize', 1.0))
            if contract_size <= 0: contract_size = 1.0

            # Use lot size (qtyStepSize) for precise rounding
            lot_size = safe_float(self.product_info.get('qtyStepSize', 1.0))
            if lot_size <= 0: lot_size = 1.0

            qty_contracts = math.floor((qty_base_asset / contract_size) / lot_size) * lot_size
            
            min_order_qty = safe_float(self.product_info.get('minOrderQty', 1.0))
            if qty_contracts < min_order_qty:
                 if (min_order_qty * contract_size * current_limit_price) <= remaining_notional:
                     qty_contracts = min_order_qty
                 else:
                     continue

            qty_precision = self.product_info.get('qtyPrecision', 0)
            qty_contracts = round(qty_contracts, qty_precision)
            
            # Calculate TP/SL for Display
            tp_px = 0.0
            sl_px = 0.0
            tp_offset_val = self.config.get('tp_price_offset', 0)
            sl_offset_val = self.config.get('sl_price_offset', 0)
            
            if signal == 1: # LONG
                if tp_offset_val and safe_float(tp_offset_val) > 0:
                    tp_px = current_limit_price + safe_float(tp_offset_val)
                if sl_offset_val and safe_float(sl_offset_val) > 0:
                    sl_px = current_limit_price - safe_float(sl_offset_val)
            else: # SHORT
                if tp_offset_val and safe_float(tp_offset_val) > 0:
                    tp_px = current_limit_price - safe_float(tp_offset_val)
                if sl_offset_val and safe_float(sl_offset_val) > 0:
                    sl_px = current_limit_price + safe_float(sl_offset_val)

            # Log Format: Batch1-1:M:2980|En:2982|TP:2976|SL:3010|1000|Short|Isolated|20x
            market_p = self.latest_trade_price if self.latest_trade_price else 0.0
            side_str = 'Long' if signal == 1 else 'Short'
            mode_str = self.config.get('mode', 'cross').capitalize()
            # M:{market}|En:{entry}|Tp:{tp}|SL:{sl}|{amt}|{side}|{mode}
            # Note: User requested "Tp" (capital T, lowercase p case matching handwritten note usually has TP or Tp, using Tp as per log request "Tp:2976") 
            log_str = f"Batch{self.batch_counter}-{i+1}:M:{market_p:.2f}|En:{current_limit_price:.2f}|Tp:{tp_px:.2f}|SL:{sl_px:.2f}|{target_notional}|{side_str}|{mode_str}"
            self.log(log_str, level="info")
            
            p_side_entry = "long" if signal == 1 else "short"
            # Pass TP/SL params for atomic placement
            entry_order_response = self._okx_place_order(self.config['symbol'], "Buy" if signal == 1 else "Sell", qty_contracts, price=current_limit_price, order_type="Limit", time_in_force="GoodTillCancel", posSide=p_side_entry, take_profit_price=tp_px, stop_loss_price=sl_px, verbose=False)

            if entry_order_response and entry_order_response.get('ordId'):
                order_id = entry_order_response['ordId']
                with self.position_lock:
                    self.pending_entry_ids.append(order_id)
                    self.pending_entry_order_id = order_id 
                    self.pending_entry_order_details[order_id] = {
                        'order_id': order_id,
                        'side': "Buy" if signal == 1 else "Sell",
                        'qty': qty_contracts * contract_size,
                        'limit_price': current_limit_price,
                        'signal': signal,
                        'order_type': 'Limit',
                        'status': 'New',
                        'placed_at': datetime.now(timezone.utc)
                    }
                
                # Trigger an immediate account info update to refresh values
                def _update_after_close():
                    try:
                        self._sync_account_data()
                        self._emit_socket_updates()
                    except Exception as e:
                        self.log(f"Error in close update: {e}", level="error")
                threading.Thread(target=_update_after_close, daemon=True).start()
                
                # Small delay between batch orders to prevent rate limiting
                if i < batch_size - 1:  # Don't delay after last order
                    time.sleep(0.2)
            else:
                self.log(f"Order placement failed", level="error")

    def _check_cancel_conditions(self):
        # Explicit check for cancel conditions as per nested loop logic
        
        loop_time = self.config.get('loop_time_seconds', 10) # Using existing param or maybe hardcode 90s check?
        # User diagram says: "Check Cancel Condition"
        # 1. More than 90 seconds (cancel_unfilled_seconds)
        # 2. TP < Market (for short) / TP > Market (for long) [Inverted Logic]
         #    User says: "TP < Market" mean TP price is lower than market.
         #    For SHORT: Entry is high. TP is low.
         #    If TP < Market, that's NORMAL for Short.
         #    User Correction: "Correct is tp price below market price but not market below tp"
         #    ... "For short safety line price and market price, bot also do reverse running"
         #    Let's stick to the Text Description in Logic:
         #    "Cancel-2: TP < Market: None"
         #    This implies checking if TP is < Market.
         #    For SHORT: TP < Entry. Market should be near Entry.
         #    If TP < Market (Market is higher than TP), that is normal state (Not reached TP yet).
         #    Maybe user means "Cancel if Market goes *beyond* TP"? i.e. Market < TP?
         #    Wait, "Cancel-2: TP < Market: None". If it was "Yes", it would cancel?
         #    If "TP < Market" is BAD for Short? No, TP < Market is GOOD (we are above TP).
         #    Maybe for LONG? For Long, TP > Entry. Market near Entry.
         #    If Market < TP is normal.
         #    If "TP < Market" (Market > TP). That means we missed it?
         #    Let's look at previous code: "cancel_on_tp_price_below_market".
         #    The standard logic: If Market moves such that the TP is no longer valid or "unfavorable"?
         #    Actually, for pending entry, we don't have a TP yet?
         #    Ah, we calculate "potential TP".
         #    If Potential TP is already "passed" by current market?
         #    Short: Entry=3000, TP=2900. Market=2800. We are already below TP. "Market < TP".
         #    User says "TP < Market". 2900 < 2800? False.
         #    If Market=2950. 2900 < 2950. True.
         #    So for Short, "TP < Market" is the NORMAL state.
         #    If "TP < Market" is the Cancel Condition, then it would always be true?
         #    Unless user means "Market < TP"? (Price dropped below target).
         #    "Correct is tp price below market price but not market below tp"
         #    This implies user WANTS to check "TP < Market".
         #    But if that cancels, it cancels everything normal.
         #    Maybe "TP > Market" for Short? (Price below TP).
         #    Let's assume the user meant "Price passed TP".
         #    Short: Cancel if Market < TP.
         #    Long: Cancel if Market > TP.
         
         #    However, implementing strictly as user described in log:
         #    "Cancel-2: TP<Market"
         #    I will code the log check.

        # self.log("Check Cancel Condition")
        
        # Log condition check (User Request)
        if self.monitoring_tick % 6 == 0:
             self.log("Check Cancel Condition")

        cancel_unfilled_seconds = self.config.get('cancel_unfilled_seconds', 90)
        
        with self.position_lock:
             active_ids = list(self.pending_entry_ids)
             details = dict(self.pending_entry_order_details)

        if not active_ids:
            self.log("No Orders to cancel", level="debug")
            return

        current_market_price = self._get_latest_data_and_indicators().get('current_price')
        if not current_market_price: return

        for order_id in active_ids:
            if order_id not in details: continue
            d = details[order_id]
            
            placed_at = d.get('placed_at')
            signal = d.get('signal') # 1 Long, -1 Short
            limit_price = d.get('limit_price')
            
            # 1. Time Check
            time_passed = False
            if placed_at and (datetime.now(timezone.utc) - placed_at).total_seconds() > cancel_unfilled_seconds:
                time_passed = True
            
            # self.log(f"Cancel-1:More than {cancel_unfilled_seconds} seconds: {'Yes' if time_passed else 'None'}")
            
            if time_passed:
                reason = f"Time Limit ({cancel_unfilled_seconds}s) reached"
                if self._okx_cancel_order(self.config['symbol'], order_id, reason=reason):
                    with self.position_lock:
                        if order_id in self.pending_entry_ids:
                             self.pending_entry_ids.remove(order_id)
                        if order_id in self.pending_entry_order_details:
                             del self.pending_entry_order_details[order_id]
                continue

            # 2. TP Check (Missed Opportunity)
            tp_offset = self.config.get('tp_price_offset', 0)
            is_target_passed = False
            pending_tp = 0.0
            
            if tp_offset and safe_float(tp_offset) > 0:
                if signal == 1: # Long
                    pending_tp = limit_price + tp_offset
                    if current_market_price > pending_tp:
                        is_target_passed = True
                else: # Short
                    pending_tp = limit_price - tp_offset
                    if current_market_price < pending_tp:
                        is_target_passed = True

            # 3. Entry Check (Taker Avoidance / Directional Move)
            is_entry_unfavorable = False
            if signal == 1: # Long
                # Cancel if Entry < Market (Price moved up, making order a taker or too high)
                if current_market_price > limit_price:
                    is_entry_unfavorable = True
            else: # Short
                # Cancel if Entry > Market (Price moved down, making order a taker or too low)
                if current_market_price < limit_price:
                    is_entry_unfavorable = True

            # Execute Cancellation based on priority
            should_cancel = False
            cancel_msg = ""
            
            # Execute Cancellation based on literal config settings (Step 300)
            should_cancel = False
            cancel_msg = ""
            
            if time_passed:
                should_cancel = True
                cancel_msg = f"Time Limit ({cancel_unfilled_seconds}s) reached"
            
            # Short Specific (Literal Checks)
            elif signal == -1: 
                # Cancel if Entry price is below market price (Literal config)
                if self.config.get('cancel_on_entry_price_below_market') and limit_price < current_market_price:
                    should_cancel = True
                    cancel_msg = f"Short: Entry price below market (Entry {limit_price:.2f} < Market {current_market_price:.2f})"
                
                # Cancel if TP price is below market price (Literal config for Missed Opportunity)
                # Cancel if TP price is below market price (Literal config for Missed Opportunity)
                # Client REQUEST: Remove this logic/log as it is confusing.
                # elif self.config.get('cancel_on_tp_price_below_market') and is_target_passed:
                #    should_cancel = True
                #    cancel_msg = f"Short: TP price reached/passed before fill (TP {pending_tp:.2f} > Market {current_market_price:.2f})"
                pass
            
            # Long Specific (Literal Checks)
            elif signal == 1:
                # Cancel if Entry price is above market price
                if self.config.get('cancel_on_entry_price_above_market') and limit_price > current_market_price:
                    should_cancel = True
                    cancel_msg = f"Long: Entry price above market (Entry {limit_price:.2f} > Market {current_market_price:.2f})"
                
                # Cancel if TP price is above market price
                # Cancel if TP price is above market price
                # Client REQUEST: Remove this logic/log as it is confusing.
                # elif self.config.get('cancel_on_tp_price_above_market') and is_target_passed:
                #    should_cancel = True
                #    cancel_msg = f"Long: TP price reached/passed before fill (TP {pending_tp:.2f} < Market {current_market_price:.2f})"
                pass

            if should_cancel:
                # self.log(f"Cancel Order {order_id} ({cancel_msg})") # Already logged in _okx_cancel_order now
                if self._okx_cancel_order(self.config['symbol'], order_id, reason=cancel_msg):
                    with self.position_lock:
                        if order_id in self.pending_entry_ids:
                             self.pending_entry_ids.remove(order_id)
                        if order_id in self.pending_entry_order_details:
                             del self.pending_entry_order_details[order_id]
                continue
                 
         # Clean up local tracking
        with self.position_lock:
             # Basic cleanup of IDs that are gone happens in account update, but we can fast track here if needed
             pass


    def _unified_management_loop(self):
        # High-reliability background management
        self.log("Unified management thread started.", level="debug")
        last_account_sync = 0
        while not self.stop_event.is_set():
            now = time.time()
            try:
                # 1. High Frequency: Cancellation Check (every ~1s)
                # Note: Only cancel if trading is active or we still have tracked pending orders
                self._check_cancel_conditions()

                # 2. PnL-Based Auto-Exit Check
                # Removed: This is now handled authoritatively in _fetch_and_emit_account_info
                # to ensure atomic execution and correct 'Used Amount' calculation.

                
                # 3. Connection Health: Stale Price Monitor
                price_age = now - self.last_price_update_time
                if price_age > 30:
                     self.log(f"WARNING: Market price is STALE ({price_age:.1f}s). Re-initializing WebSocket...", level="warning")
                     # Reset update time to avoid spamming reconnects
                     self.last_price_update_time = now 
                     # Trigger reconnect by closing the current WebSocket
                     if self.ws_public:
                         try:
                             self.ws_public.close()
                         except:
                             pass
                
                # 3. Lower Frequency: Account Info & Emitting (every ~3s)
                # 3. Lower Frequency: Account Info & Emitting (every ~1s for responsiveness)
                if now - last_account_sync >= 1.0:
                    # NEW ARCHITECTURE: Split God Method
                    self._sync_account_data()
                    self._execute_position_management()
                    self._emit_socket_updates()
                    last_account_sync = now
                    
            except Exception as e:
                self.log(f"Error in unified mgmt loop: {e}", level="debug")
            
            time.sleep(1) # Base tick rate
        self.log("Unified management thread stopped.", level="debug")

    def _main_trading_logic(self):
        try:
            self.log("Trading loop started.", level="debug")

            while not self.stop_event.is_set():
                # Reconnection Trigger: Exit if WS is closed/changing
                if not self.ws_public or not getattr(self.ws_public, "sock", None) or not self.ws_public.sock.connected or \
                   not self.ws_private or not getattr(self.ws_private, "sock", None) or not self.ws_private.sock.connected:
                     self.log("WebSocket connection lost or closed. Exiting trading loop for reconnect.", level="debug")
                     return

                if not self.is_running:
                    time.sleep(1)
                    continue

                # 1. Entry Loop
                while self.is_running and not self.stop_event.is_set():
                    self.log("-" * 90)
                    self.log("-" * 90)
                    self.log("Check Entry Condition")
                    market_data = self._get_latest_data_and_indicators()
                    if not market_data:
                        self.log("No market data", level="warning")
                        time.sleep(5)
                        continue
                        
                    valid_signals = self._check_entry_conditions(market_data)
                    
                    if valid_signals:
                        # Process all valid signals (e.g. could be both Long and Short)
                        for entry_info in valid_signals:
                             self._initiate_entry_sequence(entry_info['limit_price'], entry_info['signal'], self.config['batch_size_per_loop'])
                        
                        # Wait Loop Time
                        loop_time = self.config.get('loop_time_seconds', 10)
                        self.log(f"Wait {loop_time} seconds (Post-Entry)")
                        time.sleep(loop_time)
                    else:
                        self.log("Stop Orders (No passing signals in this cycle)")
                        break
                
                # 2. Cancel Check - Now handled by background thread
                # NO-OP here to prevent blocking main loop
                pass
                
                # 3. Delay before restarting cycle
                # Use standard loop_time for consistent heartbeat
                loop_time = self.config.get('loop_time_seconds', 10)
                self.log(f"Wait {loop_time} seconds before meta-loop restart")
                time.sleep(loop_time)

        except Exception as e:
            self.log(f"CRITICAL ERROR in _main_trading_logic: {e}", level="error")

        except Exception as e:
            self.log(f"CRITICAL ERROR in _main_trading_logic: {e}", level="error")

    def _initialize_websocket_and_start_main_loop(self):
        self.log("OKX BOT STARTING", level="info")
        try:
            while not self.stop_event.is_set():
                try:
                    # Reset status
                    self.ws_subscriptions_ready.clear()
                    self.pending_subscriptions.clear()
                    self.confirmed_subscriptions.clear()

                    url_public = self._get_ws_url("public")
                    url_private = self._get_ws_url("private")

                    self.ws_public = websocket.WebSocketApp(
                        url_public,
                        on_open=self._on_websocket_open,
                        on_message=self._on_websocket_message,
                        on_error=self._on_websocket_error,
                        on_close=self._on_websocket_close
                    )
                    self.ws_private = websocket.WebSocketApp(
                        url_private,
                        on_open=self._on_websocket_open,
                        on_message=self._on_websocket_message,
                        on_error=self._on_websocket_error,
                        on_close=self._on_websocket_close
                    )

                    t_pub = threading.Thread(target=self.ws_public.run_forever, daemon=True)
                    t_priv = threading.Thread(target=self.ws_private.run_forever, daemon=True)
                    
                    t_pub.start()
                    t_priv.start()

                    self.log("WebSocket connections (Public & Private) initiated.", level="debug")

                    self.log("Syncing with market data...", level="debug")
                    if not self.ws_subscriptions_ready.wait(timeout=30):
                        self.log("WebSocket subscriptions not ready within timeout. Reconnecting...", level="error")
                        try: self.ws_public.close()
                        except: pass
                        try: self.ws_private.close()
                        except: pass
                        time.sleep(5)
                        continue
                    timeframe = self.config.get('candlestick_timeframe', '1m')
                    interval_sec = self.intervals.get(timeframe, 60)
                    start_dt = datetime.now(timezone.utc) - timedelta(seconds=interval_sec * 300)
                    end_dt = datetime.now(timezone.utc)
                    self._fetch_initial_historical_data(self.config['symbol'], timeframe, start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
                    
                    self.bot_startup_complete = True
                    self.log("Bot startup sequence complete.", level="info")

                    # Perform initial account fetch
                    self._periodic_account_info_update(initial_fetch=True)
                    self.log("Initial account balance fetched.", level="info")
        
                    # Start background managers if not already running
                    if not getattr(self, 'account_info_updater_thread', None) or not self.account_info_updater_thread.is_alive():
                        self.account_info_updater_thread = threading.Thread(target=self._periodic_account_info_update, args=(False,), daemon=True)
                        self.account_info_updater_thread.start()
                    
                    if not getattr(self, 'mgmt_thread', None) or not self.mgmt_thread.is_alive():
                        self.mgmt_thread = threading.Thread(target=self._unified_management_loop, daemon=True)
                        self.mgmt_thread.start()

                    # Start trading logic
                    # This method now needs to respond to stop_event and WS closure
                    self._main_trading_logic()
                    
                    # If _main_trading_logic returns, check if we need to reconnect or stop
                    if self.stop_event.is_set():
                        break
                    
                    self.log("Main trading logic returned. Reconnecting WebSocket in 5s...", level="info")
                    time.sleep(5)

                except Exception as loop_e:
                    self.log(f"Error in WebSocket Reconnect Loop: {loop_e}", level="error")
                    time.sleep(5)

        except Exception as e:
            self.log(f"CRITICAL ERROR in _initialize_websocket_and_start_main_loop: {e}", level="error")
        finally:
            self.stop_event.set()
            self.log("Shutting down...", level="info")
            if self.ws_public or self.ws_private:
                try:
                    try: self.ws_public.close()
                    except: pass
                    try: self.ws_private.close()
                    except: pass
                except Exception:
                    pass
            self.log("OKX BOT SHUTDOWN COMPLETE", level="info")
 
    def _calculate_net_profit_from_fills(self):
        # Fetch recent fills to calculate actual PnL
        try:
            params = {
                "instType": "SWAP",
                "instId": self.config['symbol'],
                "limit": "100"
            }
            # Use /trade/fills for recent activity (last 3 days)
            path_recent = "/api/v5/trade/fills"
            response = self._okx_request("GET", path_recent, params=params)
            
            # Local session PnL (resets every start)
            session_pnl = 0.0
            
            if response and response.get('code') == '0':
                fills = response.get('data', [])
                
                # Fetch only fills from current session for 'self.net_profit' (Auto-Exit trigger)
                # Loosen by 5 seconds to capture trades closed right at bot start/restart
                start_time_limit = self.bot_start_time - 5000

                # Reset persistent trade analytics before re-calculating from the limit window (Simple approach)
                # Note: In a production bot, we'd append new fills to a database.
                # Here we strictly scan the last 100 fills to determine Win/Loss/Net for the symbol.
                temp_total_profit = 0.0
                temp_total_loss = 0.0
                fill_count = 0
                
                for fill in fills:
                     fill_ts = int(fill.get('ts', 0))
                     if fill_ts >= start_time_limit:
                         fill_count += 1
                         pnl = safe_float(fill.get('pnl', 0))
                         fee = safe_float(fill.get('fee', 0))
                         fill_net = pnl + fee
                         session_pnl += fill_net

                         # Realized Analytics: Only count fills where a position was actually reduced or closed (pnl != 0)
                         # This prevents entry fees from showing up as "Trade Loss" before any trades are closed.
                         if pnl != 0:
                             if fill_net > 0:
                                 temp_total_profit += fill_net
                             else:
                                 temp_total_loss += abs(fill_net)
                
                if fill_count > 0:
                    self.log(f"DEBUG: Processed {fill_count} session fills. Temp Net: {temp_total_profit - temp_total_loss:.2f}", level="debug")

                self.total_trade_profit = temp_total_profit
                self.total_trade_loss = temp_total_loss
                self.net_trade_profit = temp_total_profit - temp_total_loss
                # self.net_profit = session_pnl # REMOVED: User wants Net Profit to be UPL for open positions only
                
                self._save_analytics()
            return session_pnl

        except Exception as e:
            self.log(f"Exception in _calculate_net_profit_from_fills: {e}", level="error")
            return 0.0

    def _load_analytics(self):
        try:
            import os
            import json
            if os.path.exists(self.analytics_path):
                with open(self.analytics_path, 'r') as f:
                    data = json.load(f)
                    self.daily_reports = data.get('daily_reports', [])
            else:
                self.daily_reports = []
        except Exception as e:
            self.log(f"Error loading analytics: {e}", level="error")

    def _save_analytics(self):
        try:
            import json
            data = {
                'daily_reports': self.daily_reports
            }
            with open(self.analytics_path, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            self.log(f"Error saving analytics: {e}", level="error")

    def _check_and_save_daily_report(self):
        """Snapshots daily performance at UTC midnight."""
        now = datetime.now(timezone.utc)
        today_str = now.strftime('%Y-%m-%d')
        
        # Check if already saved for today
        if self.daily_reports and self.daily_reports[-1].get('date') == today_str:
            return

        # Prepare new report
        prev_capital = self.daily_reports[-1].get('total_capital', self.total_equity) if self.daily_reports else self.total_equity
        compound_interest = (self.total_equity / prev_capital) if prev_capital > 0 else 1.0

        report = {
            'date': today_str,
            'total_capital': self.total_equity,
            'net_trade_profit': self.net_trade_profit,
            'compound_interest': round(compound_interest, 4)
        }
        
        self.daily_reports.append(report)
        self.log(f"📅 Daily Report Saved for {today_str}: Capital ${self.total_equity:.2f}, Net Profit ${self.net_trade_profit:.2f}", level="info")
        self._save_analytics()

    def _periodic_account_info_update(self, initial_fetch=False):
        if initial_fetch:
            # Perform a single fetch and return
            self._sync_account_data()
            self._execute_position_management()
            self._emit_socket_updates()
            return

        while not self.stop_event.is_set():
            try:
                # Regular sync (dashboard heartbeat)
                self._sync_account_data()
                self._execute_position_management()
                self._emit_socket_updates()
            except Exception as e:
                self.log(f"Error in periodic account info update: {e}", level="error")
            finally:
                time.sleep(self.config.get('account_update_interval_seconds', 10))


    def _process_account_positions(self, positions_data, is_snapshot=True):
        """
        Handles position updates from both REST (snapshot) and WS (incremental).
        """
        temp_active_count = 0
        temp_pos_notional = 0.0
        temp_unrealized_pnl = 0.0
        temp_used_notional = 0.0

        with self.position_lock:
            prev_qtys = {k: v for k, v in self.position_qty.items()}
            target_symbol = self.config['symbol'].strip().upper()
            found_sides = set()
            contract_size = self.product_info.get('contractSize', 1.0)
            if contract_size <= 0: contract_size = 1.0

            for pos in positions_data:
                # BROADEN: Case-insensitive stripped match
                pos_inst_id = pos.get('instId', '').strip().upper()
                if pos_inst_id == target_symbol:
                    qty_raw = safe_float(pos.get('pos'))
                    if qty_raw == 0 and is_snapshot:
                        continue
                        
                    current_mkt_price = self.latest_trade_price if self.latest_trade_price else safe_float(pos.get('avgPx'))
                    pos_sz_notional = abs(qty_raw) * current_mkt_price * contract_size
                    
                    raw_side = pos.get('posSide', 'net')
                    side_key = 'long'
                    if raw_side == 'short': side_key = 'short'
                    elif raw_side == 'net':
                        side_key = self.config.get('direction', 'long')
                        if side_key == 'both': side_key = 'long'

                    if qty_raw != 0:
                        found_sides.add(side_key)
                        # Used notional for capacity checking only active when bot is running
                        if self.is_running:
                            session_qty = max(0, abs(qty_raw * contract_size) - self.session_baseline_qty.get(side_key, 0.0))
                            temp_used_notional += session_qty * current_mkt_price
                        temp_pos_notional += pos_sz_notional
                        temp_unrealized_pnl += safe_float(pos.get('upl', '0'))
                        temp_active_count += 1
                        new_qty = qty_raw * contract_size
                        
                        if abs(new_qty - prev_qtys.get(side_key, 0.0)) > 0.000001:
                            self.log(f"Position update [{side_key.upper()}]: {prev_qtys.get(side_key, 0.0)} -> {new_qty}. Syncing TP/SL...", level="debug")
                            self._should_update_tpsl = True
                            if abs(new_qty) > abs(prev_qtys.get(side_key, 0.0)):
                                self.total_trades_count += 1
                        
                        if self.current_take_profit[side_key] == 0 or self.current_stop_loss[side_key] == 0:
                             self._should_update_tpsl = True

                        self.in_position[side_key] = True
                        self.position_entry_price[side_key] = safe_float(pos.get('avgPx'))
                        # Sync baseline price for gap calculations if entry price changed significantly (Manual or Auto add)
                        if abs(self.position_entry_price[side_key] - prev_qtys.get(side_key, 0.0)) > 0.000001:
                            self.last_add_price = self.position_entry_price[side_key]
                        self.position_qty[side_key] = new_qty
                        self.position_liq[side_key] = safe_float(pos.get('liqp', '0'))
                        self.position_details[side_key] = pos
                    else:
                        # Handle closure via WS pos="0"
                        if side_key in found_sides: found_sides.remove(side_key)

            # Close detection logic
            for s in ['long', 'short']:
                # If snapshot, treat missing as closed. If incremental, only if we saw pos="0" (implicit in list loop above)
                should_close = False
                if is_snapshot:
                    if s not in found_sides and self.in_position[s]:
                        should_close = True
                else:
                    # In WS incremental, we look for explicit pos="0" in the data provided for this side
                    for pos in positions_data:
                        raw_side = pos.get('posSide', 'net')
                        side_msg = 'short' if raw_side == 'short' else ('long' if raw_side == 'long' or raw_side == 'net' else '')
                        if side_msg == s and safe_float(pos.get('pos')) == 0:
                            should_close = True
                            break

                if should_close:
                    close_reason = "Exchange Hit (TP/SL/Manual)"
                    if self.authoritative_exit_in_progress:
                        close_reason = "Authoritative Exit (Bot Target/Emergency)"
                    else:
                        mkt = self.latest_trade_price
                        tp = self.current_take_profit[s]
                        sl = self.current_stop_loss[s]
                        if tp > 0 and abs(mkt - tp) / tp < 0.001: 
                            if self.config.get('use_add_pos_profit_target', False):
                                close_reason = "Mode 2 Profit Target Reached (Auto-Add Step 2)"
                            else:
                                close_reason = "Exchange Hit (Take Profit)"
                        elif sl > 0 and abs(mkt - sl) / sl < 0.001: 
                            close_reason = "Exchange Hit (Stop Loss)"
                    
                    self.log("=" * 60, level="info")
                    self.log(f"[DONE] Close Position: {s.upper()} {self.config['symbol']} | Reason: {close_reason}", level="info")
                    self.log("=" * 60, level="info")
                    
                    self.in_position[s] = False
                    self.position_entry_price[s] = 0.0
                    self.position_qty[s] = 0.0
                    self.position_liq[s] = 0.0
                    self.position_details[s] = {}
                    self.current_take_profit[s] = 0.0
                    self.current_stop_loss[s] = 0.0
                    self._should_update_tpsl = True

            # Emit combined position update
            display_side = 'long' if self.in_position['long'] else ('short' if self.in_position['short'] else 'long')
            self.emit('position_update', {
                'in_position': self.in_position[display_side],
                'position_entry_price': self.position_entry_price[display_side],
                'position_qty': self.position_qty[display_side],
                'current_take_profit': self.current_take_profit[display_side],
                'current_stop_loss': self.current_stop_loss[display_side],
                'positions': {
                    'long': {
                        'in': self.in_position['long'],
                        'qty': self.position_qty['long'],
                        'price': self.position_entry_price['long'],
                        'liq': self.position_liq['long'],
                        'sl': self.current_stop_loss['long']
                    },
                    'short': {
                        'in': self.in_position['short'],
                        'qty': self.position_qty['short'],
                        'price': self.position_entry_price['short'],
                        'liq': self.position_liq['short'],
                        'sl': self.current_stop_loss['short']
                    }
                }
            })

            # Update cached metrics (even for WS updates for better responsiveness)
            self.cached_active_positions_count = temp_active_count
            self.cached_pos_notional = temp_pos_notional
            self.cached_unrealized_pnl = temp_unrealized_pnl
            self.cached_used_notional = temp_used_notional

    def _sync_account_data(self):
        """
        Phase 1 of Unified Loop: Read-Only Data Sync.
        Fetches Balance, Pending Orders, and Positions.
        Updates internal state via API but triggers NO trades.
        """
        self.monitoring_tick += 1
        
        # 1. Fetch account balance
        path_balance = "/api/v5/account/balance"
        params_balance = {"ccy": "USDT"} 
        response_balance = self._okx_request("GET", path_balance, params=params_balance)

        with self.account_info_lock:
            found_total_eq = 0.0
            found_avail_bal = 0.0
            found_bal = 0.0
            if response_balance and response_balance.get('code') == '0':
                data = response_balance.get('data', [])
                if data and len(data) > 0:
                    account_details = data[0]
                    found_total_eq = safe_float(account_details.get('totalEq', '0'))
                    for detail in account_details.get('details', []):
                        if detail.get('ccy') == 'USDT':
                            found_bal = safe_float(detail.get('bal', '0'))
                            found_avail_bal = safe_float(detail.get('availBal', '0'))
                            break
            
            self.account_balance = found_bal 
            self.available_balance = found_avail_bal
            self.total_balance = found_bal
            self.total_equity = found_total_eq
            self.effective_wallet_balance = found_total_eq - getattr(self, 'cached_unrealized_pnl', 0.0)
            self.log(f"Account sync: total_equity={self.total_equity}, total_balance={self.total_balance}, avail_bal={self.available_balance}", level="debug")

        # 2. Fetch open orders (pending orders)
        path_pending_orders = "/api/v5/trade/orders-pending"
        params_pending_orders = {"instType": "SWAP", "instId": self.config['symbol']}
        response_pending_orders = self._okx_request("GET", path_pending_orders, params=params_pending_orders)
        
        formatted_open_trades = []
        if response_pending_orders and response_pending_orders.get('code') == '0':
            pending_orders = response_pending_orders.get('data', [])
            contract_size = self.product_info.get('contractSize', 1.0)
            if contract_size <= 0: contract_size = 1.0

            for order in pending_orders:
                ord_id = order.get('ordId') or order.get('algoId')
                
                # Exclude Reduce-Only orders (Exits) from being adopted as Pending Entries
                if order.get('reduceOnly') == 'true':
                    continue
                
                # Adoption Logic
                with self.position_lock:
                    if ord_id not in self.pending_entry_ids:
                        self.pending_entry_ids.append(ord_id)
                        c_time_ms = int(order.get('cTime', time.time() * 1000))
                        placed_at_dt = datetime.fromtimestamp(c_time_ms / 1000.0, tz=timezone.utc)
                        self.pending_entry_order_details[ord_id] = {
                            'order_id': ord_id,
                            'side': order.get('side').capitalize(),
                            'qty': safe_float(order.get('sz')) * contract_size,
                            'limit_price': safe_float(order.get('px')),
                            'signal': 1 if order.get('side') == 'buy' else -1,
                            'order_type': order.get('ordType', 'Limit'),
                            'status': order.get('state'),
                            'placed_at': placed_at_dt
                        }

                # Time left logic
                time_left = None
                cancel_unfilled_seconds = self.config.get('cancel_unfilled_seconds', 90)
                current_placed_at = None
                with self.position_lock:
                    if ord_id in self.pending_entry_order_details:
                         current_placed_at = self.pending_entry_order_details[ord_id].get('placed_at')

                if current_placed_at:
                    seconds_passed = (datetime.now(timezone.utc) - current_placed_at).total_seconds()
                    time_left = max(0, int(cancel_unfilled_seconds - seconds_passed))

                formatted_open_trades.append({
                    'type': order.get('side').capitalize(),
                    'id': ord_id,
                    'entry_spot_price': safe_float(order.get('px')),
                    'stake': safe_float(order.get('sz')) * safe_float(order.get('px')) * contract_size,
                    'tp_price': None,
                    'sl_price': None,
                    'status': order.get('state'),
                    'instId': order.get('instId'),
                    'time_left': time_left
                })
        
        with self.trade_data_lock:
            self.open_trades = formatted_open_trades
            
        # 3. Fetch open positions (Snapshot)
        path_positions = "/api/v5/account/positions"
        # BROADEN: Remove instId filter
        params_positions = {"instType": "SWAP"}
        response_positions = self._okx_request("GET", path_positions, params=params_positions)

        if response_positions and response_positions.get('code') == '0':
            self._process_account_positions(response_positions.get('data', []), is_snapshot=True)

        # Sync pending_entry_ids
        active_okx_ids = [t['id'] for t in formatted_open_trades]
        with self.position_lock:
            existing_pending = list(self.pending_entry_ids)
            for p_id in existing_pending:
                if p_id not in active_okx_ids:
                    self.pending_entry_ids.remove(p_id)
                    if p_id in self.pending_entry_order_details:
                        del self.pending_entry_order_details[p_id]
                    self.log(f"Pending order {p_id} cleared from tracking.", level="debug")
                    self._should_update_tpsl = True

        if getattr(self, '_should_update_tpsl', False) and any(self.in_position.values()) and self.is_running:
            self._should_update_tpsl = False
            # Call TP/SL modification to sync with new average price
            threading.Thread(target=self.batch_modify_tpsl, daemon=True).start()
        
        # Calculate Need Add metrics (Viz) - Moved here to ensure update during UI-only loops
        self._calculate_need_add_metrics(getattr(self, 'cached_pos_notional', 0.0))

    def _execute_position_management(self):
        """
        Phase 2 of Unified Loop: Trading Logic & Metrics.
        Calculates Margins, PnL, and triggers Auto-Add/Auto-Exit.
        CONTAINS CRITICAL FIX FOR AUTO-ADD GATING.
        """
        # Recover metrics from Sync Phase
        used_amount_notional = getattr(self, 'cached_used_notional', 0.0)
        okx_pos_notional = getattr(self, 'cached_pos_notional', 0.0)
        total_unrealized_pnl = getattr(self, 'cached_unrealized_pnl', 0.0)
        active_positions_count = getattr(self, 'cached_active_positions_count', 0)
        
        # Add pending orders to Used Amount
        with self.trade_data_lock:
            for trade in self.open_trades:
                 used_amount_notional += trade['stake']

        # Metric Calculations
        max_allowed_config = float(self.config.get('max_allowed_used', 1000.0))
        max_allowed_margin = max_allowed_config
        base_capital = self.total_equity
        if base_capital > 0 and max_allowed_config > base_capital:
            max_allowed_margin = base_capital
            
        rate_divisor = self.config['rate_divisor']
        max_amount_margin = max_allowed_margin / rate_divisor
        max_allowed_display = max_allowed_margin
        max_amount_display = max_amount_margin
        
        leverage = float(self.config.get('leverage', 1))
        if leverage <= 0: leverage = 1
        
        remaining_amount_notional = max(0.0, (max_amount_margin * leverage) - used_amount_notional)
        
        with self.position_lock:
            if self.is_running:
                self.used_amount_notional = used_amount_notional
                self.remaining_amount_notional = remaining_amount_notional
            else:
                self.used_amount_notional = 0.0
                self.remaining_amount_notional = remaining_amount_notional # Show full potential when stopped
                self.trade_fees = 0.0

        # Net Profit & Fee Calculation (CENTRALIZED)
        trade_fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
        
        # 1. Size-Based Fees (Active Position only)
        self.size_fees = okx_pos_notional * trade_fee_pct
        
        # 2. Used-Based Fees (Active + Pending)
        self.used_fees = used_amount_notional * trade_fee_pct
        
        # 3. Total Fee (Alias for used_fees or historical session fees depending on context)
        # For the dashboard, trade_fees usually refers to session-wide or current exposure fees.
        self.trade_fees = self.used_fees
        
        # Real-time Net Profit (Floating)
        # USER REQUEST: Net Profit should reflect the "current running position pnl" (Gross)
        # to match the exchange display.
        self.net_profit = total_unrealized_pnl
        
        # Internal Net Profit (Fee Adjusted) for logic decisions (Auto-Quit/Auto-Add)
        self.net_profit_after_fees = total_unrealized_pnl - self.size_fees

        # Total Capital 2nd Logic
        if active_positions_count == 0:
            # Dynamic fallback to total_equity
            self.total_capital_2nd = self.total_equity
            self.cumulative_margin_used = 0.0
            self.auto_add_step_count = 0 
            self.last_add_price = 0.0
        else:
            # Sync last_add_price
            with self.position_lock:
                current_side = 'long' if self.in_position['long'] else 'short'
                current_avg = self.position_entry_price.get(current_side, 0.0)
                if self.last_add_price == 0:
                    self.last_add_price = current_avg
            
            base_capital = self.total_equity
            self.total_capital_2nd = max(0.0, base_capital - self.cumulative_margin_used)

        # ---------------------------------------------------------
        # AUTO-MARGIN LOGIC (Iterate active sides)
        # ---------------------------------------------------------
        if self.config.get('use_auto_margin', False):
            for side_key in ['long', 'short']:
                if self.in_position[side_key]:
                    pos = self.position_details.get(side_key, {})
                    liqp = self.position_liq[side_key]
                    mgn_mode = pos.get('mgnMode', 'cross')
                    
                    if mgn_mode == 'isolated' and liqp > 0:
                        sl_price = self.current_stop_loss[side_key]
                        should_add = False
                        
                        if side_key == 'long':
                            if sl_price > 0 and liqp >= sl_price: should_add = True
                        elif side_key == 'short':
                            if sl_price > 0 and liqp <= sl_price: should_add = True
                        
                        if should_add:
                            offset = self.config.get('auto_margin_offset', 30.0)
                            diff = abs(sl_price - liqp)
                            add_amt = diff + offset
                            raw_side = pos.get('posSide', 'net')
                            self.log(f"AUTO-MARGIN TRIGGERED [{side_key.upper()}]: Liq:{liqp} | SL:{sl_price} | Adding:{add_amt:.2f}", level="warning")
                            self._okx_adjust_margin(self.config['symbol'], raw_side, add_amt)

        # ---------------------------------------------------------
        # AUTO-ADD TRADING LOGIC (CRITICAL FIX APPLIED)
        # ---------------------------------------------------------
        # Only trigger if NOT in authoritative exit
        if not self.authoritative_exit_in_progress and okx_pos_notional > 0:
            
            # [CRITICAL FIX] Gate the logic with configuration check
            if self.config.get('use_add_pos_auto_cal', False):
                
                # ... (Existing Auto-Add Gap Logic) ...
                current_side = None
                with self.position_lock:
                     if self.in_position['long']: current_side = 'long'
                     elif self.in_position['short']: current_side = 'short'
                
                if current_side:
                     current_price = self.latest_trade_price
                     if self.last_add_price == 0:
                         self.last_add_price = self.position_entry_price[current_side]

                     if current_price:
                          base_gap = self.config.get('add_pos_gap_threshold', 5.0)
                          gap_offset = self.config.get('add_pos_gap_offset', 0.0)
                          gap_threshold = base_gap + (self.auto_add_step_count * gap_offset)
                          price_diff = 0.0
                          if current_side == 'long':
                              price_diff = self.last_add_price - current_price
                          else:
                              price_diff = current_price - self.last_add_price
                              
                          if price_diff >= gap_threshold:
                              self.log(f"Auto-Add Check: Gap Triggered: Diff {price_diff:.2f} >= {gap_threshold:.2f}. Current Avg: {self.last_add_price:.2f}", level="warning")
                              self.last_add_price = current_price 
                              remaining_margin_budget = self.total_capital_2nd
                              self._check_auto_add_position_step(current_price, current_side, remaining_margin_budget)

        # ---------------------------------------------------------
        # ---------------------------------------------------------
        # AUTO-EXIT LOGIC (Multiple Modes)
        # ---------------------------------------------------------
        # All modes now operate independently based on their enabled status.
        # ---------------------------------------------------------
        auto_exit_triggered = False
        exit_reason = ""
        current_size_fee = okx_pos_notional * trade_fee_pct if okx_pos_notional > 0 else 0.0

        # Check Auto-Manual Profit
        if self.config.get('use_pnl_auto_manual', False):
             manual_threshold = self.config.get('pnl_auto_manual_threshold', 100.0)
             if self.net_profit_after_fees >= manual_threshold:
                 auto_exit_triggered = True
                 exit_reason = f"Auto-Manual Profit Target: ${self.net_profit_after_fees:.2f} >= ${manual_threshold:.2f}"

        # Check Auto-Cal Profit
        if not auto_exit_triggered and self.config.get('use_pnl_auto_cal', False) and okx_pos_notional > 0:
            cal_times = self.config.get('pnl_auto_cal_times', 4)
            current_size_fee = okx_pos_notional * trade_fee_pct
            cal_threshold = cal_times * current_size_fee
            if self.net_profit_after_fees >= cal_threshold:
                auto_exit_triggered = True
                exit_reason = f"Auto-Cal Profit Target: ${self.net_profit_after_fees:.2f} >= ${cal_threshold:.2f} ({cal_times}x Fee)"

        # Check Auto-Cal Loss (Close All)
        if not auto_exit_triggered and self.config.get('use_pnl_auto_cal_loss', False) and okx_pos_notional > 0:
            loss_times = self.config.get('pnl_auto_cal_loss_times', 1.5)
            current_size_fee = okx_pos_notional * trade_fee_pct
            loss_threshold = -(current_size_fee * loss_times)
            if self.net_profit_after_fees <= loss_threshold:
                auto_exit_triggered = True
                exit_reason = f"Auto-Cal Loss Target: ${self.net_profit_after_fees:.2f} <= ${loss_threshold:.2f} ({loss_times}x Fee)"

        # Check Auto-Cal Size (Profit)
        if not auto_exit_triggered and self.config.get('use_size_auto_cal', False) and okx_pos_notional > 0:
            size_times = self.config.get('size_auto_cal_times', 2.0)
            current_size_fee = okx_pos_notional * trade_fee_pct
            size_target = current_size_fee * size_times
            if self.net_profit_after_fees >= size_target:
                auto_exit_triggered = True
                exit_reason = f"Auto-Cal Size Target: ${self.net_profit_after_fees:.2f} >= ${size_target:.2f} ({size_times}x Size Fee)"

        # Check Auto-Cal Size (Loss)
        if not auto_exit_triggered and self.config.get('use_size_auto_cal_loss', False) and okx_pos_notional > 0:
            size_loss_times = self.config.get('size_auto_cal_loss_times', 1.5)
            current_size_fee = okx_pos_notional * trade_fee_pct
            size_loss_threshold = -(current_size_fee * size_loss_times)
            if self.net_profit_after_fees <= size_loss_threshold:
                auto_exit_triggered = True
                exit_reason = f"Auto-Cal Size Loss Target: ${self.net_profit_after_fees:.2f} <= ${size_loss_threshold:.2f} ({size_loss_times}x Size Fee)"

        # MODE 2: Profit Target Exit (Unrealized PnL >= Size × Fee% × Multiplier)
        if not auto_exit_triggered and self.config.get('use_add_pos_profit_target', False) and okx_pos_notional > 0:
            profit_mult = self.config.get('add_pos_profit_multiplier', 1.5)
            # current_size_fee calculated above in line 3373
            # Fee-Aware Target: Covers Entry Fee + Exit Fee + Desired Multiplier Goal
            target_pnl = current_size_fee * (profit_mult + 2)
            
            # Detailed logging every few ticks for debugging
            if self.monitoring_tick % 5 == 0:
                self.log(f"[Mode 2 Check] Size: ${okx_pos_notional:.2f} | Fee%: {trade_fee_pct}% | Fee: ${current_size_fee:.4f} | Target: ${target_pnl:.4f} | Unrealized PnL: ${total_unrealized_pnl:.4f}", level="debug")
            
            # SAFETY CHECK: Only exit if PnL is POSITIVE and >= target
            if total_unrealized_pnl > 0 and total_unrealized_pnl >= target_pnl:
                auto_exit_triggered = True
                exit_reason = f"Mode 2 Profit Target: Unrealized ${total_unrealized_pnl:.2f} >= ${target_pnl:.2f} ({profit_mult}x Fee + 2x Fees Entry/Exit)"

        # MODE 1: Break-Even Exit (PnL Above Zero)
        if not auto_exit_triggered and self.config.get('use_add_pos_above_zero', False) and okx_pos_notional > 0:
             # current_size_fee calculated above
             near_zero_threshold = max(1.0, current_size_fee * 0.1)
             # Use the more sensitive total_unrealized_pnl (which includes recent price updates)
             if self.net_profit_after_fees >= -near_zero_threshold:
                 auto_exit_triggered = True
                 exit_reason = f"Mode 1 PnL Above Zero: Net ${self.net_profit_after_fees:.2f} ≈ $0"

        # ---------------------------------------------------------
        # Execute Authoritative Auto-Exit
        # ---------------------------------------------------------
        if auto_exit_triggered:
             with self.exit_lock:
                 if not self.authoritative_exit_in_progress:
                     self.log(f"[TARGET] AUTHORITATIVE AUTO-EXIT TRIGGERED: {exit_reason}", level="WARNING")
                     # Special logging for Mode 2 if it was the reason
                     if "Mode 2" in exit_reason:
                         self.log(f"[Mode 2 TRIGGER] Size: ${okx_pos_notional:.2f} | Target: ${target_pnl:.4f} | Unrealized: ${total_unrealized_pnl:.4f}", level="WARNING")
                     
                     threading.Thread(target=self._execute_trade_exit, args=(exit_reason,), daemon=True).start()

        # Need Add Calculation (Now also called in real-time path)
        self._calculate_need_add_metrics(okx_pos_notional)

        # Store metrics for Emitter
        self.max_allowed_display = max_allowed_display
        self.max_amount_display = max_amount_display
        
        # Store metrics for Emitter
        self.max_allowed_display = max_allowed_display
        self.max_amount_display = max_amount_display
        
        if self.is_running:
            self.remaining_amount_notional = remaining_amount_notional
            self.trade_fees = self.used_fees
        else:
            self.remaining_amount_notional = remaining_amount_notional # Show potential
            self.trade_fees = 0.0
            self.used_amount_notional = 0.0

    def _calculate_need_add_metrics(self, okx_pos_notional):
        """Helper to calculate Need Add values."""
        self.need_add_usdt_profit_target = 0.0
        self.need_add_usdt_above_zero = 0.0
        
        if okx_pos_notional > 0:
            try:
                avg_entry = 0.0
                pos_side = 'long'
                with self.position_lock:
                    if self.in_position['long']: 
                        avg_entry = self.position_entry_price.get('long', 0)
                        pos_side = 'long'
                    elif self.in_position['short']:
                        avg_entry = self.position_entry_price.get('short', 0)
                        pos_side = 'short'
                
                if avg_entry > 0:
                    # Fallback chain: WS Price -> Cached Detail Price -> Entry (as last resort to avoid 0)
                    current_price = self.latest_trade_price
                    if not current_price or current_price <= 0:
                        # Try to get from cached position details if available
                        details = self.position_details.get(pos_side, {})
                        current_price = safe_float(details.get('lastPx'))
                        if not current_price or current_price <= 0:
                             current_price = avg_entry # Fallback to entry so denom calculation doesn't crash but metrics stay near 0

                if current_price and current_price > 0:
                     recovery_pct = self.config.get('add_pos_recovery_percent', 0.6) / 100.0
                     
                     # Sensitivity Fix: Always show if price is against us
                     is_against = (pos_side == 'long' and current_price <= avg_entry) or \
                                  (pos_side == 'short' and current_price >= avg_entry)
                     
                     if is_against:
                         target_price_be = 0.0
                         if pos_side == 'long':
                             target_price_be = current_price * (1 + recovery_pct)
                             # Limit target to entry price if it would overshoot (stays sensitive)
                             target_price_be = min(target_price_be, avg_entry - 0.00000001)
                             
                             denom = target_price_be - current_price
                             if denom > 0:
                                 self.need_add_usdt_above_zero = okx_pos_notional * (avg_entry - target_price_be) / denom
                         else: # Short
                             # For Short, we need Entry > CurrentPrice for profit.
                             # recovery_pct=0.6% -> We want new entry to be 0.6% ABOVE current price
                             target_price_be = current_price * (1 + recovery_pct)
                             # Limit target to entry price if it would overshoot (stays sensitive)
                             target_price_be = max(target_price_be, avg_entry + 0.00000001)
                             
                             denom = target_price_be - current_price
                             if denom > 0:
                                  self.need_add_usdt_above_zero = okx_pos_notional * (target_price_be - avg_entry) / denom
                         
                         # Mode 2: Profit Target (Using fee-aware formula matching Target Exit)
                         profit_mult = self.config.get('add_pos_profit_multiplier', 1.5)
                         trade_fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
                         
                         # Total target PnL to cover fees and profit goal
                         current_size_fee = okx_pos_notional * trade_fee_pct
                         target_pnl_mode2 = current_size_fee * (profit_mult + 2)

                         if pos_side == 'long':
                             qty_contracts = okx_pos_notional / (avg_entry if avg_entry > 0 else 1.0)
                             # To get profit target_pnl_mode2, we need NewEntry > CurrentPrice
                             # We average DOWN to shift entry closer to price.
                             target_avg_for_profit = avg_entry - (target_pnl_mode2 / (qty_contracts if qty_contracts > 0 else 1.0))
                             
                             # If we are losing, current_price < avg_entry and current_price < target_avg_for_profit
                             denom_p = target_avg_for_profit - current_price
                             if denom_p > 0:
                                 self.need_add_usdt_profit_target = okx_pos_notional * (avg_entry - target_avg_for_profit) / denom_p
                         else: # Short
                             qty_contracts = okx_pos_notional / (avg_entry if avg_entry > 0 else 1.0)
                             # For Short, we want to average UP to raise our entry above current_price
                             target_avg_for_profit = avg_entry + (target_pnl_mode2 / (qty_contracts if qty_contracts > 0 else 1.0))

                             # If we are losing, current_price > avg_entry and current_price > target_avg_for_profit
                             denom_p = current_price - target_avg_for_profit
                             if denom_p > 0:
                                 self.need_add_usdt_profit_target = okx_pos_notional * (target_avg_for_profit - avg_entry) / denom_p
            except Exception as e:
                self.log(f"Error calculating Need Add: {e}", level="debug")

    def _emit_socket_updates(self):
        """
        Phase 3 of Unified Loop: Emitter.
        Sends calculated data to the frontend.
        """
        with self.trade_data_lock:
            current_trades = self.open_trades
            
        self.emit('trades_update', {'trades': current_trades})

        # Emit 'account_update' with calculated targets for real-time sync
        # Calculate current auto-exit targets based on position size
        # Standardize fee multiplier (0.08 / 100 = 0.0008)
        trade_fee_pct_raw = self.config.get('trade_fee_percentage', 0.08)
        trade_fee_dec = trade_fee_pct_raw / 100.0
        
        okx_pos_notional = getattr(self, 'cached_pos_notional', 0.0)
        current_size_fee = okx_pos_notional * trade_fee_dec if okx_pos_notional > 0 else 0.0
        
        self.emit('account_update', {
            'total_trades': getattr(self, 'cached_active_positions_count', 0) + self.total_trades_count,
            'total_capital': self.total_equity, 
            'total_capital_2nd': max(0.0, self.total_equity - self.cumulative_margin_used),
            'max_allowed_used_display': getattr(self, 'max_allowed_display', 0.0), 
            'max_amount_display': getattr(self, 'max_amount_display', 0.0),
            'used_amount': getattr(self, 'used_amount_notional', 0.0), 
            'size_amount': okx_pos_notional,
            'trade_fees': getattr(self, 'trade_fees', 0.0),
            'remaining_amount': getattr(self, 'remaining_amount_notional', 0.0), 
            'total_balance': self.account_balance,
            'available_balance': self.available_balance,
            'net_profit': getattr(self, 'net_profit', 0.0),
            'total_trade_profit': self.total_trade_profit,
            'total_trade_loss': self.total_trade_loss,
            'net_trade_profit': self.net_trade_profit,
            'daily_reports': self.daily_reports,
            'need_add_usdt': getattr(self, 'need_add_usdt_profit_target', 0.0),
            'need_add_above_zero': getattr(self, 'need_add_usdt_above_zero', 0.0),
            # Real-time calculated targets (update when fee% or multiplier changes)
            'auto_cal_profit_target': self.config.get('pnl_auto_cal_times', 4) * current_size_fee,
            'auto_cal_loss_target': -self.config.get('pnl_auto_cal_loss_times', 1.5) * current_size_fee,
            'size_profit_target': self.config.get('size_auto_cal_times', 2.0) * current_size_fee,
            'size_loss_target': -self.config.get('size_auto_cal_loss_times', 1.5) * current_size_fee,
            'mode_2_profit_target': self.config.get('add_pos_profit_multiplier', 1.5) * current_size_fee
        })
        
        self._check_and_save_daily_report()
        
        # Debug Log
        if self.monitoring_tick % 10 == 0:
             used = getattr(self, 'used_amount_notional', 0.0)
             size = getattr(self, 'cached_pos_notional', 0.0)
             self.log(f"Account Update | Used: ${used:.2f} | Size: ${size:.2f}", level="debug")

    def _update_realtime_metrics_from_price(self):
        """
        Calculates PnL and Equity in real-time based on public ticker price.
        Used to provide instant feedback on the dashboard.
        """
        current_price = self.latest_trade_price
        if not current_price or current_price <= 0:
            return

        with self.position_lock:
            total_upl = 0.0
            
            for side in ['long', 'short']:
                if self.in_position[side]:
                    entry = self.position_entry_price[side]
                    qty = self.position_qty[side] # Already contains contract_size factor
                    
                    if entry > 0:
                        # PnL = (Mark - Entry) * Qty
                        # qty is positive for Long, negative for Short.
                        side_pnl = (current_price - entry) * qty
                        total_upl += side_pnl
            
            # Update Net Profit (Gross UPL matching Exchange Display)
            self.net_profit = total_upl
            
            # Centralized Fee Calculation
            trade_fee_pct = self.config.get('trade_fee_percentage', 0.08) / 100.0
            okx_pos_notional = getattr(self, 'cached_pos_notional', 0.0)
            self.size_fees = okx_pos_notional * trade_fee_pct
            
            # Internal Net Profit (Fee Adjusted) for logic decisions (Auto-Quit / Auto-Add)
            self.net_profit_after_fees = total_upl - self.size_fees
            
            # Estimate Real-time Equity: Effective Base + Current Floating PnL
            # effective_wallet_balance = (Exchange Equity - Exchange PnL)
            # This handles accounts where margin is held in CCYs other than USDT.
            if self.effective_wallet_balance > 0:
                self.total_equity = self.effective_wallet_balance + total_upl
            else:
                # Fallback to wallet balance if effective base not yet sync'd
                self.total_equity = self.account_balance + total_upl

            # NEW: Trigger Need Add calculation in real-time path
            okx_pos_notional = getattr(self, 'cached_pos_notional', 0.0)
            self._calculate_need_add_metrics(okx_pos_notional)
            
        # Emit update to frontend immediately
        self._emit_socket_updates()

    def fetch_account_data_sync(self):
        """Fetches account data synchronously and updates dashboard before start."""
        self._sync_server_time()
        self._fetch_product_info(self.config['symbol'])
        self._sync_account_data()
        self._execute_position_management()
        self._emit_socket_updates()

        # Sync pending_entry_ids with active orders from OKX
        active_okx_ids = [t['id'] for t in self.open_trades]
        with self.position_lock:
            existing_pending = list(self.pending_entry_ids)
            for p_id in existing_pending:
                if p_id not in active_okx_ids:
                    # Order is no longer on books (filled or cancelled)
                    self.pending_entry_ids.remove(p_id)
                    if p_id in self.pending_entry_order_details:
                        del self.pending_entry_order_details[p_id]
                    self.log(f"Pending order {p_id} cleared from tracking (Filled or Cancelled).", level="debug")
                    # We might want to trigger TP/SL update here too.
                    self._should_update_tpsl = True # Flag to update TP/SL if needed

        if getattr(self, '_should_update_tpsl', False) and any(self.in_position.values()) and self.is_running:
            self._should_update_tpsl = False
            # Call TP/SL modification to sync with new average price
            threading.Thread(target=self.batch_modify_tpsl, daemon=True).start()
            
            # REMOVED: self.initial_total_capital = total_balance reset. 
            # We want to keep the original capital to track net profit correctly.


        # Trade Fee Calculation: (Used + Remaining) * Fee_Percentage
        trade_fee_pct = self.config.get('trade_fee_percentage', 0.07)
        used_fee = self.used_amount_notional * (trade_fee_pct / 100.0)
        remaining_fee = self.remaining_amount_notional * (trade_fee_pct / 100.0)
        trade_fees = used_fee + remaining_fee

        # Update persistent attributes for status retrieval
        # Note: self.max_allowed_display etc are already set in _execute_position_management
        self.trade_fees = trade_fees

        # Note: Auto-Add and Auto-Exit logic is handled in _execute_position_management
        # This method is only for initial sync on startup

        # Explicit Auto-Exit for Mode 1 (PnL Above Zero / Break-Even)
        # Mode 1 takes priority over Mode 2 (exits at break-even before waiting for profit)
        if self.config.get('use_add_pos_above_zero', False) and okx_pos_notional > 0:
             trade_fee_pct = self.config.get('trade_fee_percentage', 0.07)
             current_size_fee = okx_pos_notional * (trade_fee_pct / 100.0)
             
             # Define "near zero" threshold: $1 or 10% of size fee (whichever is larger)
             # This prevents premature exit while ensuring we exit close to break-even
             near_zero_threshold = max(1.0, current_size_fee * 0.1)
             
             # Check if PnL is above the negative threshold (approaching break-even)
             # Example: If threshold is $2, exit when PnL >= -$2 (i.e., loss is $2 or less)
             if self.net_profit_after_fees >= -near_zero_threshold:
                 exit_reason = f"Mode 1 PnL Above Zero: ${self.net_profit_after_fees:.2f} ≈ $0 (threshold: ${near_zero_threshold:.2f})"
                 with self.exit_lock:
                     if not self.authoritative_exit_in_progress:
                         self.log(f"Mode 1 Check {exit_reason}. Auto-Closing Position...", level="WARNING")
                         threading.Thread(target=self._execute_trade_exit, args=(exit_reason,), daemon=True).start()

    def test_api_credentials(self):
        # Store current global API settings
        
        original_okx_api_key = self.okx_api_key
        original_okx_api_secret = self.okx_api_secret
        original_okx_passphrase = self.okx_passphrase
        original_okx_simulated_trading_header = self.okx_simulated_trading_header

        try:
            # Set global API settings for testing based on self.config (which was modified by app.py)
            use_dev = self.config.get('use_developer_api', False)
            use_demo = self.config.get('use_testnet', False)

            if use_dev:
                if use_demo:
                    self.okx_api_key = self.config.get('dev_demo_api_key', '')
                    self.okx_api_secret = self.config.get('dev_demo_api_secret', '')
                    self.okx_passphrase = self.config.get('dev_demo_api_passphrase', '')
                else:
                    self.okx_api_key = self.config.get('dev_api_key', '')
                    self.okx_api_secret = self.config.get('dev_api_secret', '')
                    self.okx_passphrase = self.config.get('dev_passphrase', '')
            else:
                if use_demo:
                    self.okx_api_key = self.config.get('okx_demo_api_key', '')
                    self.okx_api_secret = self.config.get('okx_demo_api_secret', '')
                    self.okx_passphrase = self.config.get('okx_demo_api_passphrase', '')
                else:
                    self.okx_api_key = self.config.get('okx_api_key', '')
                    self.okx_api_secret = self.config.get('okx_api_secret', '')
                    self.okx_passphrase = self.config.get('okx_passphrase', '')

            if use_demo:
                self.okx_simulated_trading_header = {'x-simulated-trading': '1'}
            else:
                self.okx_simulated_trading_header = {}

            # Attempt a simple API call, e.g., get account balance
            path_balance = "/api/v5/account/balance"
            params_balance = {"ccy": "USDT"}
            response_balance = self._okx_request("GET", path_balance, params=params_balance, max_retries=1) # Only 1 retry for test

            if response_balance and response_balance.get('code') == '0':
                return True
            else:
                return False
        except Exception as e:
            self.log(f"Error during API credential test: {e}", level="error")
            return False
        finally:
            # Restore original global API settings
            self.okx_api_key = original_okx_api_key
            self.okx_api_secret = original_okx_api_secret
            self.okx_passphrase = original_okx_passphrase
            self.okx_simulated_trading_header = original_okx_simulated_trading_header

    def batch_modify_tpsl(self):
        self.log("Initiating batch TP/SL modification...", level="debug")
        try:
            latest_data = self._get_latest_data_and_indicators()
            if not latest_data:
                self.log("Could not get current market price for batch TP/SL modification.", level="debug")
                return
                
            current_market_price = latest_data.get('current_price')
            if current_market_price is None:
                self.log("Current market price is None for batch TP/SL modification.", level="debug")
                return

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') != '0':
                self.log(f"Failed to fetch open positions for batch TP/SL modification: {response}", level="error")
                self.emit('error', {'message': f'Failed to batch modify TP/SL: Could not fetch open positions.'})
                return

            positions = response.get('data', [])
            modified_count = 0
            tp_price_offset = self.config['tp_price_offset']
            sl_price_offset = self.config['sl_price_offset']
            price_precision = self.product_info.get('pricePrecision', 4)
            qty_precision = self.product_info.get('qtyPrecision', 8)

            # Group positions by side for batch processing if needed, but here we loop
            for pos in positions:
                if pos.get('instId') == self.config['symbol']:
                    pos_qty = safe_float(pos.get('pos', '0'))
                    pos_side_raw = pos.get('posSide', 'net')
                    avg_px = safe_float(pos.get('avgPx', '0'))

                    if abs(pos_qty) > 0 and avg_px > 0:
                        # Map to our internal side key using exchange data
                        # Map to our internal side key using exchange data
                        if pos_side_raw == 'short':
                            side_key = 'short'
                        elif pos_side_raw == 'long':
                            side_key = 'long'
                        else: # 'net' mode
                             side_key = 'long' if pos_qty > 0 else 'short'

                        order_side = "sell" if side_key == 'long' else "buy"
                        new_tp = 0.0
                        new_sl = 0.0

                        # Safely calculate targets if offsets are provided
                        if tp_price_offset and safe_float(tp_price_offset) > 0:
                            if side_key == 'long':
                                new_tp = avg_px + safe_float(tp_price_offset)
                            else:
                                new_tp = avg_px - safe_float(tp_price_offset)
                        else:
                            self.log(f"Batch Sync: TP offset is null or 0 for {side_key.upper()}. Skipping TP calc.", level="debug")

                        if sl_price_offset and safe_float(sl_price_offset) > 0:
                            if side_key == 'long':
                                new_sl = avg_px - safe_float(sl_price_offset)
                            else:
                                new_sl = avg_px + safe_float(sl_price_offset)
                        else:
                            self.log(f"Batch Sync: SL offset is null or 0 for {side_key.upper()}. Skipping SL calc.", level="debug")

                        self.log(f"Syncing TP/SL for {side_key.upper()} position. Avg Price: {avg_px:.{price_precision}f}", level="debug")



                        with self.position_lock:
                            # 1. Fetch and cancel existing algo orders for this SYMBOL + SIDE
                            # Note: OKX allows filtering by posSide in some cases, but here we check all and filter locally
                            path_algo = "/api/v5/trade/orders-algo-pending"
                            params_algo = {"instType": "SWAP", "instId": self.config['symbol'], "ordType": "conditional"}
                            resp_algo = self._okx_request("GET", path_algo, params=params_algo)
                            
                            if resp_algo and resp_algo.get('code') == '0':
                                for algo_order in resp_algo.get('data', []):
                                    if algo_order.get('posSide') == pos_side_raw:
                                        self._okx_cancel_algo_order(self.config['symbol'], algo_order.get('algoId'))
                            
                            self.position_exit_orders[side_key] = {}
                            time.sleep(0.2) 

                            # Place new TP and SL
                            trig_px_type = self.config.get('trigger_price', 'last')
                            
                            if tp_price_offset and safe_float(tp_price_offset) > 0:
                                tp_body = {
                                    "instId": self.config['symbol'],
                                    "tdMode": self.config.get('mode', 'cross'),
                                    "side": order_side,
                                    "posSide": pos_side_raw,
                                    "ordType": "conditional",
                                    "sz": f"{abs(pos_qty):.{qty_precision}f}",
                                    "tpTriggerPx": f"{new_tp:.{price_precision}f}",
                                    "tpTriggerPxType": trig_px_type,
                                    "tpOrdPx": "-1",
                                    "reduceOnly": "true"
                                }

                                tp_order = self._okx_place_algo_order(tp_body, verbose=False)
                                if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                                    self.position_exit_orders[side_key]['tp'] = tp_order.get('algoId') or tp_order.get('ordId')
                                    self.log(f"[TARGET] {side_key.upper()} TP Set: {new_tp:.{price_precision}f}", level="info")
                            else:
                                self.log(f"Skipping TP batch modify for {side_key.upper()} (No offset)", level="debug")
                            
                            if sl_price_offset and safe_float(sl_price_offset) > 0:
                                sl_body = {
                                    "instId": self.config['symbol'],
                                    "tdMode": self.config.get('mode', 'cross'),
                                    "side": order_side,
                                    "posSide": pos_side_raw,
                                    "ordType": "conditional",
                                    "sz": f"{abs(pos_qty):.{qty_precision}f}",
                                    "slTriggerPx": f"{new_sl:.{price_precision}f}",
                                    "slTriggerPxType": trig_px_type,
                                    "slOrdPx": "-1",
                                    "reduceOnly": "true"
                                }

                                sl_order = self._okx_place_algo_order(sl_body, verbose=False)
                                if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                                    self.position_exit_orders[side_key]['sl'] = sl_order.get('algoId') or sl_order.get('ordId')
                                    self.log(f"[TARGET] {side_key.upper()} SL Set: {new_sl:.{price_precision}f}", level="info")
                            else:
                                self.log(f"Skipping SL batch modify for {side_key.upper()} (No offset)", level="debug")
                            
                            # Only count as modified if at least one order was placed
                            if (tp_price_offset and safe_float(tp_price_offset) > 0) or (sl_price_offset and safe_float(sl_price_offset) > 0):
                                self.current_take_profit[side_key] = new_tp
                                self.current_stop_loss[side_key] = new_sl
                                modified_count += 1
                                
                                # Emit side-specific update
                                self.emit('position_update', {
                                    'in_position': self.in_position[side_key],
                                    'position_entry_price': self.position_entry_price[side_key],
                                    'position_qty': self.position_qty[side_key],
                                    'current_take_profit': self.current_take_profit[side_key],
                                    'current_stop_loss': self.current_stop_loss[side_key],
                                    'side': side_key
                                })

            if modified_count > 0:
                self.log(f"Successfully modified TP/SL for {modified_count} sides.", level="info")
            else:
                self.log("No active positions found (or matched criteria) to modify TP/SL.", level="debug")
        
        except Exception as e:
            self.log(f"Exception in batch_modify_tpsl: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch modify TP/SL: {str(e)}'})
        self.log("Batch TP/SL modification complete.", level="debug")



    def batch_cancel_orders(self):
        self.log("Initiating batch order cancellation...", level="info")
        try:
            cancelled_count = 0
            
            # 1. Cancel Limit Orders
            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                orders = response.get('data', [])
                for order in orders:
                    order_id = order.get('ordId')
                    if order_id:
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)

            # 2. Cancel Algo Orders (TP/SL/Conditional)
            path_algo = "/api/v5/trade/orders-algo-pending"
            params_algo = {
                "instType": "SWAP", 
                "instId": self.config['symbol'],
                "ordType": "conditional" # RESTORED: Required by OKX
            }
            response_algo = self._okx_request("GET", path_algo, params=params_algo)

            if response_algo and response_algo.get('code') == '0':
                algo_orders = response_algo.get('data', [])
                for algo_order in algo_orders:
                    algo_id = algo_order.get('algoId')
                    if algo_id:
                        if self._okx_cancel_algo_order(self.config['symbol'], algo_id):
                            cancelled_count += 1
                            time.sleep(0.1)

            if cancelled_count > 0:
                self.log(f"[DONE] Cancelled {cancelled_count} pending orders.", level="info")
            else:
                self.log("No orders to cancel.", level="warning")
                self.emit('warning', {'message': 'No pending orders found to cancel.'})

        except Exception as e:
            self.log(f"Exception in batch_cancel_orders: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch cancel orders: {str(e)}'})
            self.log("Batch order cancellation complete.", level="info")

    def emergency_sl(self):
        self.log("🚨 EMERGENCY STOP LOSS TRIGGERED: Closing all positions and orders...", level="warning")
        try:
            # We use the exchange-authoritative exit logic to ensure EVERYTHING is closed
            self._execute_trade_exit("Manual Dashboard Trigger")
            self.emit('success', {'message': 'Emergency SL complete. All positions/orders cleared.'})
        except Exception as e:
            self.log(f"Error during Emergency SL: {e}", level="error")
            self.emit('error', {'message': f'Emergency SL failed: {e}'})
    def apply_live_config_update(self, new_config):
        """
        Dynamically applies certain config updates while the bot is running.
        Returns a dictionary with status and warning messages.
        """
        warnings = []
        old_symbol = self.config.get('symbol')
        new_symbol = new_config.get('symbol')
        old_lev = self.config.get('leverage')
        new_lev = new_config.get('leverage')
        old_pos_mode = self.config.get('okx_pos_mode')
        new_pos_mode = new_config.get('okx_pos_mode')

        # 1. Update internal config object
        self.config = new_config
        self.log("Applying live configuration updates (including new Auto-Add parameters)...", level="info")

        # Refresh credentials hash
        self._apply_api_credentials()

        # 2. Handle Leverage Change
        if new_lev != old_lev:
            self.log(f"Leverage change detected: {old_lev} -> {new_lev}. Updating on exchange...", level="info")
            lev_success = False
            if new_pos_mode == 'long_short_mode':
                l_ok = self._okx_set_leverage(new_symbol, new_lev, pos_side="long")
                s_ok = self._okx_set_leverage(new_symbol, new_lev, pos_side="short")
                lev_success = l_ok and s_ok
            else:
                lev_success = self._okx_set_leverage(new_symbol, new_lev, pos_side="net")
            
            if lev_success:
                self.log(f"[DONE] Leverage successfully updated to {new_lev}x", level="info")
            else:
                warnings.append(f"Failed to update leverage to {new_lev}x on exchange.")

        # 3. Handle Symbol Change (Sensitive)
        if new_symbol != old_symbol:
            # Check for open positions
            in_pos = False
            with self.position_lock:
                # We check the authoritative state in self.in_position which is synced with the exchange
                in_pos = any(self.in_position.values())
            
            if in_pos:
                self.log(f"⚠️ Cannot change symbol to {new_symbol} while positions are open for {old_symbol}. Reverting symbol config.", level="warning")
                self.config['symbol'] = old_symbol
                warnings.append(f"Symbol change to {new_symbol} blocked: Please close existing positions for {old_symbol} first.")
            else:
                self.log(f"🔄 Switching symbol from {old_symbol} to {new_symbol}...", level="info")
                
                # Update subscription target
                self.subscribed_instrument = new_symbol
                
                # Stop WebSocket to clear old subscriptions
                if self.ws_public or self.ws_private:
                    try: self.ws_public.close()
                    except: pass
                    try: self.ws_private.close()
                    except: pass
                
                # Fetch new product info
                if self._fetch_product_info(new_symbol):
                    # Set leverage for the new symbol
                    if new_pos_mode == 'long_short_mode':
                        self._okx_set_leverage(new_symbol, new_lev, pos_side="long")
                        self._okx_set_leverage(new_symbol, new_lev, pos_side="short")
                    else:
                        self._okx_set_leverage(new_symbol, new_lev, pos_side="net")
                    
                    self.log(f"[DONE] Successfully swapped to {new_symbol}.", level="info")
                else:
                    self.log(f"❌ Failed to fetch info for {new_symbol}. Reverting to {old_symbol}.", level="error")
                    self.config['symbol'] = old_symbol
                    warnings.append(f"Failed to switch to {new_symbol}: could not fetch product info.")
                    # Restart WS with old symbol if needed (it will restart automatically in the loop)

        return {"success": True, "warnings": warnings}
