import time
from handlers.utils import safe_float

class StrategyManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.last_entry_time = 0
        self.last_eval_log_time = 0

    def check_entry_conditions(self):
        if not self.engine.is_running: return []

        now = time.time()
        cooldown = self.config.get('loop_time_seconds', 10)
        should_log = (now - self.last_eval_log_time) >= max(30, cooldown)

        if now - self.last_entry_time < cooldown:
            return []

        if not self.engine.indicator_manager.check_candlestick_conditions():
            return []

        price = self.engine.latest_trade_price
        if price <= 0: return []

        direction = self.config.get('direction', 'both')
        offset = safe_float(self.config.get('entry_price_offset', 0))

        long_line = self.config.get('long_safety_line_price', 0)
        short_line = self.config.get('short_safety_line_price', 0)

        if should_log:
            self.engine.log(f"Strategy Evaluation - Price: {price}, Safety Lines: [Long <= {long_line}, Short >= {short_line}], Direction: {direction}", level="debug")
            self.last_eval_log_time = now

        signals = []
        # Check Long
        if direction in ['long', 'both']:
            if self.engine.in_position['long'] or self.engine.order_manager.pending_entry_ids:
                pass # Already in long or pending
            elif long_line > 0 and price <= long_line:
                self.engine.log(f"Long Signal Triggered: Price {price} <= {long_line}", level="info")
                signals.append({'side': 'long', 'price': price - offset})

        # Check Short
        if direction in ['short', 'both']:
            if self.engine.in_position['short'] or self.engine.order_manager.pending_entry_ids:
                pass # Already in short or pending
            elif short_line > 0 and price >= short_line:
                self.engine.log(f"Short Signal Triggered: Price {price} >= {short_line}", level="info")
                signals.append({'side': 'short', 'price': price + offset})

        return signals

    def execute_strategy(self):
        self.engine.log("--- Executing Strategy Analysis ---", level="debug")
        signals = self.check_entry_conditions()
        for sig in signals:
            self.engine.order_manager.initiate_entry_batch(sig['price'], sig['side'], self.config.get('batch_size_per_loop', 1))
            self.last_entry_time = time.time()
