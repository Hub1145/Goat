import json
import os
import logging
from datetime import datetime, timezone
from collections import deque

class ReportingHandler:
    def __init__(self, logger_func, config, analytics_path="analytics.json"):
        self.log = logger_func
        self.config = config
        self.analytics_path = analytics_path
        self.daily_reports = []
        self.total_trade_profit = 0.0
        self.total_trade_loss = 0.0
        self.net_trade_profit = 0.0
        self._load_analytics()

    def _load_analytics(self):
        try:
            if os.path.exists(self.analytics_path):
                with open(self.analytics_path, 'r') as f:
                    data = json.load(f)
                    self.daily_reports = data.get('daily_reports', [])
            else:
                self.daily_reports = []
        except Exception as e:
            self.log(f"Error loading analytics: {e}", level="error")

    def save_analytics(self):
        try:
            data = {
                'daily_reports': self.daily_reports
            }
            with open(self.analytics_path, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            self.log(f"Error saving analytics: {e}", level="error")

    def check_and_save_daily_report(self, total_equity):
        now = datetime.now(timezone.utc)
        today_str = now.strftime('%Y-%m-%d')

        # Only save once per day at midnight UTC (or when first checked after midnight)
        if not self.daily_reports or self.daily_reports[-1].get('date') != today_str:
            # Calculate compound interest since start or last day
            compound = 1.0
            if self.daily_reports:
                initial_capital = self.daily_reports[0].get('total_capital', total_equity)
                if initial_capital > 0:
                    compound = total_equity / initial_capital

            report = {
                "date": today_str,
                "total_capital": total_equity,
                "net_trade_profit": self.net_trade_profit,
                "compound_interest": round(compound, 4)
            }
            self.daily_reports.append(report)
            self.save_analytics()
            self.log(f"Daily performance report saved for {today_str}", level="info")
