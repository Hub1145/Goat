import time
import logging
import math
from datetime import datetime, timezone
from handlers.utils import safe_float

class AccountManager:
    def __init__(self, engine):
        self.engine = engine
        self.config = engine.config
        self.daily_reports = []

    def sync_server_time(self):
        return self.engine.okx_client.sync_server_time()

    def fetch_product_info(self, symbol):
        info = self.engine.okx_client.fetch_product_info(symbol)
        if info:
            self.engine.product_info = {
                'priceTickSize': float(info.get('tickSz')),
                'qtyPrecision': int(abs(math.log10(float(info.get('lotSz'))))),
                'pricePrecision': int(abs(math.log10(float(info.get('tickSz'))))),
                'contractSize': float(info.get('ctVal', '1'))
            }
            return True
        return False

    def sync_account_data(self):
        path = "/api/v5/account/balance"
        params = {"ccy": "USDT"}
        res = self.engine.okx_client.request("GET", path, params=params)
        if res and res.get('code') == '0':
            data = res.get('data', [{}])[0]
            self.engine.total_equity = float(data.get('totalEq', 0))
            for d in data.get('details', []):
                if d.get('ccy') == 'USDT':
                    self.engine.account_balance = float(d.get('bal', 0))
                    self.engine.available_balance = float(d.get('availBal', 0))
                    break

    def check_daily_report(self):
        now = datetime.now(timezone.utc)
        today = now.strftime('%Y-%m-%d')
        if not self.daily_reports or self.daily_reports[-1]['date'] != today:
            self.daily_reports.append({"date": today, "total_capital": self.engine.total_equity})
