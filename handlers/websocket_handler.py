import json
import time
import threading
import websocket
import hmac
import hashlib
import base64

class WebSocketHandler:
    def __init__(self, logger_func, config, okx_client, message_callback):
        self.log = logger_func
        self.config = config
        self.okx_client = okx_client
        self.message_callback = message_callback

        self.ws_public = None
        self.ws_private = None
        self.ws_thread_public = None
        self.ws_thread_private = None

        self.stop_event = threading.Event()
        self.ws_subscriptions_ready = threading.Event()
        self.pending_subscriptions = set()
        self.confirmed_subscriptions = set()
        self.current_url_params = {'use_testnet': None, 'symbol': None}

    def _get_ws_url(self, ws_type="public"):
        use_testnet = self.config.get('use_testnet', False)
        if ws_type == "public":
            return "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999" if use_testnet else "wss://ws.okx.com:8443/ws/v5/public"
        else:
            return "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999" if use_testnet else "wss://ws.okx.com:8443/ws/v5/private"

    def start(self):
        self.stop()
        self.stop_event.clear()
        self.ws_subscriptions_ready.clear()
        self.pending_subscriptions.clear()
        self.confirmed_subscriptions.clear()

        self.current_url_params['use_testnet'] = self.config.get('use_testnet', False)
        self.current_url_params['symbol'] = self.config.get('symbol')

        url_public = self._get_ws_url("public")
        url_private = self._get_ws_url("private")

        self.ws_public = websocket.WebSocketApp(
            url_public,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self.ws_private = websocket.WebSocketApp(
            url_private,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.ws_thread_public = threading.Thread(target=self.ws_public.run_forever, daemon=True)
        self.ws_thread_private = threading.Thread(target=self.ws_private.run_forever, daemon=True)

        self.ws_thread_public.start()
        self.ws_thread_private.start()
        self.log("WebSocket connections (Public & Private) initiated.", level="debug")

    def stop(self):
        self.stop_event.set()
        if self.ws_public:
            try: self.ws_public.close()
            except: pass
        if self.ws_private:
            try: self.ws_private.close()
            except: pass

    def restart(self):
        self.log("Restarting WebSocket connections...", level="info")
        self.stop()
        time.sleep(1) # Give it a moment to close
        self.start()

    def _on_message(self, ws, message):
        is_private = (ws == self.ws_private)
        try:
            msg = json.loads(message)
            if 'event' in msg:
                self._handle_event(msg, is_private)
            self.message_callback(msg, is_private)
        except Exception as e:
            self.log(f"Exception in WebSocket _on_message: {e}", level="error")

    def _handle_event(self, msg, is_private):
        if msg['event'] == 'subscribe':
            arg = msg.get('arg', {})
            prefix = "private" if is_private else "public"
            channel_id = f"{prefix}:{arg.get('channel')}:{arg.get('instId') if arg.get('instId') else ''}"
            self.confirmed_subscriptions.add(channel_id)
            if self.pending_subscriptions and self.pending_subscriptions == self.confirmed_subscriptions:
                self.ws_subscriptions_ready.set()
        elif msg['event'] == 'login':
            if msg.get('code') == '0':
                self.log("WebSocket Login Successful.", level="info")
                self._send_subscriptions(is_private=True)

    def _on_open(self, ws):
        if ws == self.ws_private:
            self._login_websocket()
        else:
            self._send_subscriptions(is_private=False)

    def _login_websocket(self):
        timestamp = str(int(time.time()))
        message = timestamp + "GET/users/self/verify"
        signature = base64.b64encode(hmac.new(self.okx_client.okx_api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
        payload = {"op": "login", "args": [{"apiKey": self.okx_client.okx_api_key, "passphrase": self.okx_client.okx_passphrase, "timestamp": timestamp, "sign": signature}]}
        self.ws_private.send(json.dumps(payload))

    def _send_subscriptions(self, is_private=False):
        ws = self.ws_private if is_private else self.ws_public
        symbol = self.config['symbol']
        if not is_private:
            channels = [{"channel": "trades", "instId": symbol}, {"channel": "tickers", "instId": symbol}]
            prefix = "public"
        else:
            channels = [{"channel": "account"}, {"channel": "positions", "instType": "ANY"}, {"channel": "orders", "instType": "ANY"}]
            prefix = "private"
        ws.send(json.dumps({"op": "subscribe", "args": channels}))
        self.pending_subscriptions.update({f"{prefix}:{arg['channel']}:{arg.get('instId', '')}" for arg in channels})

    def _on_error(self, ws, error): self.log(f"WebSocket error: {error}", level="error")
    def _on_close(self, ws, code, msg): self.log(f"WebSocket closed: {code} {msg}", level="debug")
