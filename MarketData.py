from websocket import create_connection
import websocket
import logging
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger('wrapper')

base_url = {
    'futures': 'wss://stream.binance.com:9443/'
}

class BinanceWebSocketApp:
    def __init__(
        self,
        symbols,
        channels,
        on_open = None,
        on_message = None,
        on_error = None,
        on_close = None
    ):
        # TODO — Maybe Testnet in the future
        streams = [f'{symbol}@{channel}' for symbol in symbols for channel in channels]
        streams = '/'.join(streams)
        self.message_count = 0
        self.custom_on_open = on_open if on_open is not None else lambda ws: logger.debug('Opened connection')
        self.custom_on_error = on_error if on_error is not None else lambda ws, error: logger.error(error)
        self.custom_on_close = on_close if on_close is not None else lambda ws, close_status_code, close_msg: logger.debug('Closed connection')
        self.custom_on_message = on_message if on_message is not None else lambda ws, message: logger.debug(message)
        self.ws = websocket.WebSocketApp(base_url['futures'] + f'stream?streams={streams}', on_message=self.on_message, on_open=self.on_open, on_error=self.on_error, on_close=self.on_close)
        self.scheduler  = None
    
    def on_message(self, ws, message):
        self.message_count += 1
        self.custom_on_message(ws, message)
    
    def on_open(self, ws):
        self.custom_on_open(ws)
    
    def on_error(self, ws, error):
        self.custom_on_error(ws, error)
    
    def on_close(self, ws, close_status_code, close_msg):
        self.custom_on_close(ws, close_status_code, close_msg)
    
    def record_statistics(self):
        logger.debug(f'Received {self.message_count} messages')
        self.message_count = 0
    
    def run(self, dispatcher = None):
        self.ws.run_forever(
            reconnect=0,
            http_proxy_host='127.0.0.1',
            http_proxy_port='7890',
            proxy_type='http',
            dispatcher = dispatcher
        )
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.record_statistics, 'interval', seconds=60)