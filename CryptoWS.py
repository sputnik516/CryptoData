import asyncio
import websockets
import json
import sys
from datetime import datetime as dt
from pytz import timezone, utc
import numpy as np
from db import DBtools
import pandas as pd
from concurrent.futures._base import CancelledError


class CryptoWS(object):

    def __init__(self):
        # DB Connection
        self.db_con = DBtools()
        self.est_tz = timezone('America/New_York')

    def parse_resp(self, resp_str):
        """
        Parse response data
        :param resp_str:
        :return:
        """
        resp = json.loads(resp_str)

        # If BFX and If "te" message
        if self.src == 'BFX' and isinstance(resp, list) and resp[1] == 'te':
            # Convert to UTC timestamp from local (EST)
            timestamp = dt.fromtimestamp(resp[3], self.est_tz).astimezone(utc).replace(tzinfo=None)

            temp_dict = {'ticker': self.cur,
                         'venue': self.src,
                         'seq': resp[0],
                         'tr_id': resp[2],
                         'trade_ts': timestamp,
                         'tr_price': resp[4],
                         'tr_size': abs(resp[5]),
                         'tr_side': int(np.sign(resp[5]))
                         }

        # Coinbase
        elif self.src == 'CB' and (resp['type'] in ['match', 'last_match']):
            timestamp = dt.strptime(resp['time'], '%Y-%m-%dT%H:%M:%S.%fZ')

            if resp['side'] == 'buy':
                side = 1
            elif resp['side'] == 'sell':
                side = -1
            else:
                side = 0

            temp_dict = {'ticker': self.cur,
                         'venue': self.src,
                         'seq': resp['trade_id'],
                         'tr_id': resp['sequence'],
                         'trade_ts': timestamp,
                         'tr_price': resp['price'],
                         'tr_size': float(resp['size']),
                         'tr_side': side
                         }

        else:
            # No suitable response
            return

        out_df = pd.DataFrame(temp_dict, index=[0])
        self.resp_to_db(out_df=out_df)

    def resp_to_db(self, out_df):
        """
        Write response to database
        :param out_df:
        :return:
        """
        self.db_con.df_to_sql_duplicate_update(out_df, db_name='TrainingData', db_table='CryptoTrades')

    async def get_resp(self, uri):
        async with websockets.connect(uri) as ws:
            await ws.send(self.req_str)

            while True:
                try:
                    resp_str = await asyncio.wait_for(ws.recv(), timeout=20)
                except asyncio.TimeoutError:
                    # No data in 20 seconds, check the connection.
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=10)
                    except asyncio.TimeoutError:
                        # No response to ping in 10 seconds, disconnect.
                        break
                else:
                    self.parse_resp(resp_str)

    def run(self, src, cur):

        self.src = src
        self.cur = cur

        if self.src == 'BFX':

            # Bitfinex
            self.req_str = r'{"event": "subscribe","channel": "trades","pair": "' + self.cur + '"}'
            ws_url = r'wss://api.bitfinex.com/ws'
        # Coinbase
        elif self.src == 'CB':
            # Format ticker in request to match Coinbase requirements
            cur_fmt = self.cur[:3] + '-' + self.cur[3:]
            # Coinbase
            self.req_str = """
            {{
                "type": "subscribe",
                "product_ids": [
                    "{}"
                ],
                "channels": [
                    "matches",
                    "heartbeat",
                    {{
                        "name": "ticker",
                        "product_ids": [
                            "{}"
                        ]
                    }}
                ]
            }}
            """.format(cur_fmt, cur_fmt)
            ws_url = r'wss://ws-feed.pro.coinbase.com'

        else:
            raise ValueError('Exchange not properly selected. Received: {}'.format(self.src))

        while True:
            try:
                asyncio.get_event_loop().run_until_complete(self.get_resp(ws_url))

            except CancelledError as e:
                print(e)

            except Exception as e:
                # Reset connection
                print(e)


if __name__ == "__main__":
    cur = sys.argv[1]
    src = sys.argv[2]

    b = CryptoWS()
    b.run(cur=cur, src=src)
