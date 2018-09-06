#!/usr/bin/env python3
import ccxt
from ccxt import ExchangeError
import os, json, logging
import pandas as pd
from database import Database
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta


def main():
    print("Fetch OrderBook demo")
    os.chdir("ccxt")

    filename = os.path.splitext(__file__)[0] + ".log"
    logging.basicConfig(filename=filename, level=logging.INFO, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
    db = Database(os.getcwd() + "\\database.ini")
    pairs = db.query("select exchange, pair from mem.exchanges_pairs with (snapshot) where enabled=1")

    # exchanges = ['binance','huobipro','bittrex','cryptopia','exmo','hitbtc2','kraken','okex','poloniex','yobit']
    # limit = 100

    for _, row in pairs.iterrows():
        exchange, pair = row
        print(f"Fetching orderbook for {exchange}/{pair}...")
        logging.info(f"{exchange}/{pair} fetch_order_book started")

        ex_obj = getattr(ccxt, exchange)
        ex = ex_obj()
        ex.enableRateLimit = True
        ex.load_markets()

        try:
            dt = datetime.utcnow()
            ts = int(dt.replace(tzinfo=timezone.utc).timestamp())

            orderbook = ex.fetch_order_book(symbol=pair)
            orderbook = {'exchange': exchange,
                         'pair': pair,
                         'timestamp': ts,
                         'orderbook': orderbook
                        }
            # bids = orderbook['bids']
            # asks = orderbook['asks']
            # size = min(len(bids), len(asks))

            # cols = ['timestamp','exchange','pair','bids amount','bids price','asks amount','asks price']

            # df = pd.DataFrame({
            #                    'timestamp': ts,
            #                    'exchange': exchange,
            #                    'pair': pair,
            #                    'bids amount': [x[1] for x in bids[:size]],
            #                    'bids price': [x[0] for x in bids[:size]], 
            #                    'asks price': [x[0] for x in asks[:size]], 
            #                    'asks amount': [x[1] for x in asks[:size]]
            #                    })[cols]
            # print(tabulate(df, tablefmt="pipe"))
            print(json.dumps(orderbook))
            #db.execute("mem.save_orderbook_json", json.dumps(orderbook))
            
        except ExchangeError:
            print("FAILED!")
            logging.info("{exchange}/{pair} fetch_order_book failed!")
            
        except Exception as e:
            print(f"Error in fetch_order_book(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.info("{exchange}/{pair} fetch_order_book failed!")
        
        finally:
            orderbook = []

if __name__ == '__main__':
    main()