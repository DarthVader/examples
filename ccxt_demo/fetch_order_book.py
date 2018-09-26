#!/usr/bin/env python3
import ccxt
from ccxt import ExchangeError
import asyncio, os, json, logging
from os.path import basename
import pandas as pd
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

from database import Database

markets = {}

async def load_exchange(exchange):
    #print(f"loading {exchange}")
    ex_obj = getattr(ccxt, exchange)
    ex = ex_obj()
    ex.enableRateLimit = True
    try:
        ex.load_markets()
        markets[exchange] = ex
        print(f"{exchange} loaded.")
    except:
        print(f"{exchange} NOT loaded!")
    await asyncio.sleep(0.01)


def main():
    print("Fetch OrderBook demo")
    os.chdir("ccxt")

    filename = os.path.splitext(basename(__file__))[0] + ".log"
    logging.basicConfig(filename=filename, level=logging.INFO, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
    db = Database(os.getcwd() + "\\database.ini")
    
    exchanges_list = db.query("select distinct exchange from mem.exchanges_pairs with (snapshot) where enabled=1")['exchange'].tolist()

    tasks = [load_exchange(exchange) for exchange in exchanges_list]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

    pairs = db.query("select exchange, pair from mem.exchanges_pairs with (snapshot) where enabled=1")
    # limit = 100

    for _, row in pairs.iterrows():
        exchange, pair = row
        print(f"Fetching orderbook for {exchange}/{pair}")
        # logging.info(f"{exchange}/{pair} fetch_order_book()")

        market = markets[exchange]

        try:
            dt = datetime.utcnow()
            ts = int(dt.replace(tzinfo=timezone.utc).timestamp())

            orderbook = market.fetch_order_book(symbol=pair)
            orderbook = {'exchange': exchange,
                         'pair': pair,
                         'timestamp': ts,
                         'orderbook': orderbook
                        }

            db.execute("mem.save_orderbook_json", json.dumps(orderbook))
            
        except ExchangeError:
            print("FAILED!")
            logging.info(f"{exchange}/{pair} fetch_order_book failed!")
            
        except Exception as e:
            print(f"Error in fetch_order_book(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.info(f"{exchange}/{pair} fetch_order_book failed!")
        
        finally:
            orderbook = []

if __name__ == '__main__':
    main()