#!/usr/bin/env python3
import ccxt
import os, sys, time
import json, logging
import threading, zlib, pickle
# import asyncio
from os.path import basename
from ccxt import ExchangeError
from database import Database
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

markets = {}
histories = []

def load_exchange(exchange):
    # print(f"loading {exchange}")
    ex = getattr(ccxt, exchange)({
        'enableRateLimit': True,
        # 'verbose': True,
        # 'proxy':'https://cors-anywhere.herokuapp.com/',
    })
    try:
        ex.load_markets()
        markets[exchange] = ex
        print(f"{exchange} loaded.")
    except:
        print(f"{exchange} NOT loaded!")
    #await asyncio.sleep(0.01)
    

def fetch(db: Database, exchange: str, pair: str, limit, filename):
    # exchange, pair = row
    print(f"Fetching history and orderbook for {exchange}/{pair}")
    
    if exchange not in markets:
        print(f"Failed! {exchange} not in markets list")
        return

    market = markets[exchange]

    sql = f"select timestamp from v_last_ts where exchange='{exchange}' and pair='{pair}'"

    dt = datetime.utcnow() - relativedelta(months=1) # month ago from now
    since_db = 0
    try:
        since_db = int(db.query(sql).values[0])+1
    except:
        pass

    since_1m = int(dt.replace(tzinfo=timezone.utc).timestamp())*1000
    since = max(since_db, since_1m) # 

    # fetching history and orderbook
    ratelimit = market.rateLimit
    start = time.time()
    try:
        histories = market.fetch_trades(symbol=pair, since=since, limit=limit)
        orderbook = market.fetch_order_book(symbol=pair, )
        ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        if histories != []:
            try:
                for row in histories: # remove info row from result set
                    del row['info']
            except:
                pass
            finally:
                history = { 'exchange': exchange,
                            'pair': pair,
                            'histories': histories
                        }                    
                orderbook = {'exchange': exchange,
                            'pair': pair,
                            'timestamp': ts,
                            'orderbook': orderbook
                            }
        else:
            # search for acceptable since value
            # since += 1000 # increment by a second
            logging.info(f"fetch_trades({exchange}/{pair}) returned empty dataset, next ts={since}")
            return

    except ExchangeError: # Must specify a time window of no more than 1 month.
        pass
    except Exception as e:
        print(f"Error in {filename}.fetch_all(). {Fore.YELLOW}{e}{Fore.RESET}")
        logging.info(f"fetch_all({exchange}/{pair}) FAILED!")
    

    # compressing fetched results
    # start = time.time()
    data = pickle.dumps([histories, orderbook])
    #logging.info(f"{exchange}/{pair} before compression: {len(data)}")
    compressed_data = zlib.compress(data, 9)
    #elapsed = time.time() - start
    #logging.info(f"{exchange}/{pair} after compression: {len(compressed_data)}. Compressed in {elapsed:.4f} seconds")
    
    # -- BUS BACK SENDING CODE STARTS HERE
    # -- BUS BACK SENDING CODE ENDS HERE

    # decompressing fetched results
    #start = time.time()
    data = zlib.decompress(compressed_data)
    histories, orderbook  = pickle.loads(data)
    #elapsed = time.time() - start
    #logging.info(f"{exchange}/{pair} after decompression: {len(data)}. Decompressed in {elapsed:.4f} seconds")

    try:
        db.execute("mem.save_history_json", json.dumps(history))
        db.execute("mem.save_orderbook_json", json.dumps(orderbook))

    except Exception as e:
        print(f"Error in mem.save_history_json() or mem.save_orderbook_json(). {Fore.YELLOW}{e}{Fore.RESET}")
        logging.error(f"fetch({exchange}/{pair}) FAILED!")


    elapsed = time.time() - start
    delay = max(ratelimit - elapsed*1000, 0) + 0.1
    print(f"waiting {delay} seconds")
    time.sleep(delay)



def main():
    print("Fetch History demo")
    # os.chdir("ccxt")

    filename = os.path.splitext(basename(__file__))[0] + ".log"
    logging.basicConfig(filename=filename, level=logging.INFO, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
    #db = Database(os.getcwd() + "\\database.ini")
    db = Database.getInstance()
    #filename = os.path.splitext(__file__)[0] + ".log"
    
    pair = 'ETH/USDT'
    #exchanges_list = db.query(f"select distinct exchange from mem.exchanges_pairs with (snapshot) where enabled=1 and pair='{pair}'")['exchange'].tolist()
    exchanges_list = ['binance', 'exmo', 'bittrex', 'okex']

    threads = [threading.Thread(target=load_exchange, args=(exchange,)) for exchange in exchanges_list]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    [thread._stop() for thread in threads]
    threads = []

    print("All exchanges are loaded")

    # pairs = db.query("select exchange, pair from mem.exchanges_pairs with (snapshot) where enabled=1")
    limit = 100
    
    # for _, row in pairs.iterrows():

    while True:
        try:
            threads = [threading.Thread(target=fetch, args=(db, exchange, pair, limit, filename)) for exchange in exchanges_list]
            [thread.start() for thread in threads]
            [thread.join() for thread in threads]
            # [thread._stop() for thread in threads]
            #del threads

        except KeyboardInterrupt:
            print("Quit by Ctrl-C")
            # [thread._stop() for thread in threads]
            sys.exit()

if __name__ == '__main__':
    main()