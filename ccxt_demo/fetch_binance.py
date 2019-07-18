#!/usr/bin/env python3
import ccxt
import os, sys, time
import json, logging, zlib, pickle
import threading
from threading import Lock
# import multiprocessing as mp
# import asyncio
from os.path import basename
from ccxt import ExchangeError
from database import Database
from tabulate import tabulate
from colorama import Fore, Style, init
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

markets = {}
histories = []
sinces = {}

lock = Lock()

def tprint(*args, **kwargs):
    ''' Threaded print - printing in multi-threaded environement using simple lock
        Please add following lines to your code:
        from threading import Lock
        lock = Lock() # global lock
    '''
    lock.acquire()
    print(*args, **kwargs)
    lock.release()


class LoadExchange(threading.Thread):
    def __init__(self, exchange):
        threading.Thread.__init__(self)
        self.exchange = exchange

    def run(self):
        # print(f"loading {exchange}")
        exchange = self.exchange
        ex = getattr(ccxt, exchange)({
            'enableRateLimit': True,
            # 'verbose': True,
            # 'proxy':'https://cors-anywhere.herokuapp.com/',
        })
        try:
            ex.load_markets()
            markets[exchange] = ex
            tprint(f"{exchange} loaded.")
        except:
            tprint(f"{exchange} NOT loaded!")
        #await asyncio.sleep(0.01)
    

class Fetch(threading.Thread):

    def __init__(self, db: Database, exchange: str, pair: str, limit: int, filename: str):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        self.db = db
        self.exchange = exchange
        self.pair = pair
        self.limit = limit
        self.filename = filename
        

    def run(self):
        # exchange, pair = row
        start = time.time()

        db, exchange, pair, limit, filename  = self.db, self.exchange, self.pair, self.limit, self.filename
        
        tprint(f"Fetching history and orderbook for {exchange}/{pair}")
        
        if exchange not in markets:
            tprint(f"Failed! {exchange} not in markets list")
            return

        market = markets[exchange]
        
        # fetching history and orderbook
        histories = []
        history = {}
        orderbook = {}        
        ratelimit = market.rateLimit

        sql = f"select timestamp, from_id from dbo.v_last_ts where exchange='{exchange}' and pair='{pair}'"
        dt = datetime.utcnow() - relativedelta(months=1) # ago from now

        since_db = 0
        last_since = 0
        since_default = 0
        #since_default = int(dt.replace(tzinfo=timezone.utc).timestamp())*1000+1
        #since_default = market.milliseconds() - 3*86400000 # 3 days from current moment

        try:
            since_db, from_id = db.query(sql).values[0]
            since_db = int(since_db*1000) + 1 if since_db != None else since_default
            from_id = int(from_id) if from_id != None else None

            # BUG workaround: binance or okex2 tend to stuck on same SINCE from time to time
            if exchange in sinces and pair in sinces[exchange]:
                if sinces[exchange][pair] >= since_db:
                    sinces[exchange][pair] += 60000 # increment SINCE by one minute if stuck
                    since_default = sinces[exchange][pair]

        except Exception as e:
            tprint(f"Error occured while getting since value for {exchange}/{pair}. {filename}.fetch_all(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.info(f"Error occured while getting since value for {exchange}/{pair}")

        since = max(since_db, since_default) # 
        
        # params = {
        #     'from_id': str(from_id),  # exchange-specific non-unified parameter name
        # }

        try:
            ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
            #if from_id == None:
            histories = market.fetchTrades(symbol=pair, since=since, limit=limit) #, params={'from_id': from_id}
            if len(histories) == 0:
                tprint(f"{Fore.RED}Fetched history for {exchange}/{pair} is empty, using next SINCE={since} [{datetime.utcfromtimestamp(since/1000).strftime('%Y-%m-%d %H:%M:%S')}] {Fore.RESET}")
            sinces[exchange] = {}
            sinces[exchange][pair] = since
            #else:
            #    histories = market.fetch_trades(pair, since, limit, params)
            orderbook = market.fetch_order_book(symbol=pair, )
            
            if len(histories)>0:
                last_since = int(histories[-1]['timestamp'])
                #last_dt = histories[-1]['datetime']

            elif last_since != since: # histories != []:
                for row in histories: # remove info row from result set
                    del row['info']

            else:
                # search for acceptable since value
                # since += 1000 # increment by a second
                logging.info(f"fetch_trades({exchange}/{pair}) returned empty dataset, next ts={since}")
                # return

        except ExchangeError as e: # Must specify a time window of no more than 1 month.
            tprint(f"ExchangeError in {exchange}/{pair}. {Fore.YELLOW}{e}{Fore.RESET}")
            logging.error(f"ExchangeError exception in fetch_all:run({exchange}/{pair})")
            pass

        except Exception as e:
            tprint(f"Error in processing {exchange}/{pair}. {filename}.fetch_all(). {Fore.RED}{e}{Fore.RESET}")
            logging.error(f"fetch_all({exchange}/{pair}) FAILED!")

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

        # compressing fetched results
        # start = time.time()
        #data = pickle.dumps([histories, orderbook]) ## <<----
        # logging.info(f"{exchange}/{pair} before compression: {len(data)}")
        #compressed_data = zlib.compress(data, 9) ## <<----
        
        #elapsed = time.time() - start
        #logging.info(f"{exchange}/{pair} after compression: {len(compressed_data)}. Compressed in {elapsed:.4f} seconds")
        
        # -- BUS SENDING CODE STARTS HERE
        # -- BUS SENDING CODE ENDS HERE

        # -- BUS RECEIVING CODE STARTS HERE
        # -- BUS RECEIVING CODE ENDS HERE

        # decompressing fetched results
        #start = time.time()
        
        #data = zlib.decompress(compressed_data) ## <<----
        #histories, orderbook  = pickle.loads(data) ## <<----

        #elapsed = time.time() - start
        #logging.info(f"{exchange}/{pair} after decompression: {len(data)}. Decompressed in {elapsed:.4f} seconds")
        # logging.info(json.dumps(history))

        try:
            db.execute("dbo.save_order_book_json", json.dumps(orderbook))
            if histories != []:
                db.execute("mem.save_history_json", json.dumps(history))

        except Exception as e:
            tprint(f"Error in mem.save_history_json() or mem.save_order_book_json(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.error(f"fetch({exchange}/{pair}) FAILED!")


        elapsed = time.time() - start
        delay = 0.0 + max(ratelimit/1000 - elapsed, 0)
        
        tprint(f"[{datetime.now()}] {exchange}/{pair}: Ratelimit={ratelimit}, elapsed={elapsed:.4f} seconds") # , since={since}, last_since={last_since}, last_dt={last_dt}")
        
        time.sleep(delay)

    def stop(self):
        self._stop_event.set()
    
    def stopped(self):
        return self._stop_event.is_set()



def main():
    init(convert=True)  # colorama init 
    print("Fetch History demo")
    print(f"CCXT version: {ccxt.__version__}")
    # os.chdir("ccxt")

    filename = os.path.splitext(basename(__file__))[0] + ".log"
    logging.basicConfig(filename=filename, level=logging.INFO, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
    #db = Database(os.getcwd() + "\\database.ini")
    path = os.getcwd()
    if "ccxt_demo" not in path:
        path += "/ccxt_demo"
    db = Database.getInstance(config_ini=f"{path}/database/database.ini")
    #filename = os.path.splitext(__file__)[0] + ".log"
    
    pair = 'ETH/USDT'
    # sql = f"select distinct exchange from mem.exchanges_pairs with (snapshot) where enabled=1"
    # exchanges_list = db.query(sql)['exchange'].tolist()
    # exchanges_list = ['binance', 'exmo', 'bittrex', 'okex', 'hitbtc2', 'cryptopia', 'poloniex']
    # exchanges_list = ['binance', 'exmo', 'okex', 'poloniex']
    # exchanges_list = ['hitbtc2']
    # exchanges_list = ['kraken']
    exchanges_list = ['binance']
    
    start = time.time()
    threads = [LoadExchange(exchange) for exchange in exchanges_list]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    [thread._stop() for thread in threads]
    threads = []
    elapsed = time.time() - start
    print("=============================================")
    print(f"Loading completed in {elapsed:.4f} seconds")
    print("=============================================")
    # pairs = db.query("select exchange, pair from mem.exchanges_pairs with (snapshot) where enabled=1")
    # for _, row in pairs.iterrows():
    limit = 100
    while True:
        try:
            start = time.time()
            threads = [Fetch(db, exchange, pair, limit, filename) for exchange in exchanges_list]
            [thread.start() for thread in threads]
            [thread.join() for thread in threads]
            [thread.stop() for thread in threads]
            elapsed = time.time() - start
            del threads
            print(f"Fetching completed in {Fore.YELLOW+Style.BRIGHT}{elapsed:.4f}{Style.RESET_ALL} seconds")
            print("=============================================")

        except KeyboardInterrupt:
            #[thread.join() for thread in threads]
            print("\nQuit by Ctrl-C\n")
            sys.exit()


if __name__ == '__main__':
    main()