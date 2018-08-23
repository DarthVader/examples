#!/usr/bin/python3.6

# markets.py
# High-level class for Markets data loading and management
__version__ = "1.2.0"

import os, sys, time
import asyncio
import ccxt as ccxt_s
import ccxt.async_support as ccxt
from ccxt.async_support import Exchange
from colorama import init, Fore, Back, Style # color printing
from datetime import datetime
import logging
# our imports
from messages import Messages #Jobs, Results

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
#from settings.settings import Settings
#from settings.settings import Settings
#from database.database import Database


class Timer:
    def __init__(self):
        self.start = time.time()

    def tic(self):
        return "%2.2f" % (time.time() - self.start)



class Markets:

    fiat = ['USD','EUR','JPY','UAH','USDT','RUB','CAD','NZDT']
    allowed_tsyms = ['USD', 'USDT', 'BTC', 'ETH', 'DOGE', 'LTC', 'EUR', 'RUB'] # allowed symbols for convertion to

    def __init__(self, results):
        self.exchanges = {}          #  exchanges - dict of ccxt objects
        self.exchanges_list = []     #  exchanges_list - custom list of exchanges to filter (lowercase)
        self.ex_pairs = {}           #  ex_pairs - dict of exchanges which contains corresponding trading pairs
        self.my_tokens = []          #  list of string values representing which token is allowed either on fsym or tsym
        self.last_fetch = {}         #  init dict with last fetches
        self._cache = []

        self.results = results # Messages(type="results", "emitter.ini", exchange_name="history_results", queue_name="history_results")
        #self.db_context = db_context #  database context
        #self._cache = db_context._cache  #  local cache for storing last access times to exchanges and pairs
        #self.config = Settings()
        #logging.basicConfig(filename=self.db_context.config.log_file, level=logging.INFO, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
        init(convert=True) # colorama init  
        print(f"CCXT version: {Fore.GREEN+Style.BRIGHT+ccxt.__version__+Style.RESET_ALL}")
        

    def _init_metadata(self, exchanges_list):
        """
        used in _load_exchanges method to create list of ccxt market instances - self.exchanges[]

            :param exchanges_list: : list(str) - list of exchanges (lowercase strings)
        """   
        self.exchanges_list = exchanges_list
        for id in exchanges_list:
            exchange = getattr(ccxt, id)
            # this option enables the built-in rate limiter <<=============
            exchange.enableRateLimit = True, 
            self.exchanges[id] = exchange()        


    def __del__(self):
        if self.exchanges != {}:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._shutdown())


    async def _shutdown(self):
        """ Deletes all instances in self.exchanges list """
        for ex in self.exchanges.items():
            print("Closing {}".format(ex[0]))
            await ex[1].close()


    async def _load_exchange(self, exchange: object):
        """
        Loads market metadata to exchange object, passed as parameter. Called from internal self._run_tasks() method.

            :param exchange: ccxt object instance
        """   
        #assert(self.exchanges != {}, "exchanges are not loaded!")
        await exchange.load_markets()
        print("{} market metadata loaded".format(exchange.name))


    async def _run_tasks(self, exchanges):
        """
        Executes ccxt load_market() in parallel manner using asyncio.

            :param exchanges: - list(object) list of ccxt exchange objects. ex[0] - string name of exchange, ex[1] - ccxt object
        """   
        #assert(self.exchanges != {}, "exchanges are not loaded!")
        tasks = []
        for ex in exchanges.items():
            tasks.append(asyncio.ensure_future(self._load_exchange(ex[1])))
        #await asyncio.gather(*tasks)
        await asyncio.wait(tasks)

    
    def load_exchanges(self, exchanges_list):
        """
        Loads exchanges metadata self.exchanges (list of ccxt objects)

            :param exchanges_list: - list(str) list of ids of exchanges (look up exchange id here: https://github.com/ccxt/ccxt)
        """
        if self.exchanges_list == []:
            self._init_metadata(exchanges_list) # init by exchanges list from settings.exchanges table
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self._run_tasks(self.exchanges))
            #loop.close()
        except KeyboardInterrupt:
            print("Leaving by Ctrl-C...")
            sys.exit()      


    def reload_pairs(self, my_tokens):
        """
        Returns dict of exchanges, corresponding pairs.

            :param my_tokens: list(str) - list of string values representing which token is allowed either on fsym or tsym
            self.exchanges - dict of ccxt objects
            self.exchanges_list - custom list of exchanges to filter (lowercase)
        """
        self.my_tokens = my_tokens
        for ex in self.exchanges_list:
            my_pairs = [sym.split('/') for sym in self.exchanges[ex].symbols 
                        if sym.split('/')[0] in my_tokens and sym.split('/')[1] in my_tokens]
            my_pairs = [x[0]+'/'+x[1] for x in my_pairs]
            self.ex_pairs[ex] = my_pairs



    def fetch_trades(self, exchange: str, pair: str, limit: int=100):
        """ 
        Fetches last N trades for given exchange and pair. SYNCHRONOUS version!

            :param exchange: string value of ccxt exchange id (look up exchange id here: https://github.com/ccxt/ccxt)
            :param pair: string value for exchange pair (for example "BTC/USDT")
            :param limit: integer value for how many rows to fetch from the exchange
        """
        ex_obj = getattr(ccxt_s, exchange)
        ex = ex_obj()
        #ex.load_markets()  # --------- WHAT FOR ???
        ex.enableRateLimit = True,
        histories = None
        try:
            if limit==None:
                histories = ex.fetch_trades(symbol=pair)
            else:
                histories = ex.fetch_trades(symbol=pair, limit=100)
        except Exception as e:
            print(f"Error in {__file__}.fetch_trades(). {Fore.YELLOW}{e}{Fore.RESET}")

        return histories


    async def _get_history(self, exchange, pairs):
        """ 
        Get historic data for MANY pairs from ONE single exchange 

            :param exchange: string value of ccxt exchange id. Ex: "hitbtc2" 
            :param pairs: list of string values for pairs. Ex. ["BTC/USDT", "ETH/BTC", "LTC/BTC"]

            (look up for full exchanges IDs list here: https://github.com/ccxt/ccxt)
        """
        pairs_txt = ", ".join(pairs)
        print(f"\t{Style.DIM}{datetime.now()} Requested {Fore.YELLOW}{exchange}{Style.RESET_ALL}: {Fore.BLUE}{pairs_txt}{Style.RESET_ALL}")
        for pair in pairs:
            try:
                timer = Timer()
                #rateLimit = self.exchanges[exchange].rateLimit
                since = self._cache[exchange][pair]
                #since = None
                if since == None:
                    histories = await self.exchanges[exchange].fetch_trades(pair, limit=100)
                else:
                    histories = await self.exchanges[exchange].fetch_trades(pair, since=since)

                fetched_rows = len(histories)
                #print(f"\t{Style.DIM}{datetime.now()} Received {Fore.YELLOW}{exchange}: {Fore.BLUE}{pair} {Fore.WHITE}({len(histories)} rows, {timer.tic()} seconds){Style.RESET_ALL}")
                logging.info(f"{exchange}, {pair} fetched {fetched_rows} rows in {timer.tic()} seconds")

                if histories != []:
                    ## SAVING LAST ACCESS TIME TO CACHE...
                    timer2 = Timer()
                    self._cache[exchange][pair] = histories[-1]['timestamp'] + 1
                    
                    ##  SAVING history TO DATABASE...
                    # batch_cql = []
                    # for x in histories:
                    #     batch_cql.append(f"INSERT INTO {self.config.data_keyspace}.{self.config.history_table} (exchange, pair, ts, id, price, amount, type, side, insert_date) VALUES " +
                    #     f"('{exchange}', '{pair}', {x['timestamp']}, '{x['id']}', {x['price']}, {x['amount']}, '{x['type']}', '{x['side']}', toTimestamp(now()) )")
                    #     #print(batch_cql)
                    # self.db_context.batch_execute(batch_cql)
                    # logging.info(f"{exchange}, {pair} saved {fetched_rows} rows in {timer2.tic()} seconds")
                    
                    #   SENDING history to RabbitMQ
                    # results.send(message=histories)

                    print(f"{exchange}, {pair} processed {fetched_rows} rows in {timer2.tic()} seconds")
                    logging.info(f"{exchange}, {pair} processed {fetched_rows} rows in {timer2.tic()} seconds")
                
                #print(f"\t{Style.DIM}{datetime.now()} Saved {exchange}: {pair} ({len(histories)} rows, {timer.tic()} seconds){Style.RESET_ALL}")
                print(f"\t{Style.DIM}{datetime.now()} Received and Saved {Fore.YELLOW}{exchange}: {Fore.BLUE}{pair} {Fore.WHITE}({len(histories)} rows, {timer.tic()} seconds){Style.RESET_ALL}")

            except Exception as e:
                print(f"Error in {__file__}._get_history(). {Fore.YELLOW}{e}{Fore.RESET}")
                logging.error(e)

            #finally:
            #    await asyncio.sleep(self.exchanges[exchange].rateLimit/1000)
                    #sleep(rateLimit/1000)

    
    async def _get_histories(self, job: str):
        """ 
        Fetches history in parallel manner 
        
        """
        try:
            exchanges = job.keys()
            tasks = []
            for exchange in exchanges:
                pairs = list(job[exchange]["pairs"])
                tasks.append(asyncio.ensure_future(self._get_history(exchange, pairs)))

        except Exception as e:
            print(f"Error in {__file__}._get_histories(). {Fore.YELLOW}{e}{Fore.RESET}")
                
        finally:
            await asyncio.gather(*tasks)


    def process_job(self, job: str, db_context):
        """
        Method collects historic and orderbook data from exchanges 
        and saves it to database given by context parameter db_context.
        Returns large chunk of collected data by each requested pair.

        :param db_context: reference to Database object
        :param job: must contain json dictionary with the follownig structure:

        Example of job parameter:
        {
            "exchange1": {
                "ratelimit": 3000,
                "pairs": ["BTC/USD", "BTC/ETH", "ETH/USD"],
                "timestamp": "2018-07-27 13:05:31",
            }
            "exchange2_id": {
                "ratelimit": 2000,
                "pairs": ["BTC/OMG", "BTC/ETH"],
                "timestamp": 1234567890,
            },...
        }
        """
        try:
            timer = Timer()
            if self.exchanges_list == {}:
                raise ValueError("Markets instance is not properly initialized! load_exchanges() must be called first!")
            
            logging.info(f"Job {job['timestamp']} added")

            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._get_histories(job['job']))
            print(f"{Fore.MAGENTA+Style.BRIGHT}\tJob completed in {Fore.WHITE}{timer.tic()}{Fore.MAGENTA} seconds{Style.RESET_ALL}")
            
            logging.info(f"Job {job['timestamp']} completed in {timer.tic()} seconds")

        except Exception as e:
            init(convert=True) # colorama init  
            print(f"{Fore.RED}{e}{Fore.RESET}")
        

if __name__ == '__main__':
    print("This file is not intened for direct execution")