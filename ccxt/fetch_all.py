#!/usr/bin/env python3
import ccxt
import os, json, asyncio, logging
from os.path import basename
from ccxt import ExchangeError
from database import Database
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

markets = {}
histories = []

async def load_exchange(exchange):
    #print(f"loading {exchange}")
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
    await asyncio.sleep(0.01)


def main():
    print("Fetch History demo")
    os.chdir("ccxt")

    filename = os.path.splitext(basename(__file__))[0] + ".log"
    logging.basicConfig(filename=filename, level=logging.ERROR, format=u'%(filename)s:%(lineno)d %(levelname)-8s [%(asctime)s]  %(message)s')
    db = Database(os.getcwd() + "\\database.ini")
    #filename = os.path.splitext(__file__)[0] + ".log"

    exchanges_list = db.query("select distinct exchange from mem.exchanges_pairs with (snapshot) where enabled=1")['exchange'].tolist()

    tasks = [load_exchange(exchange) for exchange in exchanges_list]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

    pairs = db.query("select exchange, pair from mem.exchanges_pairs with (snapshot) where enabled=1")
    limit = 100

    for _, row in pairs.iterrows():
        exchange, pair = row
        print(f"Fetching history and orderbook for {exchange}/{pair}")
        
        if exchange not in markets:
            continue

        market = markets[exchange]

        sql = f"select timestamp from v_last_ts where exchange='{exchange}' and pair='{pair}'"

        dt = datetime.utcnow() - relativedelta(months=1) # month ago from now
        try:
            since_db = int(db.query(sql).values[0])+1
        except:
            pass
        since_1m = int(dt.replace(tzinfo=timezone.utc).timestamp())*1000
        since = max(since_db, since_1m) # 

        try:
            histories = market.fetch_trades(symbol=pair, since=since, limit=limit)
            orderbook = market.fetch_order_book(symbol=pair, )
            ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
            orderbook = {'exchange': exchange,
                         'pair': pair,
                         'timestamp': ts,
                         'orderbook': orderbook
                        }

        except ExchangeError: # Please specify a time window of no more than 1 month.
            pass

        except Exception as e:
            print(f"Error in {filename}.fetch_all(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.info(f"fetch_all({exchange}/{pair}) FAILED!")
        
        if histories != []:
            try:
                for row in histories: # remove info row from result set
                    del row['info']
            except:
                pass
            history = { 'exchange': exchange,
                        'pair': pair,
                        'histories': histories
                       }            
            try:
                db.execute("mem.save_history_json", json.dumps(history))

            except Exception as e:
                print(f"Error in {filename}.mem.save_history_json(). {Fore.YELLOW}{e}{Fore.RESET}")
                logging.error(f"fetch_trades({exchange}/{pair}) FAILED!")
        else:
            # search for acceptable since value
            since += 1000 # increment by a second
            logging.info(f"fetch_trades({exchange}/{pair}) returned empty dataset, next ts={since}")

        try:
            db.execute("mem.save_orderbook_json", json.dumps(orderbook))

        except ExchangeError:
            print("FAILED!")
            logging.error(f"{exchange}/{pair} fetch_order_book failed!")

        except Exception as e:
            print(f"Error in fetch_order_book(). {Fore.YELLOW}{e}{Fore.RESET}")
            logging.info(f"{exchange}/{pair} fetch_order_book failed!")


if __name__ == '__main__':
    main()