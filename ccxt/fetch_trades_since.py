#!/usr/bin/env python3
import ccxt, os
from ccxt import ExchangeError
from database import Database
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

def main():
    os.chdir("ccxt")
    db = Database(os.getcwd() + "\\database.ini")
    exchanges = db.query("select id from exchanges where enabled=1").id.tolist()

    # exchanges = ['binance','huobipro','bittrex','cryptopia','exmo','hitbtc2','kraken','okex','poloniex','yobit']
    pair = "ETH/USDT"
    limit = 10

    # dt = datetime.strptime('01.09.2018 15:30:25', '%d.%m.%Y %H:%M:%S')
    # ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
    # since = ts *1000
    print(f"Selected pair: {pair}")
    # print(f"Selected pair: {pair}, start date: {dt.strftime('%d.%m.%Y %H:%M:%S')} [since={since}]\n")

    for exchange in exchanges:
        sql = f"select timestamp from v_last_ts where exchange='{exchange}' and pair='{pair}'"
        since = int(db.query(sql).values[0])+1
        print(f"{exchange}. ", end="")

        #ex = ccxt.binance()
        ex_obj = getattr(ccxt, exchange)
        ex = ex_obj()
        ex.enableRateLimit = True
        ex.load_markets()

        try:
            histories = ex.fetch_trades(symbol=pair, since=since, limit=limit)
            if histories == []:
                dt = datetime.utcnow() - relativedelta(months=1) # month ago from now
                since = int(dt.replace(tzinfo=timezone.utc).timestamp())*1000
                histories = ex.fetch_trades(symbol=pair, since=since, limit=limit)
            #print(tabulate(histories, headers="keys", tablefmt="fancy_grid"))
            
        except ExchangeError:
            pair = pair.split("/")[0]+"/USD" if pair.split("/")[1]=="USDT" else pair.split("/")[0]+"/USDT"
            histories = ex.fetch_trades(symbol=pair, since=since, limit=limit)
            
        except Exception as e:
            print(f"Error in {__file__}.fetch_trades(). {Fore.YELLOW}{e}{Fore.RESET}")
        
        finally:
            if histories != []:
                for row in histories: # remove info row from result set
                    del row['info']
            else:
                # search for acceptable since value
                since += 1000 # increment by a second

            status = "WORKS" if str(histories[0]['timestamp'])[:4] == str(since)[:4] else "DOESN'T WORK!"
            print(f"dt=[{histories[0]['datetime']}], ts=[{histories[0]['timestamp']}], price={histories[0]['price']} -- {status}")
            histories = []

if __name__ == '__main__':
    main()