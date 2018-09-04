#!/usr/bin/env python3
import ccxt, os
from database import Database
from tabulate import tabulate
from colorama import Fore
from datetime import datetime, timezone

def main():
    os.chdir("ccxt")
    # db = Database(os.getcwd() + "\\database.ini")
    # ex_list = db.query("select id from exchanges where enabled=1").id.tolist()

    exchanges = ['binance','bittrex','huobi','cryptopia','exmo','hitbtc2','kraken','okex','poloniex','yobit']
    #exchange = "binance"
    pair = "ETH/USDT"
    limit = 10

    dt = datetime.strptime('13.08.2018 15:30:25', '%d.%m.%Y %H:%M:%S')
    ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
    since = ts*1000
    print(f"Selected pair: {pair}, start date: {dt.strftime('%d.%m.%Y %H:%M:%S')} [since={since}]")

    for exchange in exchanges:
        # sql = f"select timestamp from v_last_ts where exchange='{exchange}' and pair='{pair}'"
        # since = int(db.query(sql).values[0])+1
        print(f"{exchange}. ", end="")

        #ex = ccxt.binance()
        ex_obj = getattr(ccxt, exchange)
        ex = ex_obj()
        ex.enableRateLimit = True
        ex.load_markets()

        try:
            histories = ex.fetch_trades(symbol=pair, since=since, limit=limit)
            for row in histories: # remove info row from result set
                del row['info']
            print(f"dt=[{histories[0]['datetime']}], ts=[{histories[0]['timestamp']}], price={histories[0]['price']}")
            #print(tabulate(histories, headers="keys", tablefmt="fancy_grid"))

        except Exception as e:
            print(f"Error in {__file__}.fetch_trades(). {Fore.YELLOW}{e}{Fore.RESET}")

if __name__ == '__main__':
    main()