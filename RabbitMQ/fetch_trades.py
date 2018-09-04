#!/usr/bin/env python3
import ccxt
from database import Database
from tabulate import tabulate
from colorama import Fore

def main():
    db = Database("database.ini")
    # ex_list = db.query("select id from exchanges where enabled=1").id.tolist()

    exchange = "okex"
    pair = "ETH/USDT"
    
    sql = f"select timestamp from v_last_ts where exchange='{exchange}' and pair='{pair}'"
    since = int(db.query(sql).values[0])+1
    limit = 50

    #ex = ccxt.binance()
    ex_obj = getattr(ccxt, exchange)
    ex = ex_obj()
    ex.enableRateLimit = True
    ex.load_markets()

    try:
        histories = ex.fetch_trades(symbol=pair, since=since, limit=limit)
        for row in histories:
            del row['info']
        print(tabulate(histories, headers="keys", tablefmt="fancy_grid"))

    except Exception as e:
        print(f"Error in {__file__}.fetch_trades(). {Fore.YELLOW}{e}{Fore.RESET}")

if __name__ == '__main__':
    main()   