#!/usr/bin/env python3

import sys, json
import pandas as pd
import logging
from time import sleep, time
from pprint import pprint
from database import Database
from markets.markets import Markets

def main():
    #config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
    markets = Markets()
    db = Database("database.ini")
    exchange = 'exmo'
    pair = 'EOS/BTC'
    markets.load_exchanges([exchange])

    # while True:
    try:
        histories = markets.fetch_trades(exchange=exchange, pair=pair)
        histories = [x.pop('info') for x in histories] # delete all infos
        histories = {'exchange': exchange,
                    'pair': pair,
                    'histories': histories
                    }

        # print(json.dumps(histories))
        rowcount = db.execute("dbo.save_histories_json", json.dumps(histories))
        print(f"Inserted {rowcount} row(s)")

    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

        # sleep(1)

    #df = pd.DataFrame(histories)
    #df.to_csv("100rows.csv", header=True, index=False)

if __name__ == "__main__":
    main()