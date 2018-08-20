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
    markets.load_exchanges(['exmo'])
    histories = markets.fetch_trades(exchange='exmo', pair='ETH/USDT')
    histories = [x.pop('info') for x in histories] # delete all infos
        
    #print(json.dumps(histories))
    df = pd.DataFrame(histories)
    df.to_csv("100rows.csv", header=True, index=False)

if __name__ == "__main__":
    main()