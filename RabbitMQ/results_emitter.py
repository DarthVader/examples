#!/usr/bin/env python3
''' ================
    Results emitter
    ================

        jobs_emitter.py         | Dispatcher server
               |
               V
            RabbitMQ            | message queue bus
            |  |  |
            V  V  V
        jobs_receiver.py        |       Workers
    (x) results_emitter.py      | (runs on multiple nodes)
            |  |  |
            V  V  V
            RabbitMQ            | message queue bus
               |
               V
        results_receiver.py
'''

import sys
import pandas as pd
import logging
from time import sleep, time
from pprint import pprint
from messages import Results
from database import Database


def main():
    try:
        print("Fanout results emitter started.")
        filename = __file__.split(".")[0]
        begin = time()
        logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
                                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')
        logging.info("Fanout results emitter started.")
        
        config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
        db = Database(config_ini)
        #sql = "select exchange, pair, ts from v_last_ts" #
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"
        sql = "select exchange, pair, ts from mem.history_cache with (snapshot)"

        results = Results(config_ini=config_ini, 
                            exchange_name="history_results", 
                            queue_name="results") #MessageBus()
        df = db.query(sql)
        if len(df)==0:
            raise ValueError("DataFrame is empty!")
        
        # spit out Dataframe line-by-line
        for index, row in df.iterrows():
            msg = f"{index} - {row['exchange']}: {row['pair']}"
            print(msg)
            results.send(message=msg)
            
            sleep(2)

        elapsed = time() - begin
        logging.info(f"Program finished in {elapsed:.4f} seconds.")
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()
