#!/usr/bin/env python3

import sys
import pandas as pd
import logging
from time import sleep, time
from pprint import pprint
from messages import Jobs #, Results
from database import Database


def main():
    try:
        filename = __file__.split(".")[0]
        begin = time()
        logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
                                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')
        print("Roundrobin emitter started.")
        logging.info("Roundrobin emitter started.")
        
        config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
        db = Database(config_ini)
        #sql = "select exchange, pair, ts from v_last_ts" #
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"
        sql = "select exchange, pair, ts from last_history_cache with (snapshot)"

        results = Jobs(config_ini=config_ini, exchange_name="history_jobs", queue_name="jobs") #MessageBus()
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
