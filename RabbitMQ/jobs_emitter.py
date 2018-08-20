#!/usr/bin/env python3
''' ================
    Jobs emitter
    ================

    (x) jobs_emitter.py         | Dispatcher server
               |
               V
            RabbitMQ            | message queue bus
            |  |  |
            V  V  V
        jobs_receiver.py        |        Workers
        results_emitter.py      | (runs on multiple nodes)
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
from messages import Jobs #, Results
from database import Database


def main():
    try:
        print("Jobs roundrobin emitter started.")
        filename = __file__.split(".")[0]
        begin = time()
        logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
                                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')
        logging.info("Jobs roundrobin emitter started.")
        
        config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
        db = Database(config_ini)
        #sql = "select exchange, pair, ts from v_last_ts" #
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"
        sql = "select exchange, pair, ts from mem.history_cache with (snapshot)"

        jobs = Jobs(config_ini=config_ini, exchange_name="history_jobs", queue_name="jobs")  #MessageBus()
        df = db.query(sql)
        if len(df)==0:
            raise ValueError("There is no jobs to emit! (dataFrame is empty)")
        
        # spit out Dataframe line-by-line
        for index, row in df.iterrows():
            msg = f"{index} - {row['exchange']}: {row['pair']}"
            print(msg)
            jobs.send(message=msg, routing_key="history_jobs")
            
            sleep(2)

        elapsed = time() - begin
        logging.info(f"Program finished in {elapsed:.4f} seconds.")
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()
