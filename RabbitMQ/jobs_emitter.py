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
from colorama import init, Fore, Back, Style # color printing
import requests, json
from requests.auth import HTTPBasicAuth
from messages import Messages #Jobs #, Results
from database import Database


common_delay = 2000


def getActiveWorkers(host, port, queue_name="history_jobs"):
    # Requests RabbitMQ server for active consumers
    # This block sometimes raises an exception when called too frequently
    try:
        req = f"http://{host}:{port}/api/consumers"
        consumers = requests.get(req, auth=HTTPBasicAuth("rabbit", "rabbit")).json()
        workers = [x['queue']['name'] for x in consumers if x['queue']['name']==queue_name]
    except Exception as e:
        print(f"Failure in getActiveWorkers():\n{Fore.RED}{e}{Fore.RESET}")
        workers = 0
    finally:    
        return workers



def main():
    try:
        init(convert=True) # colorama init    
        print("Jobs roundrobin emitter started.")
        filename = __file__.split(".")[0]
        #begin = time()
        logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
                                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')
        logging.info("Jobs roundrobin emitter started.")
        
        config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
        db = Database(config_ini)
        #sql = "select exchange, pair, ts from v_last_ts" #
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"
        sql = "select exchange, pair, ts from mem.history_cache with (snapshot)"

        jobs = Messages(type="jobs", config_ini=config_ini, exchange_name="history_jobs", queue_name="jobs")  #MessageBus()
        df = db.query(sql)
        if len(df)==0:
            raise ValueError("No jobs to run. (DataFrame is empty)")
        
        # spit out Dataframe line-by-line
        print("Waiting for workers...")
        while True:
            for index, row in df.iterrows():
                workers = getActiveWorkers(jobs.host, jobs.port, "history_jobs") #! get active RabbitMQ connections
                if len(workers) != 0:
                    msg = f"{index} - {row['exchange']}: {row['pair']}"
                    delay = common_delay/len(workers)/1000
                    jobs.send(message=msg, routing_key="history_jobs")
                    print(msg, " active workers =", len(workers), " delay =", delay)
                    sleep(delay)
                else:
                    break
            else:
                sleep(1)

        #elapsed = time() - begin
        #logging.info(f"Program finished in {elapsed:.4f} seconds.")
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()
