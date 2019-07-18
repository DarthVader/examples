#!/usr/bin/env python3
''' ================
    Worker
    ================

        jobs_emitter.py         | Dispatcher server
               |
               V
            RabbitMQ            | message queue bus
            |  |  |
            V  V  V
    (x)     worker.py           | workers (receives job, fetches results and resends back to message bus)
            |  |  |
            V  V  V
            RabbitMQ            | message queue bus
               |
               V
        results_receiver.py
'''

import sys
import pandas as pd
from messages import Messages
from markets import Markets
from database import Database #! remove ASAP!
import argparse

def main():
    print("Jobs receiver started.")
    #ap = argparse.ArgumentParser()
    #ap.add_argument("-e", "--exchange", required=True, help="Exchange name") # history_jobs
    #ap.add_argument("-q", "--queue", required=True, help="Queue name") # jobs
    #ap.add_argument("-k", "--routing_key", required=False, help="Routing key")
    #args = vars(ap.parse_args())

    #exchange = args["exchange"]
    #queue_name = args["queue"] # jobs
    #routing_key = args["routing_key"]
    config_ini = "emitter.ini"
    job     = Messages(type="jobs", config_ini=config_ini, exchange_name= "history_jobs", queue_name="history_jobs")
    results = Messages(type="results", config_ini=config_ini, exchange_name="history_results", queue_name="history_results") #MessageBus()    
    market = Markets()

    db = Database("database.ini")
    ex_list = db.query("select id from exchanges where enabled=1").id.tolist()
    market.load_exchanges(ex_list)

    #print(f"Exchange: {exchange}, queue: {queue_name}, routing_key: {routing_key}. Receiving messages...")
    print(f"Exchange: history_jobs, queue: history_jobs.\nReceiving messages...")
    
    try:
        callbacks = [ market, results ]
                    #  {'function': results.send,        'params': {'routing_key': 'history_results'} ]
        job.receive(callbacks)
        # if job.message != "":
        #     print(f"Resending {job.message} from history_job to history_results)")
        #     #results.send(job.message)
        #     job.message = ""
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()