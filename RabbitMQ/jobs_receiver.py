#!/usr/bin/env python3
''' ================
    Jobs receiver
    ================

        jobs_emitter.py         | Dispatcher server
               |
               V
            RabbitMQ            | message queue bus
            |  |  |
            V  V  V
    (x) jobs_receiver.py        |       Workers
        results_emitter.py      | (runs on multiple nodes)
            |  |  |
            V  V  V
            RabbitMQ            | message queue bus
               |
               V
        results_receiver.py
'''

import sys
from messages import Jobs
import argparse

def main():
    print("Jobs receiver started.")
    #ap = argparse.ArgumentParser()
    #ap.add_argument("-e", "--exchange", required=True, help="Exchange name") # history_jobs
    #ap.add_argument("-q", "--queue", required=True, help="Queue name") # jobs
    #ap.add_argument("-k", "--routing_key", required=False, help="Routing key")
    #args = vars(ap.parse_args())

    #exchange = args["exchange"]
    exchange = "history_jobs"
    #queue_name = args["queue"] # jobs
    queue_name = "history_jobs"
    #routing_key = args["routing_key"]

    jobs = Jobs(config_ini="emitter.ini", exchange_name=exchange, queue_name=queue_name)
    #print(f"Exchange: {exchange}, queue: {queue_name}, routing_key: {routing_key}. Receiving messages...")
    print(f"Exchange: {exchange}, queue: {queue_name}.\nReceiving messages...")
    
    try:
        jobs.receive()
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()