#!/usr/bin/env python3
''' ================
    Results receiver
    ================

        jobs_emitter.py         | Dispatcher server
               |
               V
            RabbitMQ            | message queue bus
            |  |  |
            V  V  V
        jobs_receiver.py        |       Workers
        results_emitter.py      | (runs on multiple nodes)
            |  |  |
            V  V  V
            RabbitMQ            | message queue bus
               |
               V
    (x) results_receiver.py

    Results receiver is meant to be runnig as close as possible to Database server.
'''

import sys
from messages import Messages #Results
import argparse

def main():
    print("Results receiver started.")
    ap = argparse.ArgumentParser()
    #ap.add_argument("-e", "--exchange", required=True, help="Exchange name")
    ap.add_argument("-q", "--queue", required=True, help="Queue name")
    #ap.add_argument("-k", "--routing_key", required=False, help="Routing key")
    args = vars(ap.parse_args())

    #exchange = args["exchange"]
    exchange = "history_results"
    queue_name = args["queue"] # any name. For example "queue1"
    #routing_key = args["routing_key"]

    results = Messages(type="results", config_ini="rabbitmq.ini", exchange_name=exchange, queue_name=queue_name)
    #print(f"Exchange: {exchange}, queue: {queue_name}, routing_key: {routing_key}. Receiving messages...")
    print(f"Exchange: {exchange}, queue: {queue_name}.\nReceiving messages...")
    
    try:
        results.receive()
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()