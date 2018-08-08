#!/usr/bin/env python3
import sys
from messages import Results
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--exchange", required=True, help="Exchange name")
    ap.add_argument("-q", "--queue", required=True, help="Queue name")
    #ap.add_argument("-k", "--routing_key", required=False, help="Routing key")
    args = vars(ap.parse_args())

    exchange = args["exchange"]
    queue_name = args["queue"]
    #routing_key = args["routing_key"]

    results = Results(config_ini="emitter.ini", exchange_name=exchange, queue_name=queue_name)
    #print(f"Exchange: {exchange}, queue: {queue_name}, routing_key: {routing_key}. Receiving messages...")
    print(f"Exchange: {exchange}, queue: {queue_name}.\nReceiving messages...")
    
    try:
        results.receive()
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()