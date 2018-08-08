#!/usr/bin/env python3
import sys
from messages import Results
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-q", "--queue_name", required=True, help="Unique queue name")
    args = vars(ap.parse_args())

    queue_name=args["queue_name"]
    results = Results(config_ini="emitter.ini", queue_name=queue_name)
    print(f"{queue_name} is receiving messages...")
    
    try:
        results.receive()
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()