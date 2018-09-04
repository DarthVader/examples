#!/usr/bin/env python3
''' ================
    Jobs emitter
    ================

    (x) loader_v2.py               | History loader
               |
               V
        Direct load history to Database
'''
import sys
import pandas as pd
import logging
from time import sleep, time
from pprint import pprint
from colorama import init, Fore, Back, Style # color printing
import requests, json, pickle
from requests.auth import HTTPBasicAuth
#from messages import Messages #Jobs #, Results
from database import Database
from markets import Markets

common_delay = 2000


def main():
    try:
        init(convert=True) # colorama init    
        print("Loader started.")
        # filename = __file__.split(".")[0]
        # begin = time()
        # logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
        #                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')

        
        config_ini = "emitter.ini" #__file__.split('.')[0]+'.ini'
        db = Database(config_ini)
        #sql = "select exchange, pair, ts from v_last_ts" #
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"

        market = Markets()
        ex_list = db.query("select id from dbo.exchanges where enabled=1").id.tolist()
        market.load_exchanges(ex_list)
        #with open("markets.ser", "wb") as f:
        #    json.dump(market, fp=f)

        sql = "select exchange, pair, [timestamp], ts from dbo.v_last_ts where exchange='binance'"
        # jobs = Messages(type="jobs", config_ini=config_ini, exchange_name="history_jobs", queue_name="jobs")  #MessageBus()
        
        # spit out Dataframe line-by-line
        # print("Waiting for workers...")
        while True:
            df = db.query(sql)
            if len(df)==0:
                raise ValueError("Database error. (DataFrame is empty)")

            for _, row in df.iterrows():
                #workers = getActiveWorkers(jobs.host, jobs.port, "history_jobs") #! get active RabbitMQ connections
                #if len(workers) != 0:
                exchange, pair, since, ts = row['exchange'], row['pair'], int(row['timestamp']), row['ts']
                print(f"Loading {exchange}/{pair} since {since} [{ts}]...", end='')

                try:
                    histories = market.fetch_trades(exchange=exchange, pair=pair, since=since+1, limit=100)
                    # histories = [x.pop('info') for x in histories] # delete all infos
                    if histories != []:
                        for row in histories:
                            del row['info']
                            # for v in row:
                            #     if row[v] is None:
                            #         del row[v]
                        
                        histories = {'exchange': exchange,
                                    'pair': pair,
                                    'histories': histories
                                    }
                        last_ts = histories['histories'][-1]['timestamp']
                        last_price = histories['histories'][-1]['price']
                        print(f"OK. {last_ts} Last price = {last_price}")

                        db.execute("mem.save_histories_json", json.dumps(histories))
                        print(f"Inserted {len(histories)} row(s)")

                except Exception as e:
                    print(f"FAILED.\n\n{e}\n")
                
                # msg = {'exchange': row['exchange'], 'pair': row['pair'], 'ts': row['timestamp']}
                # msg = f"{index} - {row['exchange']}: {row['pair']}"
                # msg = f"'exchange':'{row['exchange']}','pair':'{row['pair']}'"
                # msg = '"exchange:"{}"","pair":"{}"'.format(row['exchange'], row['pair'])
                # msg = f"\"exchange\":\"{row['exchange']}\",\"pair\":\"{row['pair']}\""
                # delay = common_delay/len(workers)/1000
                # jobs.send(message=json.dumps(msg), routing_key="history_jobs")
                # print(msg)
                # sleep(common_delay)

        #elapsed = time() - begin
        #logging.info(f"Program finished in {elapsed:.4f} seconds.")
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()
