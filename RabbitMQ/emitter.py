#!/usr/bin/env python3

import sys
import sqlalchemy as sa
import pandas as pd
import logging, time
from time import sleep
from configparser import ConfigParser
from pprint import pprint
import pika # RabbitMQ
from messages import Results


class Database():
    """
    Arbitron MSSQL database interaction
    """

    def __init__(self): #, server, database, user, password):

        try:
            self.config = ConfigParser()
            self.config.read(__file__.split('.')[0]+'.ini')

            self.server = self.config['mssql']['server_address']
            self.database = self.config['mssql']['database_name']
            self.user = self.config['mssql']['user_name']
            self.password = self.config['mssql']['password']

            logging.info("Connecting to database...")
            start = time.time()
            self.connection_string = f"mssql+pymssql://{self.user}:{self.password}@{self.server}/{self.database}"
            self.engine = sa.create_engine(self.connection_string)
            self.connection = self.engine.connect()  # -- открываем соединение, пока открыто - висит блокировка. Закрытие по con.Close()
            elapsed = time.time() - start
            logging.info(f"Connected in {elapsed:.4f} seconds")

        except Exception as e:
            print(e)
            sys.exit()


    def query(self, sql):
        """
        Выполняет запрос к базе данных MSSQL, возвращает Pandas Dataframe
        """
        try:
            logging.info("Starting query...")
            start = time.time()
            df = pd.read_sql_query(sql, self.connection)  # выполняем sql запрос и записываем результат в pandas dataframe
            elapsed = time.time() - start
            logging.info(f"Query completed in {elapsed:.4f} seconds")
            return df
        
        except Exception as e:
            print(e)

    def __del__(self):
        self.connection.close()


def main():
    try:
        filename = __file__.split(".")[0]
        begin = time.time()
        logging.basicConfig(filename=filename+'.log', filemode='w', level=logging.DEBUG, 
                                    format=u'%(filename)s:%(lineno)d %(levelname)-4s [%(asctime)s]  %(message)s')
        logging.info("Program started.")
        #db = Database("(local)", "Arbitron", "arb", "arb")
        db = Database()
        #sql = "select exchange, pair, ts from v_last_ts" #
        sql = "select exchange, pair, ts from last_history_cache with (snapshot)"
        #sql = "select exchange, pair, ts from history where exchange='Cryptopia' and pair='DASH/LTC' order by ts"

        results = Results(config_ini="emitter.ini", queue_name="results") #MessageBus()
        df = db.query(sql)
        if len(df)==0:
            raise ValueError("DataFrame is empty!")
        
        # spit out Dataframe line-by-line
        for index, row in df.iterrows():
            msg = f"{index} - {row['exchange']}: {row['pair']}"
            print(msg)
            results.send(message=msg)
            sleep(2)

        elapsed = time.time() - begin
        logging.info(f"Program finished in {elapsed:.4f} seconds.")
        
    except KeyboardInterrupt:
        print("\nLeaving by CTRL-C")
        sys.exit()

if __name__ == "__main__":
    main()
