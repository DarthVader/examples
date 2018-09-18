#!/usr/bin/env python3

import sqlalchemy as sa
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import logging, time, sys, json
import pandas as pd
from configparser import ConfigParser
from colorama import init, Fore, Back, Style # color printing
# from Queue import Queue, Empty as QueueEmpty


class Database():
    """
    Arbitron MSSQL database interaction
    """
    __instance = object

    @classmethod
    def getInstance(self, config_ini = "database.ini"):
        if not isinstance(Database.__instance, Database):
            Database.__instance = Database("database.ini")
        return Database.__instance
        

    def __init__(self, config_ini = "database.ini"): #, server, database, user, password):
        try:
            init(convert=True)  # colorama init 
            self.config = ConfigParser()
            #self.config.read(__file__.split('.')[0]+'.ini')
            self.config.read(config_ini)

            self.server = self.config['mssql']['server_address']
            self.database = self.config['mssql']['database_name']
            self.user = self.config['mssql']['user_name']
            self.password = self.config['mssql']['password']

            # self.exchanges = {}          #  exchanges - dict of ccxt objects
            # self.exchanges_list = []     #  exchanges_list - custom list of exchanges to filter (lowercase)
            # self.ex_pairs = {}           #  ex_pairs - dict of exchanges which contains corresponding trading pairs
            # self.df_exchanges = pd.DataFrame()
            # self.df_tokens = pd.DataFrame()

            logging.info("Connecting to database...")
            start = time.time()
            self.connection_string = f"mssql+pymssql://{self.user}:{self.password}@{self.server}/{self.database}"
            self.engine = sa.create_engine(self.connection_string, pool_size=16, pool_pre_ping=True, 
                                           strategy='threadlocal', isolation_level='AUTOCOMMIT')
            # self.session_factory = sessionmaker(bind=self.engine)
            # self.Session = scoped_session(self.session_factory)
            
            # self.Session = sessionmaker(bind=self.engine, autocommit=True)
            # self.Session = scoped_session(
            #     sessionmaker(
            #         autoflush=True,
            #         bind=self.engine,
            #         autocommit=True,
            #         #expire_on_commit=False
            #     )
            # )
            # self.session = self.Session(autocommit=False)  

            # self.connection = self.engine.connect()  # -- открываем соединение, пока открыто - висит блокировка. Закрытие по con.Close()
            # self.connection.execution_options(autocommit=True)

            elapsed = time.time() - start
            logging.info(f"Connected in {elapsed:.4f} seconds")

        except Exception as e:
            print(e)
            sys.exit()
            

    def query(self, sql):
        """
        executes query against MSSQL, returns Pandas Dataframe
        """
        try:
            connection = self.engine.connect()
            # logging.info("Starting query...")
            # start = time.time()
            # session = self.Session()
            # df = pd.read_sql_query(sql, session.bind)  # выполняем sql запрос и записываем результат в pandas dataframe
            df = pd.read_sql_query(sql, connection)  # выполняем sql запрос и записываем результат в pandas dataframe
            connection.close()
            # df = self.engine.query(sql)
            # session.close()
            # elapsed = time.time() - start
            # logging.info(f"Query {sql} completed in {elapsed:.4f} seconds")
            return df
        
        except Exception as e:
            print(e)


    def execute_sql(self, sql):
        try:
            connection = self.engine.connect()
            #logging.info("Starting SP...")
            #start = time.time()
            #session = self.Session(autocommit=True)
            #session.begin()
            transaction = connection.begin()
            result = self.engine.execute(sql)  # выполняем sql запрос и записываем результат в pandas dataframe
            transaction.commit()
            #session.query(sql)
            #session.commit()
            #elapsed = time.time() - start
            #logging.info(f"{sql} executed in {elapsed:.4f} seconds")
            return result
        
        except Exception as e:
            #session.rollback()
            print(e)
        finally:
            connection.close()
            #session.close()


    def execute(self, sp, json):
        """
        executes stored procedure against MSSQL, returns Pandas Dataframe
        """
        #start = time.time()
        try:
            #logging.info("Executing stored procedure {sp} ...")
            #df = pd.read_sql_query(sql, self.connection)  # выполняем sql запрос и записываем результат в pandas dataframe
            #cursor = self.connection.cursor()
            # transaction = connection.begin()
            self.engine.execute(text(f"{sp} '{json}'").execution_options(autocommit=True))
            # transaction.commit()
            # self.connection.commit()
            
            # elapsed = time.time() - start
            # logging.info(f"{sp} executed in {elapsed:.4f} seconds")

        except Exception as e:
            # session.rollback()
            print(e)        

    def __del__(self):
        #self.connection.close()
        pass


if __name__ == "__main__":
    print("This module is not intended for direct use")