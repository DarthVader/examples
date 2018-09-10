#!/usr/bin/env python3

import sqlalchemy as sa
import logging, time, sys, json
import pandas as pd
from configparser import ConfigParser
from colorama import init, Fore, Back, Style # color printing


class Database():
    """
    Arbitron MSSQL database interaction
    """
    __instance = object

    @classmethod
    def getInstance(self):
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

            self.exchanges = {}          #  exchanges - dict of ccxt objects
            self.exchanges_list = []     #  exchanges_list - custom list of exchanges to filter (lowercase)
            self.ex_pairs = {}           #  ex_pairs - dict of exchanges which contains corresponding trading pairs
            self.df_exchanges = pd.DataFrame()
            self.df_tokens = pd.DataFrame()

            logging.info("Connecting to database...")
            start = time.time()
            self.connection_string = f"mssql+pymssql://{self.user}:{self.password}@{self.server}/{self.database}"
            self.engine = sa.create_engine(self.connection_string)

            self.connection = self.engine.connect()  # -- открываем соединение, пока открыто - висит блокировка. Закрытие по con.Close()
            self.connection.execution_options(autocommit=True)
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
            #logging.info("Starting query...")
            start = time.time()
            df = pd.read_sql_query(sql, self.connection)  # выполняем sql запрос и записываем результат в pandas dataframe
            elapsed = time.time() - start
            logging.info(f"Query {sql} completed in {elapsed:.4f} seconds")
            return df
        
        except Exception as e:
            print(e)


    def execute_sql(self, sql):
        try:
            #logging.info("Starting SP...")
            start = time.time()
            transaction = self.connection.begin()
            result = self.connection.execute(sql)  # выполняем sql запрос и записываем результат в pandas dataframe
            transaction.commit()
            elapsed = time.time() - start
            logging.info(f"{sql} executed in {elapsed:.4f} seconds")
            return result
        
        except Exception as e:
            print(e)        


    def execute(self, sp, json):
        """
        executes stored procedure against MSSQL, returns Pandas Dataframe
        """
        try:
            #logging.info("Executing stored procedure {sp} ...")
            start = time.time()
            #df = pd.read_sql_query(sql, self.connection)  # выполняем sql запрос и записываем результат в pandas dataframe
            #cursor = self.connection.cursor()
            transaction = self.connection.begin()
            self.connection.execute(f"{sp} '{json}'")
            transaction.commit()
            # self.connection.commit()
            elapsed = time.time() - start
            logging.info(f"{sp} executed in {elapsed:.4f} seconds")

        except Exception as e:
            print(e)        



    def __del__(self):
        self.connection.close()


if __name__ == "__main__":
    print("This module is not intended for direct use")