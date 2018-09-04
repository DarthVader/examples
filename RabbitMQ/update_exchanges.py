#!/usr/bin/python3

# update_exchanges
# Loads/updates exchanges params in MSSQL database

__version__ = '2.0.0'

import os, sys
import pandas as pd
from colorama import init, Fore, Back, Style # color printing

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
from markets.markets import Markets
from database import Database


if __name__ == '__main__':
    init(convert=True) # colorama init  
    db = Database("database.ini")
    db.get_exchanges(all=True)
    db.get_tokens(all=True)
    my_exchanges = db.exchanges_list
    my_tokens = db.tokens_list
    #print("OK")

    print("Loading markets from WWW...")
    markets = Markets()
    markets.load_exchanges(exchanges_list=my_exchanges)

    sql = []
    parameters = ['publicAPI', 'privateAPI', 'CORS', 'cancelOrder', 'cancelOrders', 'createDepositAddress', 
                  'createOrder', 'createMarketOrder', 'deposit', 'editOrder', 'fetchBalance', 'fetchClosedOrders', 
                  'fetchCurrencies', 'fetchDepositAddress', 'fetchDeposits', 'fetchFundingFees', 'fetchL2OrderBook', 
                  'fetchMarkets', 'fetchMyTrades', 'fetchOpenOrders', 'fetchOrder', 'fetchOrderBook', 'fetchOrderBooks', 
                  'fetchOrders', 'fetchTicker', 'fetchTickers', 'fetchTrades', 'fetchTradingFees', 'fetchTradingLimits', 
                  'fetchTransactions', 'fetchWithdrawals', 'withdraw']
                  
    for ex in markets.exchanges_list:
        print(f"{ex}...", end="")
        rateLimit = markets.exchanges[ex].rateLimit
        rateLimitMaxTokens = markets.exchanges[ex].rateLimitTokens
        apiVersion = markets.exchanges[ex].version

        
        db.execute_sql(f"UPDATE dbo.exchanges SET rateLimit={rateLimit} WHERE id='{ex}' and rateLimit is null")
        db.execute_sql(f"UPDATE dbo.exchanges SET rateLimitMaxTokens={rateLimitMaxTokens} WHERE id='{ex}' and rateLimitMaxTokens is null")
        db.execute_sql(f"UPDATE dbo.exchanges SET apiVersion='{apiVersion}' WHERE id='{ex}' and apiVersion is null")

        for param in parameters:
            vars = {}
            if markets.exchanges[ex].has[param] in (True, False):
                vars[param] = markets.exchanges[ex].has[param]
                sql = f"UPDATE dbo.exchanges SET {param}='{vars[param]}' WHERE id='{ex}' and {param} IS NULL"

                rateLimit = None
                rateLimitMaxTokens = None

                try:
                    db.execute_sql(sql)
                    #print("OK")
                except Exception:
                    print("FAILED!")

   
    #a = input("Press Enter...")
    print(f"SUCCESS! Exchanges table parameters has been updated.")