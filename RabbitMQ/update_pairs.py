#!/usr/bin/python3.6

# load_pairs
# Loads/updates meaningful pairs in MSSQL database

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
    has_params = ['publicAPI', 'privateAPI', 'CORS', 'cancelOrder', 'cancelOrders', 'createDepositAddress', 
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

        # apiURL = markets.exchanges[ex].api
        publicAPI = markets.exchanges[ex].has['publicAPI']
        privateAPI = markets.exchanges[ex].has['privateAPI']
        CORS = markets.exchanges[ex].has['CORS']
        cancelOrder = markets.exchanges[ex].has['cancelOrder']
        cancelOrders = markets.exchanges[ex].has['cancelOrders']
        createDepositAddress = markets.exchanges[ex].has['createDepositAddress']
        createOrder = markets.exchanges[ex].has['createOrder']
        createMarketOrder = markets.exchanges[ex].has['createMarketOrder']
        deposit = markets.exchanges[ex].has['deposit']
        editOrder = markets.exchanges[ex].has['editOrder']
        fetchBalance = markets.exchanges[ex].has['fetchBalance']
        fetchClosedOrders = markets.exchanges[ex].has['fetchClosedOrders']
        fetchCurrencies = markets.exchanges[ex].has['fetchCurrencies']
        fetchDepositAddress = markets.exchanges[ex].has['fetchDepositAddress']
        fetchDeposits = markets.exchanges[ex].has['fetchDeposits']
        fetchFundingFees = markets.exchanges[ex].has['fetchFundingFees']
        fetchL2OrderBook = markets.exchanges[ex].has['fetchL2OrderBook']
        fetchMarkets = markets.exchanges[ex].has['fetchMarkets']
        fetchMyTrades = markets.exchanges[ex].has['fetchMyTrades']
        fetchOpenOrders = markets.exchanges[ex].has['fetchOpenOrders']
        fetchOrder = markets.exchanges[ex].has['fetchOrder']
        fetchOrderBook = markets.exchanges[ex].has['fetchOrderBook']
        fetchOrderBooks = markets.exchanges[ex].has['fetchOrderBooks']
        fetchOrders = markets.exchanges[ex].has['fetchOrders']
        fetchTicker = markets.exchanges[ex].has['fetchTicker']
        fetchTickers = markets.exchanges[ex].has['fetchTickers']
        fetchTrades = markets.exchanges[ex].has['fetchTrades']
        fetchTradingFees = markets.exchanges[ex].has['fetchTradingFees']
        fetchTradingLimits = markets.exchanges[ex].has['fetchTradingLimits']
        fetchTransactions = markets.exchanges[ex].has['fetchTransactions']
        fetchWithdrawals = markets.exchanges[ex].has['fetchWithdrawals']
        withdraw = markets.exchanges[ex].has['withdraw']

        #countries = "{'" + "','".join(markets.exchanges[ex].countries) + "'}"
        sql.append(f"UPDATE dbo.exchanges SET rateLimit={rateLimit} WHERE id='{ex}' and rateLimit is null")
        sql.append(f"UPDATE dbo.exchanges SET rateLimitMaxTokens={rateLimitMaxTokens} WHERE id='{ex}' and rateLimitMaxTokens is null")
        sql.append(f"UPDATE dbo.exchanges SET apiVersion='{apiVersion}' WHERE id='{ex}' and apiVersion is null")
        sql.append(f"UPDATE dbo.exchanges SET publicAPI='{publicAPI}' WHERE id='{ex}' and publicAPI IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET privateAPI='{privateAPI}' WHERE id='{ex}' and privateAPI IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET CORS='{CORS}' WHERE id='{ex}' and CORS IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET cancelOrder='{cancelOrder}' WHERE id='{ex}' and cancelOrder IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET cancelOrders='{cancelOrders}' WHERE id='{ex}' and cancelOrders IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET createDepositAddress='{createDepositAddress}' WHERE id='{ex}' and createDepositAddress IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET createOrder='{createOrder}' WHERE id='{ex}' and createOrder IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET createMarketOrder='{createMarketOrder}' WHERE id='{ex}' and createMarketOrder IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET deposit='{deposit}' WHERE id='{ex}' and deposit IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET editOrder='{editOrder}' WHERE id='{ex}' and editOrder IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchBalance='{fetchBalance}' WHERE id='{ex}' and fetchBalance IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchClosedOrders='{fetchClosedOrders}' WHERE id='{ex}' and fetchClosedOrders IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchCurrencies='{fetchCurrencies}' WHERE id='{ex}' and fetchCurrencies IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchDepositAddress='{fetchDepositAddress}' WHERE id='{ex}' and fetchDepositAddress IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchDeposits='{fetchDeposits}' WHERE id='{ex}' and fetchDeposits IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchFundingFees='{fetchFundingFees}' WHERE id='{ex}' and fetchFundingFees IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchL2OrderBook='{fetchL2OrderBook}' WHERE id='{ex}' and fetchL2OrderBook IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchMarkets='{fetchMarkets}' WHERE id='{ex}' and fetchMarkets IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchMyTrades='{fetchMyTrades}' WHERE id='{ex}' and fetchMyTrades IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchOpenOrders='{fetchOpenOrders}' WHERE id='{ex}' and fetchOpenOrders IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchOrder='{fetchOrder}' WHERE id='{ex}' and fetchOrder IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchOrderBook='{fetchOrderBook}' WHERE id='{ex}' and fetchOrderBook IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchOrderBooks='{fetchOrderBooks}' WHERE id='{ex}' and fetchOrderBooks IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchOrders='{fetchOrders}' WHERE id='{ex}' and fetchOrders IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTicker='{fetchTicker}' WHERE id='{ex}' and fetchTicker IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTickers='{fetchTickers}' WHERE id='{ex}' and fetchTickers IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTrades='{fetchTrades}' WHERE id='{ex}' and fetchTrades IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTradingFees='{fetchTradingFees}' WHERE id='{ex}' and fetchTradingFees IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTradingLimits='{fetchTradingLimits}' WHERE id='{ex}' and fetchTradingLimits IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchTransactions='{fetchTransactions}' WHERE id='{ex}' and fetchTransactions IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET fetchWithdrawals='{fetchWithdrawals}' WHERE id='{ex}' and fetchWithdrawals IS NULL")
        sql.append(f"UPDATE dbo.exchanges SET withdraw='{withdraw}' WHERE id='{ex}' and withdraw IS NULL")
        
        rateLimit = None
        rateLimitMaxTokens = None
        publicAPI = None
        privateAPI = None
        CORS = None
        cancelOrder = None
        cancelOrders = None
        createDepositAddress = None
        createOrder = None
        createMarketOrder = None
        deposit = None
        editOrder = None
        fetchBalance = None
        fetchClosedOrders = None
        fetchCurrencies = None
        fetchDepositAddress = None
        fetchDeposits = None
        fetchFundingFees = None
        fetchL2OrderBook = None
        fetchMarkets = None
        fetchMyTrades = None
        fetchOpenOrders = None
        fetchOrder = None
        fetchOrderBook = None
        fetchOrderBooks = None
        fetchOrders = None
        fetchTicker = None
        fetchTickers = None
        fetchTrades = None
        fetchTradingFees = None
        fetchTradingLimits = None
        fetchTransactions = None
        fetchWithdrawals = None
        withdraw = None

        try:
            db.execute_sql(";".join(sql))
            print("OK")
        except Exception:
            print("FAILED!")

    print("Loading pairs...", end="")
    #markets.load_pairs()
    markets.reload_pairs(my_tokens)
    print("OK")

    #cql_list = [f'TRUNCATE TABLE {config.pairs_table}']
    sql = []
    for ex in markets.exchanges_list:
        for pair in markets.ex_pairs[ex]:
            fsym, tsym = pair.split("/")
            try:
                tsym_withdraw_fee = 'NULL'
                tsym_withdraw_fee = markets.exchanges[ex].fees['funding']['withdraw'][tsym]
            except:
                pass
            try:
                fsym_withdraw_fee = 'NULL'
                fsym_withdraw_fee = markets.exchanges[ex].fees['funding']['withdraw'][fsym]
            except:
                pass
            sql.append(f"INSERT INTO mem.exchanges_pairs (exchange, pair, fsym, tsym, tsym_withdraw_fee, fsym_withdraw_fee, enabled)"+
                            f"VALUES ('{ex}', '{pair}', '{fsym}', '{tsym}', {tsym_withdraw_fee}, {fsym_withdraw_fee}, true)")
    db.query("\nGO\n".join(sql))

    #a = input("Press Enter...")
    print(f"SUCCESS! Table Exchange has been updated.")