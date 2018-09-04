import time
import asyncio
import ccxt as ccxt_s
import ccxt.async_support as ccxt
from ccxt.async_support import Exchange


exchanges = ['binance', 'bittrex', 'cryptopia', 'hitbtc2', 'kraken', 'poloniex', 'yobit']

async def _load_exchange(self, exchange: object):
    """
    Loads market metadata to exchange object, passed as parameter. Called from internal self._run_tasks() method.

        :param exchange: ccxt object instance
    """   
    #assert(self.exchanges != {}, "exchanges are not loaded!")
    await exchange.load_markets()
    print("{} market metadata loaded".format(exchange.name))


def main():
    for exchange in exchanges:
        if exchange.has['fetchTrades']:
            for symbol in exchange.markets:  # ensure you have called loadMarkets() or load_markets() method.
                time.sleep (exchange.rateLimit / 1000)  # time.sleep wants seconds
                print (symbol, exchange.fetch_trades (symbol))


if __name__ == "__main__":
    main()

