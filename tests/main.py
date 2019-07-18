import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
from alex.database import Database
import sys


def main():
    print(sys.path)

    print(f"Pandas version is {pd.__version__}")
    print(f"Numpy version is {np.__version__}")

    db = Database() # подключение к базе данных с учётными данными для Alex

    # exchange = "binance"
    # pair = "ETH/USDT"
    lag_seconds = 43200 # 86400 - 1 day

    sql = f"select * from tvf_get_history_seconds_ago({lag_seconds}) order by dt, id"
    history = db.query(sql)

    # pprint(history.head())
    plt.rcParams['figure.figsize'] = [16, 5]
    history['price'].plot(grid=True, marker='o', color='gray', linestyle='--', linewidth=1, markerfacecolor='r')
    plt.title(f"Price history for last {int(lag_seconds/60)} minutes")
    plt.show()


if __name__ == "__main__":
    main()
