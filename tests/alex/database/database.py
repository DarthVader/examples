from sqlalchemy import create_engine
import pandas as pd
from decimal import Decimal

class Database:
    def __init__(self):        
        self.connection_string = f"mssql+pymssql://Alex:cawa1728@10.7.0.19/Arbitron"
        self.engine = create_engine(self.connection_string, pool_size=4, pool_pre_ping=True, 
                                    strategy='threadlocal', isolation_level='AUTOCOMMIT')
        self.connection = self.engine.connect()
        self.engine.execute("set language 'russian'")
        
    def query(self, sql):
        """
        executes query against MSSQL, returns Pandas Dataframe
        """
        try:
            df = pd.read_sql_query(sql, self.connection)
            return df

        except Exception as e:
            print(e)
    
    def __del__(self):
        self.connection.close()
		
    def optimize_df(self, df):
		# оптимизация размера датафрейма - преобразование в категории того, чего надо 
		# и присвоение типа Decimal фин.показателям
        try:
            for col in df.columns:
                if col in ('id'):
                    df[col] = df[col].astype(Decimal)
                if col in ('exchange', 'pair', 'bid_ask', 'side'):
                    df[col] = df[col].astype('category')
                if col in ('price', 'amount', 'volume'):
                    df[col] = df[col].astype(Decimal)
            return df
        except Exception as e:
            print(e)		