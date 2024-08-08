import pandas as pd
import numpy as np
import warnings
from sqlalchemy import create_engine, text
from extract import *
from load import *
from transform import *

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class MainETL:
    def __init__(self, month, year):
        self.month = month
        self.year = year

    def main_run_etl(self):
        print(f"---Start{self.month} {self.year}---")
        transformer = TransformDataSalesAccum(month=self.month, year=self.year)
        df_sales_akum, df_sales_promo = transformer.transform_data_report()

        loader = SalesAkumLoader(month=self.month, year=self.year)

        print("loaded data akum")
        loader.load_data_sales_akum(df_sales_akum)

        print("loaded data promo")
        loader.load_data_sales_promo(df_sales_promo)

        print(f"---Finished {self.month} {self.year}---")
        print(f"----------------------------------")
