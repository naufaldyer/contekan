import pandas as pd
import numpy as np
import warnings
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class SalesAccumDataExtractor:
    def __init__(self, month: "str", year: "str"):
        """_summary_

        Args:
            month (str): Bulan berjalan
            year (str): Tahun bulan berjalan
        """
        self.month = month
        self.year = year

    def extract_master_store(self):
        year = self.year
        month_name = pd.to_datetime(self.month, format="%m").month_name()

        df = pd.read_parquet(
            f"s3://mega-dev-lake/Staging/Master/Master Store/{year}/data.parquet"
        ).query(f"month == '{month_name}'")
        df = df.drop(columns=["month", "category"])
        df = df.rename(
            columns={
                "so dept head": "so_dept_head",
                "area head": "area_head",
                "city head": "city_head",
            }
        )

        return df

    def extract_pattern(self):
        month = self.month

        df = pd.read_excel(
            "s3://mega-lake/ProcessedData/Sales/pattern/pattern_24_with_festive.xlsx"
        )
        df = df[pd.to_datetime(df["pattern_date"]).dt.strftime("%m") == month]

        return df

    def extract_master_product(self, columns_list: list):
        df = pd.concat(
            [
                pd.read_parquet(
                    f"s3://mega-dev-lake/ProcessedData/D365/Product/{company}/data.parquet"
                )
                .query("PullStatus == 'Current'")
                .assign(dataareaid=company)
                for company in ["mpr", "mgl"]
            ]
        )
        df.columns = df.columns.str.lower()
        df = df.drop_duplicates(subset=["codebars", "dataareaid"])
        df = df[columns_list]
        return df

    def extract_sales_offline(self, month, year):
        """_summary_

        Args:
            month (_type_): ada 3 kondisi:
                - sales bulan ini, ambil bulan berjalan.
                - sales bulan lalu , ambil bulan dari kolom pattern_bulan_lalu yang ada di dataframe pattern
                - sales tahun lalu, ambil bulan berjalan tahun ini.
            year (_type_): _description_
        """
        query_txt = "istransaction != '0' & issettled != False & status_order != 7 & channel != 'ONLINE'"

        df = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_detail_indie/{year}/{month}/"
        ).query(query_txt)[
            [
                "dataareaid",
                "axcode",
                "order_create_date",
                "codebars",
                "productdata_barcode",
                "ordernumber",
                "quantity",
                "cust_paid_peritem",
                "hpp_total",
                "promocode",
                "remarks",
            ]
        ]

        return df

    def extract_sales_online(self, month, year):
        df = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{month}/data.parquet"
        ).query("`Status Sales` != 'CANCELED'")
        df["Date"] = df["Date"].astype(str)
        df = df[
            [
                "Brand",
                "Marketplace",
                "Date",
                "No Order",
                "CODEBARS",
                "Qty Sold",
                "Value After Voucher",
                "Existing HPP",
            ]
        ].rename(
            columns={
                "Date": "order_create_date",
                "No Order": "ordernumber",
                "CODEBARS": "codebars",
                "Qty Sold": "quantity",
                "Value After Voucher": "cust_paid_peritem",
                "Existing HPP": "hpp_total",
                "Brand": "brand",
                "Marketplace": "marketplace",
            }
        )
        return df
