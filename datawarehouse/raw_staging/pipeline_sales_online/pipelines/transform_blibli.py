import pandas as pd
import numpy as np
import warnings
import s3fs
import openpyxl

import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace

def transform_blibli(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"
        
    month_name = pd.to_datetime(month, format="%m").month_name()

    try:
        df_raw = pd.read_csv(
            f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Blibli_{month_name}_{brand}.csv"
        )

        df = df_raw[
            [
                "No. Order",
                "Tanggal Order",
                "Merchant SKU",
                "Total Barang",
                "Harga item pesanan",
                "Total harga item pesanan",
                "Order Status",
                "Total",
                "Diskon",
                "Voucher seller",
                "Servis Logistik",
                "No. Awb",
            ]
        ].rename(
            columns={
                "No. Order": "No Order",
                "Tanggal Order": "Date",
                "Merchant SKU": "Barcode",
                "Total Barang": "Qty Sold",
                "Harga item pesanan": "Basic Price",
                "Total harga item pesanan": "Existing Basic Price",
                "Diskon": "Discount",
                "Voucher seller": "Voucher",
                "Order Status": "Status Pesanan",
                "Servis Logistik": "Pickup & Courier",
                "No. Awb": "Resi/Pin",
            }
        )

        df["Brand"] = brand
        df["Marketplace"] = "BLIBLI"
        df["Date"] = pd.to_datetime(
            pd.to_datetime(df["Date"], format="%d/%m/%Y %H:%M").dt.strftime(
                "%Y-%m-%d"
            )
        )
        df["Status Pesanan"] = df["Status Pesanan"].str.upper()

        process_columns = ["No Order", "Barcode"]
        for c in process_columns:
            df[c] = df[c].astype(str)
            if df[c].str.contains('"').any():
                df[c] = df[c].str.split('"').str[1]
            else:
                pass
        df = mapping_barcode(df_marketplace=df,
                             columns_marketplace = "Barcode",
                             company=company)
        # Rumus
        # df_blibli1["Discount"] = df_blibli1["Basic Price"] - df_blibli1["Net Sales"]
        # df_blibli1["Existing Basic Price"] = (
        #     df_blibli1["Basic Price"] * df_blibli1["Qty Sold"]
        # )
        df["Net Sales"] = df["Basic Price"] - df["Discount"]
        df["Existing Discount"] = df["Discount"] * df["Qty Sold"]
        df["Existing Net Sales"] = df["Existing Basic Price"] - df["Existing Discount"]
        df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]
        df["Value After Voucher"] = df["Existing Net Sales"] - df["Existing Voucher"]
        df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

        return df
    except Exception as e:
        print(e)


def run_blibli(year, month):
    list_brand = ["SZ", "MN", "MC", "MT", "ED"]
    df = pd.concat(
        [transform_blibli(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)