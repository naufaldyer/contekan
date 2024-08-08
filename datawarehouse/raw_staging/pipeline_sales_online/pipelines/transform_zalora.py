import pandas as pd
import numpy as np
import warnings

import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_zalora(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"
    month_name = pd.to_datetime(month, format="%m").month_name()

    try:
        df_raw = pd.read_excel(
            f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Zalora_{month_name}_{brand}.xlsx",
            dtype={"Seller SKU": "str"},
        )

        df = df_raw[
            [
                "Created at",
                "Updated at",
                "Order Number",
                "Status",
                "Reason",
                "Seller SKU",
                "Unit Price",
                "Paid Price",
                "Store Credits",
                "Wallet Credits",
                "Shipping Provider",
                "Tracking Code",
                "Shipping Fee",
                "Shipping Name",
                "Shipping Phone Number2",
                "Shipping Address",
                "Shipping Region",
                "Shipping City",
                "Shipping Postcode",
            ]
        ].rename(
            columns={
                "Created at": "Date",
                "Updated at": "Delivered Date",
                "Order Number": "No Order",
                "Seller SKU": "Barcode",
                "Unit Price": "Net Sales",
                "Status": "Status Pesanan",
                "Reason": "Alasan Pembatalan",
                "Shipping Provider": "Pickup & Courier",
                "Tracking Code": "Resi/Pin",
                "Shipping Fee": "Delivery Fee",
                "Shipping Name": "Recipient Name",
                "Shipping Phone Number2": "Recipient Phone",
                "Shipping Address": "Recipient Address",
                "Shipping Region": "Recipient District",
                "Shipping City": "Recipient City",
                "Shipping Postcode": "Recipient Postcode",
            }
        )

        df["Brand"] = brand
        df["Marketplace"] = "ZALORA"
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df["Delivered Date"] = pd.to_datetime(df["Delivered Date"]).dt.date
        df["Barcode"] = df["Barcode"].astype(str)
        df["Resi/Pin"] = df["Resi/Pin"].astype(str)
        df["Recipient Address"] = df["Recipient Address"].astype(str)
        df["Qty Sold"] = 1

        df = mapping_barcode(
            df_marketplace=df, columns_marketplace="Barcode", company=company
        )
        df["Basic Price"] = df["BASICPRICE"]

        # Status yang tidak dibawa

        # status_zalora = ["canceled", "delivery failed", "returned"]

        # df = df[~(df["Status Pesanan"].isin(status_zalora))]
        df["Status Pesanan"] = df["Status Pesanan"].str.upper()

        # Rumus
        df["Voucher"] = (
            df["Net Sales"]
            - df["Paid Price"]
            - df["Store Credits"]
            - df["Wallet Credits"]
        )
        df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]
        df["Discount"] = df["Basic Price"] - df["Net Sales"]
        df["Existing Basic Price"] = df["Basic Price"] * df["Qty Sold"]
        df["Existing Discount"] = df["Discount"] * df["Qty Sold"]

        df["Existing Net Sales"] = df["Existing Basic Price"] - df["Existing Discount"]
        df["Value After Voucher"] = df["Existing Net Sales"] - df["Existing Voucher"]
        df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

        df = df.drop(columns=["Paid Price", "Store Credits", "Wallet Credits"])
        return df
    except:
        pass


def run_zalora(year, month):
    list_brand = ["SZ", "MN", "MC", "MT", "ED"]
    df = pd.concat(
        [transform_zalora(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)
