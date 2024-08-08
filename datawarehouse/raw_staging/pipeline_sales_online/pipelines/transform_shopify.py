import pandas as pd
import numpy as np
import warnings
import s3fs
import openpyxl

import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_shopify(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"

    month_name = pd.to_datetime(month, format="%m").month_name()

    try:
        df_raw = pd.read_csv(
            f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Shopify_{month_name}_{brand}.csv",
            dtype={
                "Lineitem sku": "str",
                "Subtotal": "float",
                "Total": "float",
                "Discount Amount": "float",
                "lineitem quantity": "int",
            },
        )

        df_raw["Lineitem sku"] = (
            df_raw["Lineitem sku"].astype("str").str.split(".").str[0]
        )
        df_raw[["Email", "Shipping Province Name"]] = df_raw[
            ["Email", "Shipping Province Name"]
        ].fillna("")
        df_header = df_raw.groupby(
            [
                "Created at",
                "Fulfilled at",
                "Name",
                "Email",
                "Financial Status",
                "Fulfillment Status",
                "Payment Reference",
                "Shipping Name",
                "Shipping Phone",
                "Shipping Street",
                "Shipping Province Name",
                "Shipping City",
                "Shipping Zip",
            ],
            as_index=False,
        )[["Subtotal", "Total", "Discount Amount"]].sum()
        df_detail = df_raw[
            ["Name", "Lineitem quantity", "Lineitem price", "Lineitem sku"]
        ]

        # Voucher
        df_header = df_header.merge(
            df_detail.groupby(["Name"], as_index=False)[["Lineitem quantity"]].sum(),
            "left",
            on=["Name"],
        )
        df_header["Voucher"] = (
            df_header["Discount Amount"] / df_header["Lineitem quantity"]
        )
        df = df_header.rename(columns={"Lineitem quantity": "Total quantity"}).merge(
            df_detail, "left", on=["Name"]
        )

        df = df.rename(
            columns={
                "Created at": "Date",
                "Fulfilled at": "Ship Date",
                "Financial Status": "Status Pesanan",
                "Fulfillment Status": "Sub Status",
                "Lineitem quantity": "Qty Sold",
                "Lineitem price": "Net Sales",
                "Lineitem sku": "Barcode",
                "Discount Amount": "Existing Voucher",
                "Shipping Name": "Recipient Name",
                "Shipping Phone": "Recipient Phone",
                "Shipping Street": "Recipient Address",
                "Shipping Province Name": "Recipient District",
                "Shipping City": "Recipient City",
                "Shipping Zip": "Recipient Postcode",
            }
        )

        df["Brand"] = brand
        df["Marketplace"] = "SHOPIFY"
        df["Status Pesanan"] = df["Status Pesanan"].str.upper()
        df["Sub Status"] = df["Sub Status"].str.upper()
        df["No Order"] = df["Payment Reference"] + df["Name"]
        df = df.drop(columns={"Payment Reference", "Name"})
        df["Date"] = pd.to_datetime(pd.to_datetime(df["Date"]).dt.date)
        df["Ship Date"] = pd.to_datetime(pd.to_datetime(df["Ship Date"]).dt.date)
        df["Ship Date"] = df["Ship Date"].fillna(pd.to_datetime("1900-01-01"))
        df["Recipient Phone"] = df["Recipient Phone"].astype(str)

        df = mapping_barcode(
            df_marketplace=df, columns_marketplace="Barcode", company=company
        )
        df["Basic Price"] = df["BASICPRICE"]

        # Rumus
        df["Existing Net Sales"] = df["Net Sales"] * df["Qty Sold"]

        df["Existing Basic Price"] = df["Basic Price"] * df["Qty Sold"]

        df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]

        df["Discount"] = df["Basic Price"] - df["Net Sales"]
        df["Existing Discount"] = df["Discount"] * df["Qty Sold"]
        df["Existing HPP"] = df["HPP"] * df["Qty Sold"]
        df["Value After Voucher"] = df["Existing Net Sales"] - df["Existing Voucher"]

        df = df.drop(columns=["Subtotal", "Total", "Total quantity"])

        return df
    except Exception as e:
        print(e)


def run_shopify(year, month):
    list_brand = ["SZ", "MN", "MC", "MT", "ED"]
    df = pd.concat(
        [transform_shopify(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)
