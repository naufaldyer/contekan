import pandas as pd
import numpy as np
import warnings
import s3fs
import openpyxl

import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_tiktok(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"
    month_name = pd.to_datetime(month, format="%m").month_name()

    try:
        path = f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Tiktok_{month_name}_{brand}.xlsx"
        data = s3fs.S3FileSystem().open(path, mode="rb")
        wb = openpyxl.load_workbook(data)
        sheet = wb["OrderSKUList"]

        data = []
        for row in sheet.iter_rows(values_only=True):
            data.append(row)

        df_raw = pd.DataFrame(data[2:], columns=data[0])

        col_name = [
            "SKU Unit Original Price",
            "SKU Seller Discount",
            "SKU Seller Discount",
            "SKU Subtotal After Discount",
            "Order Amount",
            "SKU Platform Discount",
            "SKU Subtotal Before Discount",
            "Shipping Fee After Discount",
        ]

        for col in col_name:
            df_raw[col] = df_raw[col].fillna("IDR 0")
            df_raw[col] = (
                df_raw[col]
                .astype(str)
                .str.replace("IDR ", "")
                .str.replace(".", "", regex=False)
            ).astype(int)

        df_raw["Detail Address"] = df_raw["Detail Address"].str.replace("=", "")
        df_raw["Seller SKU"] = df_raw["Seller SKU"].astype(str)
        if df_raw["Seller SKU"].str.contains(".").any():
            df_raw["Seller SKU"] = df_raw["Seller SKU"].str.split(".").str[0]
        else:
            pass

        df = df_raw[
            [
                "Created Time",
                "Shipped Time",
                "Delivered Time",
                "Order Status",
                "Order Substatus",
                "Order ID",
                "Seller SKU",
                "Quantity",
                "SKU Unit Original Price",
                "SKU Subtotal Before Discount",
                "SKU Platform Discount",
                "SKU Seller Discount",
                "SKU Subtotal After Discount",
                "Order Amount",
                "Cancelation/Return Type",
                "Cancel By",
                "Cancel Reason",
                "Shipping Provider Name",
                "Tracking ID",
                "Shipping Fee After Discount",
                "Recipient",
                "Phone #",
                "Detail Address",
                "Districts",
                "Regency and City",
                "Zipcode",
            ]
        ].rename(
            columns={
                "Created Time": "Date",
                "Shipped Time": "Ship Date",
                "Delivered Time": "Delivered Date",
                "Order Status": "Status Pesanan",
                "Order Substatus": "Sub Status",
                "Order ID": "No Order",
                "Seller SKU": "Barcode",
                "Quantity": "Qty Sold",
                "SKU Unit Original Price": "Basic Price",
                "SKU Subtotal Before Discount": "Existing Basic Price",
                "SKU Subtotal After Discount": "Existing Net Sales",
                "SKU Platform Discount": "Subsidi",
                "Cancel Reason": "Alasan Pembatalan",
                "Cancelation/Return Type": "Status Pembatalan/Pengembalian",
                "Shipping Provider Name": "Pickup & Courier",
                "Tracking ID": "Resi/Pin",
                "Shipping Fee After Discount": "Delivery Fee",
                "Recipient": "Recipient Name",
                "Phone #": "Recipient Phone",
                "Detail Address": "Recipient Address",
                "Districts": "Recipient District",
                "Regency and City": "Recipient City",
                "Zipcode": "Recipient Postcode",
            }
        )

        numeric_columns = [
            "Qty Sold",
            "Basic Price",
            "Existing Basic Price",
            "Subsidi",
            "SKU Seller Discount",
            "Existing Net Sales",
        ]
        df[numeric_columns] = df[numeric_columns].fillna(0).astype(int)

        df["Brand"] = brand
        df["Marketplace"] = "TIKTOK"
        df["Date"] = df["Date"].str.replace("\t", "")
        df["Ship Date"] = df["Ship Date"].str.replace("\t", "")
        df["Delivered Date"] = df["Delivered Date"].str.replace("\t", "")
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y %H:%M:%S").dt.strftime(
            "%Y-%m-%d"
        )
        df["Ship Date"] = pd.to_datetime(
            df["Ship Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df["Delivered Date"] = pd.to_datetime(
            df["Delivered Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        df["Barcode"] = df["Barcode"].astype(str)
        df["Voucher"] = 0
        df["Existing Voucher"] = 0
        df["Status Pesanan"] = df["Status Pesanan"].str.upper()

        df = mapping_barcode(
            df_marketplace=df, columns_marketplace="Barcode", company=company
        )

        # Rumus
        # df["Existing Discount"] = (df["SKU Seller Discount"] + df["SKU Platform Discount"])
        # Moc SKU Plarform discount nya dijadiin discount

        # df["Existing Net Sales"] = (
        #     df["Existing Basic Price"] - df["Existing Discount"]
        # )

        df["Existing Discount"] = df["SKU Seller Discount"]
        df["Discount"] = df["Existing Discount"] / df["Qty Sold"]
        df["Net Sales"] = df["Existing Net Sales"] / df["Qty Sold"]
        df["Value After Voucher"] = (
            df["Existing Net Sales"] - df["Existing Voucher"] + df["Subsidi"]
        )
        df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

        df = df.drop(columns=["SKU Seller Discount", "Order Amount"])

        return df
    except Exception as e:
        print(e)
        print(df_raw.columns)


def run_tiktok(year, month):
    list_brand = ["SZ", "MN", "MC", "MT", "ED"]
    df = pd.concat(
        [transform_tiktok(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)
