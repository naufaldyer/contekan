import pandas as pd
import numpy as np
import warnings

import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_lazada(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"
    month_name = pd.to_datetime(month, format="%m").month_name()

    try:
        df_raw = pd.read_excel(
            f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Lazada_{month_name}_{brand}.xlsx",
            dtype={"sellerSku": str},
        )
        df = df_raw[
            [
                "createTime",
                "updateTime",
                "deliveredDate",
                "status",
                "orderNumber",
                "sellerSku",
                "unitPrice",
                "sellerDiscountTotal",
                "buyerFailedDeliveryReturnInitiator",
                "buyerFailedDeliveryReason",
                "shippingProvider",
                "trackingCode",
                "shippingFee",
                "customerName",
                "shippingAddress",
                "shippingCity",
                "shippingPostCode",
            ]
        ].rename(
            columns=(
                {
                    "createTime": "Date",
                    "updateTime": "Ship Date",
                    "deliveredDate": "Delivered Date",
                    "status": "Status Pesanan",
                    "orderNumber": "No Order",
                    "sellerSku": "Barcode",
                    "unitPrice": "Net Sales",
                    "sellerDiscountTotal": "Voucher",
                    "buyerFailedDeliveryReturnInitiator": "Status Pembatalan/Pengembalian",
                    "buyerFailedDeliveryReason": "Alasan Pembatalan",
                    "shippingProvider": "Pickup & Courier",
                    "trackingCode": "Resi/Pin",
                    "shippingFee": "Delivery Fee",
                    "customerName": "Recipient Name",
                    "shippingAddress": "Recipient Address",
                    "shippingCity": "Recipient City",
                    "shippingPostCode": "Recipient Postcode",
                }
            )
        )
        df["Brand"] = brand
        df["Marketplace"] = "LAZADA"

        try:
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            df["Ship Date"] = pd.to_datetime(df["Ship Date"]).dt.date
            df["Delivered Date"] = pd.to_datetime(df["Delivered Date"]).dt.date
        except:
            df["Date"] = pd.to_datetime(
                df["Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            df["Ship Date"] = pd.to_datetime(
                df["Ship Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            df["Delivered Date"] = pd.to_datetime(
                df["Delivered Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        df["Barcode"] = df["Barcode"].astype("str")

        df["Voucher"] = df["Voucher"].fillna(0)
        df["Voucher"] = df["Voucher"].apply(lambda x: x * -1 if x < 0 else x)

        df["Qty Sold"] = 1

        df["Status Pesanan"] = df["Status Pesanan"].str.upper()

        if not df["Status Pembatalan/Pengembalian"].isnull().all():
            df["Status Pembatalan/Pengembalian"] = df[
                "Status Pembatalan/Pengembalian"
            ].str.upper()

        # replace value
        to_replace = {
            "6602762588-1699015282254-2": "1000000055576",
            "4706454460-1699014985409-1": "1000000046013",
            "5352440765-1699015083673-2": "1000000050742",
            "7297014564-1699015393637-1": "8990100029488",
            "7522936912-1699015452330-1": "1000000048232",
            "5352440765-1699015083673-0": "1000000050745",
            "7305118850-1699015398170-1": "1000000031093",
            "4649934693-1699014956928-1": "1000000042404",
            "7305118850-1699015398170-3": "1000000031091",
            "7522936912-1699015452330-4": "1000000048234",
            "6602388074-1699015280054-6": "1000000055416",
            "5352514169-1699015084655-4": "1000000050749",
            "3723278397-1699014951607-2": "1000000045751",
            "5894284477-1699015176635-0": "1000000052600",
            "5271584448-1699015053124-4": "1000000044392",
            "5271702013-1699015053483-0": "1000000044427",
            "6639176097-1699015302656-4": "1000000056804",
            "5820826883-1699015167635-0": "1000000053672",
            "5820880795-1699015168648-2": "1000000053667",
            "4707216187-1699014987176-0": "1000000055956",
            "6149578751-1699015242385-3": "1000000044509",
            "1454814545-1699014932147-0": "1000000042425",
        }

        df["Barcode"] = df["Barcode"].replace(to_replace)
        df = mapping_barcode(
            df_marketplace=df, columns_marketplace="Barcode", company=company
        )
        df["Basic Price"] = df["BASICPRICE"]

        # Rumus
        df["Discount"] = df["Basic Price"] - df["Net Sales"]
        df["Existing Basic Price"] = df["Basic Price"] * df["Qty Sold"]
        df["Existing Discount"] = df["Discount"] * df["Qty Sold"]
        df["Existing Net Sales"] = df["Net Sales"] * df["Qty Sold"]
        df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]
        df["Value After Voucher"] = df["Existing Net Sales"] - df["Existing Voucher"]
        df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

        return df
    except:
        pass


def run_lazada(year, month):
    list_brand = ["SZ", "MN", "MC", "MT", "ED"]
    df = pd.concat(
        [transform_lazada(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)
