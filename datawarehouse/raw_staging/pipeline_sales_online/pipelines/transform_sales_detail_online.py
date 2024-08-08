import pandas as pd
import numpy as np
import re
import warnings


def status_sales(row):
    status_mapping = {
        "BATAL": "CANCELED",
        "CANCELED": "CANCELED",
        "COMPLETED": (
            "CANCELED" if row["Status Pembatalan/Pengembalian"] else "DELIVERED"
        ),
        "CONFIRMED": (
            "DELIVERED"
            if row["Status Pembatalan/Pengembalian"] in ("CANCEL-BUYER", "")
            else "CANCELED"
        ),
        "DELIVERED": (
            "DELIVERED" if not row["Status Pembatalan/Pengembalian"] else "CANCELED"
        ),
        "DELIVERY FAILED": "CANCELED",
        "DIBATALKAN PEMBELI": "CANCELED",
        "DIBATALKAN PENJUAL [PERMINTAAN PEMBELI]": "CANCELED",
        "DIBATALKAN SISTEM": "CANCELED",
        "EXPIRED": "CANCELED",
        "IN TRANSIT: RETURNING TO SELLER": "CANCELED",
        "ORDER BATAL": "CANCELED",
        "PACKAGE RETURNED": "CANCELED",
        "RETURNED": "CANCELED",
        "paid": (
            "ONPROGRESS"
            if row["Sub Status"] in ("UNFULFILLED", "unfulfilled")
            else "DELIVERED"
        ),
        "PAID": (
            "ONPROGRESS"
            if row["Sub Status"] in ("UNFULFILLED", "unfulfilled")
            else "DELIVERED"
        ),
        "PESANAN SELESAI": "DELIVERED",
        "PESANAN TIBA": "DELIVERED",
        # Uncomment the following lines if needed:
        # "SEDANG DIKIRIM": "ONPROGRESS" if not row["Sub Status"] else "CANCELED",
        "SELESAI": (
            "CANCELED" if row["Sub Status"] == "PERMINTAAN DISETUJUI" else "DELIVERED"
        ),
        "BELUM BAYAR": "CANCELED",
        "TERKIRIM": "DELIVERED",
        "UNPAID": "CANCELED",
        "SHIPPED": (
            "ONPROGRESS"
            if not row["Status Pembatalan/Pengembalian"]
            or row["Status Pembatalan/Pengembalian"] == "CANCEL-BUYER"
            else "CANCELED"
        ),
        "LOST BY 3PL": "CANCELED",
    }

    return status_mapping.get(row["Status Pesanan"], "ONPROGRESS")


def string_standarize(input_string):
    regex_pattern = r"[^a-zA-Z0-9]"
    output_string = re.sub(regex_pattern, "", input_string)
    return output_string


def clear_string(input_string):
    # Remove non-alphanumeric characters (excluding spaces)
    cleaned_string = re.sub(r"[^a-zA-Z0-9\s]", "", input_string)

    return cleaned_string


def transform_online_detail(year, month):
    df = pd.concat(
        [
            pd.read_parquet(
                f"s3://mega-dev-lake/Staging/Sales/sales_online/marketplace/{marketplace}/2024/07/data.parquet"
            )
            for marketplace in [
                "shopee",
                "shopify",
                "tiktok",
                "tokopedia",
                "blibli",
                "lazada",
                "zalora",
            ]
        ]
    )

    df[df.select_dtypes(include=float).columns] = df[
        df.select_dtypes(include=float).columns
    ].fillna(0.0)
    df[df.select_dtypes(include=float).columns] = (
        df[df.select_dtypes(include=float).columns].astype(float).round(2)
    )

    # Standarisasi Date Columns
    df["Date"] = pd.to_datetime(df["Date"])
    df["monthnum"] = df["Date"].dt.strftime("%m")
    df["Month_In_Data"] = df["Date"].dt.strftime("%B")
    df["Day"] = df["Date"].dt.strftime("%d")
    df["Ship Date"] = pd.to_datetime(df["Ship Date"])
    df["Delivered Date"] = pd.to_datetime(df["Delivered Date"])

    # Standarize Columns
    df["No Order"] = df["No Order"].astype(str)

    df["Sub Status"] = df["Sub Status"].fillna("")
    df["Sub Status"] = df["Sub Status"].astype(str)
    df["Sub Status"] = df["Sub Status"].str.upper()

    df["Alasan Pembatalan"] = df["Alasan Pembatalan"].fillna("")

    df["Status Pembatalan/Pengembalian"] = df["Status Pembatalan/Pengembalian"].fillna(
        ""
    )

    if "Cancel By" in df.columns:
        df["Cancel By"] = df["Cancel By"].fillna("")
    else:
        df["Cancel By"] = ""

    list_object = [
        "Pickup & Courier",
        "Resi/Pin",
        "Recipient Name",
        "Recipient Phone",
        "Recipient Address",
        "Recipient District",
        "Recipient City",
        "Recipient Postcode",
        "Barcode",
    ]

    for c in list_object:
        df[c] = df[c].astype(str)
        df[c] = df[c].fillna("")
        df[c] = df[c].replace("nan", "")

    # Standarize Columns
    df["Resi/Pin"] = df["Resi/Pin"].apply(lambda x: string_standarize(x))

    df["Recipient Name"] = df["Recipient Name"].apply(lambda x: clear_string(x))

    df["Recipient Phone"] = df["Recipient Phone"].apply(lambda x: string_standarize(x))

    df["Recipient Phone"] = np.where(
        df["Recipient Phone"].str.startswith("62"),
        "0" + df["Recipient Phone"].str[2:],
        df["Recipient Phone"],
    )

    df["Recipient Phone"] = np.where(
        ~df["Recipient Phone"].str.startswith("0"),
        "0" + df["Recipient Phone"],
        df["Recipient Phone"],
    )

    df["Recipient Phone"] = np.where(
        df["Recipient Phone"].str.len() < 10,
        "*******",
        df["Recipient Phone"],
    )

    df["Recipient Postcode"] = (
        df["Recipient Postcode"]
        .str.replace(".0", "", regex=False)
        .str.replace("'", "", regex=False)
    )

    df["Recipient Postcode"] = df["Recipient Postcode"].str.replace("None", "")

    # Added Status Sales
    df["Status Sales"] = df.apply(status_sales, axis=1)

    # Added Category Product
    df["SUBCATEGORY"] = df["SUBCATEGORY"].fillna("")
    df["Category Product"] = np.where(
        df["SUBCATEGORY"].str.contains(
            "|".join(["KAFTAN", "GAMIS", "SARUNG", "SHANGHAI"])
        ),
        "FESTIVE",
        "REGULAR",
    )

    # Margin % from master
    margin_all = pd.read_excel(
        "s3://mega-lake/RawData/Sales_Online/margin/margin_ecom_2024.xlsx"
    )

    margin_all["Marketplace"] = margin_all["Marketplace"].str.replace(
        "WEBSITE", "SHOPIFY"
    )
    margin_all["Marketplace"] = margin_all["Marketplace"].str.replace(
        "TIK-TOK", "TIKTOK"
    )

    df = df.merge(
        margin_all[["Brand", "Marketplace", "Margin %"]],
        "left",
        on=["Brand", "Marketplace"],
    )

    df.loc[
        (~df["monthnum"].isin(["01", "02", "03", "04"]))
        & (df["Marketplace"] == "TIKTOK"),
        "Margin %",
    ] = 0.06500

    # df["Margin %"] = df.apply(
    #     lambda x: 0.114 if x["Brand"] == "SZ" else x["Margin %"], axis=1
    # )

    df["Existing HPP + 11%"] = df["Existing HPP"] + (df["Existing HPP"] * 0.11)
    df["DPP"] = df["Value After Voucher"] / 1.11
    df["Gross Profit (With Hpp + 11%)"] = (
        df["Value After Voucher"] - df["Existing HPP + 11%"]
    )
    df["Gross Profit (With DPP)"] = df["DPP"] - df["Existing HPP"]

    # Round all numeric columns
    list_round = [
        "Basic Price",
        "Discount",
        "Voucher",
        "Net Sales",
        "HPP",
        "Existing Basic Price",
        "Existing Discount",
        "Existing Voucher",
        "Existing Net Sales",
        "Value After Voucher",
        "Existing HPP",
        "DPP",
        "Gross Profit (With Hpp + 11%)",
        "Gross Profit (With DPP)",
        "Existing HPP + 11%",
    ]

    df[list_round] = df[list_round].round()

    # Clean up the columns
    df = df[
        [
            "Brand",
            "Marketplace",
            "monthnum",
            "Date",
            "Ship Date",
            "Delivered Date",
            "Status Pesanan",
            "Sub Status",
            "Status Pembatalan/Pengembalian",
            "Alasan Pembatalan",
            "Cancel By",
            "No Order",
            "Barcode",
            "CODEBARS",
            "ITEMID",
            "UMBRELLABRAND",
            "ALUCODE",
            "SEASON",
            "WORLD",
            "CATEGORY",
            "SUBCATEGORY",
            "YEARMONTH",
            "Qty Sold",
            "Basic Price",
            "Discount",
            "Voucher",
            "Net Sales",
            "HPP",
            "Subsidi",
            "Existing Basic Price",
            "Existing Discount",
            "Existing Voucher",
            "Existing Net Sales",
            "Value After Voucher",
            "Existing HPP",
            "Margin %",
            "Existing HPP + 11%",
            "DPP",
            "Gross Profit (With Hpp + 11%)",
            "Gross Profit (With DPP)",
            "Pickup & Courier",
            "Resi/Pin",
            "Delivery Fee",
            "Recipient Name",
            "Recipient Phone",
            "Recipient Address",
            "Recipient District",
            "Recipient City",
            "Recipient Postcode",
            "Status Sales",
        ]
    ]

    df["Ship Date"] = np.where(
        (df["Status Sales"] == "ONPROGRESS") & (df["Ship Date"].isna()),
        df["Date"],
        df["Ship Date"],
    )
    df["Delivered Date"] = np.where(
        (df["Status Sales"] == "DELIVERED") & (df["Delivered Date"].isna()),
        df["Ship Date"],
        df["Delivered Date"],
    )

    if df["Delivered Date"].isna().sum() > 0:
        df["Delivered Date"] = df["Delivered Date"].fillna(df["Ship Date"])
        df["Delivered Date"] = df["Delivered Date"].fillna(df["Date"])

    return df


def loader_sales_detail_online(df, year, month):
    path = f"s3://mega-dev-lake/Staging/Sales/sales_online/sales_detail_online/{year}/{month}/data.parquet"
    df.to_parquet(path)


def run_sales_detail_online(year, month):
    df = transform_online_detail(year=year, month=month)
    loader_sales_detail_online(df, year=year, month=month)
