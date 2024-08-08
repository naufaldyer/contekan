import pandas as pd
import numpy as np
import sys
import boto3
import datetime
import openpyxl
import s3fs
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import warnings

warnings.filterwarnings("ignore")

# Import Function
sys.path.append("/home/ubuntu/dags/SALES_ONLINE_INGESTION")
from production_pipeline.processed.pipeline_sales_online.function_salesonlineingestion import (
    preprocessing_raw_blibli,
    preprocessing_raw_lazada,
    preprocessing_raw_shopee,
    preprocessing_raw_shopify,
    preprocessing_raw_tiktok,
    preprocessing_raw_tokopedia,
    preprocessing_raw_zalora,
)
from production_pipeline.processed.pipeline_sales_online.Sales_Online_Indipos_Temp import (
    posindi_temp_excel,
)
from production_pipeline.processed.pipeline_sales_online.function_report_sales_online_daily import (
    export,
)
from production_pipeline.processed.pipeline_sales_online.last_modified_ecom import *


def getInventable(company):
    newitable = pd.read_parquet(
        "s3://mega-lake/RawData/SOH/INVENTITEMINVENTSETUP/"
    ).query(f"DATAAREAID == '{company}'")
    itable = pd.read_parquet(
        f"s3://mega-lake/RawData/SOH/INVENTTABLE/{company}/data.parquet"
    )[
        [
            "DATAAREAID",
            "ITEMID",
            "CODEBARS",
            "ALUCODE",
            "ARTICLECODE",
            "UMBRELLABRAND",
            "CATEGORY",
            "SUBCATEGORY",
            "WORLD",
            "SEASON",
            "HPP",
            "SIZE_",
            "STYLE",
            "YEARMONTH",
            "BASICPRICE",
            "SALEPRICE",
            "PRICECATEGORY",
        ]
    ]

    itable = (
        itable.merge(newitable.drop(columns="DATAAREAID"), "left", on="ITEMID")
        .fillna({"STOPPED": 0, "RECID": 101010101, "INVENTDIMID": ""})
        .query("STOPPED == 0")
    )

    itable = itable.drop_duplicates(subset=["CODEBARS"], keep="last")

    return itable


# Function for Main Processing
def main_process(df, ecom, brand, month_report, df_invent):
    if ecom == "Lazada":
        return preprocessing_raw_lazada(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Tiktok":
        return preprocessing_raw_tiktok(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Tokopedia":
        return preprocessing_raw_tokopedia(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Zalora":
        return preprocessing_raw_zalora(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Shopee":
        return preprocessing_raw_shopee(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Shopify":
        return preprocessing_raw_shopify(df, df_invent, brand).assign(
            Month_Report=month_report
        )
    elif ecom == "Blibli":
        return preprocessing_raw_blibli(df, df_invent, brand).assign(
            Month_Report=month_report
        )


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
        # "SHIPPED" : "DELIVERED" if row['Sub Status'] == 'DELIVERED',
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


# def getAutomatedName():
#     today = datetime.datetime.now() - pd.DateOffset(months=1)
#     this_month = today.strftime("%B")
#     date = today.day
#     year = today.strftime("%Y")

#     if date <= 5:
#         last_month = today.replace(month=today.month - 1).strftime("%B")
#         month_name = [last_month, this_month]
#     else:
#         month_name = [this_month]
#     return year, month_name


def mapping_new_master_product(df):
    df_joined = pd.DataFrame()

    for company in ["mgl", "mpr"]:
        # print(company)
        if company == "mgl":
            df_process = df[df["Brand"] == "SZ"]
            # print(company)
        else:
            df_process = df[df["Brand"] != "SZ"]
            # print(company)

        df_master_product = pd.read_parquet(
            f"s3://mega-dev-lake/RawData/D365/Product/{company}/data.parquet"
        )
        df_master_product = df_master_product[
            df_master_product["PullStatus"] == "Current"
        ]
        df_master_product = df_master_product.drop_duplicates(
            subset="VariantNumber", keep="last"
        )
        df_master_product = df_master_product.drop_duplicates(
            subset="CodeBars", keep="last"
        )

        df_master_product_selected = df_master_product[
            [
                "CodeBars",
                "VariantNumber",
                "ItemId",
                "UmbrellaBrand",
                "SubBrand",
                "AluCode",
                "Season",
                "MonthYear",
                "World",
                "Category",
                "SubCategory",
                "HPP",
                "BasicPrice",
            ]
        ]

        df_master_product_selected["MonthYear"] = pd.to_datetime(
            df_master_product_selected["MonthYear"]
        )
        df_master_product_selected.columns = (
            df_master_product_selected.columns.str.upper()
        )
        df_master_product_selected = df_master_product_selected.rename(
            columns={"MONTHYEAR": "YEARMONTH"}
        )

        data = df_process.merge(
            df_master_product_selected, "left", on="CODEBARS", suffixes=("_drop", "")
        )
        data["HPP"] = np.where(data["HPP"] == 0, data["HPP_drop"], data["HPP"])
        data["Existing HPP"] = data["HPP"] * data["Qty Sold"]

        data = data[[x for x in data.columns if ("_drop") not in x]]

        df_joined = pd.concat([df_joined, data])

    return df_joined


def automated_date_name():
    today = pd.to_datetime(datetime.datetime.today())
    today_year = today.strftime("%Y")
    today_month = today.strftime("%B")

    last = today - pd.DateOffset(months=1)
    last_year = last.strftime("%Y")
    last_month = last.strftime("%B")

    if today.day == 1:
        dict_month = {"list_month": [last_year, last_month]}
    elif today.day > 1 and today.day <= 19:
        dict_month = {
            "list_last_month": [last_year, last_month],
            "list_month": [today_year, today_month],
        }
    else:
        dict_month = {"list_month": [today_year, today_month]}
    return dict_month


def preprocessing_online():
    # Check the last update
    getCheckLastUpdated()

    # Main Processing
    invent_mg = getInventable("mgl")
    invent_mp = getInventable("mpr")
    data_invent = {"mg": invent_mg, "mp": invent_mp}

    # Use the session to create an S3 client
    s3 = boto3.resource("s3", region_name="ap-southeast-3")
    bucket = s3.Bucket("mp-users-report")
    prefix_objs = bucket.objects.filter(Prefix="e-commerce/raw/")

    dict_month = automated_date_name()
    # dict_month = {"March": ["2024", "March"]}

    for key in dict_month.keys():
        year = dict_month[key][0]
        month = dict_month[key][1]

        datas = {}

        for obj in prefix_objs:
            if obj.key.endswith("csv") or obj.key.endswith("xlsx"):
                if obj.key.split("/")[2] == "SZ":
                    df_invent = pd.concat(data_invent)
                elif obj.key.split("/")[2] in {"MN", "MC", "MZ", "MT", "ED"}:
                    df_invent = data_invent["mp"]

                if obj.key.split("/")[3] == year and obj.key.split("/")[4] == month:
                    # Create variable
                    brand_ecom = obj.key.split("/")[2]
                    month_report = obj.key.split("/")[4]
                    ecom = obj.key.split("/")[5].split("_")[0]
                    if brand_ecom in ["MN", "MC", "MZ", "MT", "ED", "SZ"]:
                        try:
                            if obj.key.split("/")[5].split("_")[0] == "Tiktok":
                                try:
                                    datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                        main_process(
                                            pd.read_csv(
                                                f"s3://mp-users-report/{obj.key}"
                                            ),
                                            obj.key.split("/")[5].split("_")[0],
                                            brand_ecom,
                                            month_report,
                                            df_invent,
                                        )
                                    )

                                except:
                                    path = f"s3://mp-users-report/{obj.key}"
                                    data = s3fs.S3FileSystem().open(path, mode="rb")
                                    wb = openpyxl.load_workbook(data)
                                    sheet = wb["OrderSKUList"]

                                    data = []
                                    for row in sheet.iter_rows(values_only=True):
                                        data.append(row)

                                    df = pd.DataFrame(data[2:], columns=data[0])
                                    datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                        main_process(
                                            df,
                                            obj.key.split("/")[5].split("_")[0],
                                            brand_ecom,
                                            month_report,
                                            df_invent,
                                        )
                                    )

                            elif obj.key.split("/")[5].split("_")[0] == "Tokopedia":
                                datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                    main_process(
                                        pd.read_excel(
                                            f"s3://mp-users-report/{obj.key}",
                                            header=[4],
                                            dtype={"Nomor SKU": "str"},
                                        ),
                                        obj.key.split("/")[5].split("_")[0],
                                        brand_ecom,
                                        month_report,
                                        df_invent,
                                    )
                                )

                            elif obj.key.split("/")[5].split("_")[0] == "Shopee":
                                datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                    main_process(
                                        pd.read_excel(
                                            f"s3://mp-users-report/{obj.key}",
                                            dtype={
                                                "Nomor Referensi SKU": "str",
                                                "Voucher Ditanggung Penjual": "str",
                                                "Harga Awal": "str",
                                                "Total Diskon": "str",
                                                "Diskon Dari Shopee": "str",
                                                "Voucher Ditanggung Shopee": "str",
                                                "Voucher Ditanggung Penjual": "str",
                                                "Ongkos Kirim Dibayar oleh Pembeli": "str",
                                                "Estimasi Potongan Biaya Pengiriman": "str",
                                                "Perkiraan Ongkos Kirim": "str",
                                            },
                                        ),
                                        obj.key.split("/")[5].split("_")[0],
                                        brand_ecom,
                                        month_report,
                                        df_invent,
                                    )
                                )

                            elif obj.key.split("/")[5].split("_")[0] == "Shopify":
                                datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                    main_process(
                                        pd.read_csv(
                                            f"s3://mp-users-report/{obj.key}",
                                            dtype={
                                                "Lineitem sku": "str",
                                                "Subtotal": "float",
                                                "Total": "float",
                                                "Discount Amount": "float",
                                                "lineitem quantity": "int",
                                            },
                                        ),
                                        obj.key.split("/")[5].split("_")[0],
                                        brand_ecom,
                                        month_report,
                                        df_invent,
                                    )
                                )

                            elif obj.key.split("/")[5].split("_")[0] == "Zalora":
                                datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                    main_process(
                                        pd.read_excel(
                                            f"s3://mp-users-report/{obj.key}",
                                            dtype={"Seller SKU": "str"},
                                        ),
                                        obj.key.split("/")[5].split("_")[0],
                                        brand_ecom,
                                        month_report,
                                        df_invent,
                                    )
                                )

                            else:
                                datas[f"{brand_ecom}_{ecom}_{month_report}"] = (
                                    main_process(
                                        (
                                            pd.read_csv(
                                                f"s3://mp-users-report/{obj.key}"
                                            )
                                            if obj.key.endswith("csv")
                                            else pd.read_excel(
                                                f"s3://mp-users-report/{obj.key}"
                                            )
                                        ),
                                        obj.key.split("/")[5].split("_")[0],
                                        brand_ecom,
                                        month_report,
                                        df_invent,
                                    )
                                )
                        except Exception as e:
                            print(f"{obj.key} Error")
                            print(e)
                            print("----------------------------")
                    else:
                        pass

        print("Finished Main Processing")

        # Concat all data
        df_all = pd.concat(datas)
        df_all = mapping_new_master_product(df_all)
        df_all[df_all.select_dtypes(include=float).columns] = df_all[
            df_all.select_dtypes(include=float).columns
        ].fillna(0.0)
        df_all[df_all.select_dtypes(include=float).columns] = (
            df_all[df_all.select_dtypes(include=float).columns].astype(float).round(2)
        )

        # Standarisasi Date Columns
        df_all["Date"] = pd.to_datetime(df_all["Date"])
        df_all["monthnum"] = df_all["Date"].dt.strftime("%m")
        df_all["Month_In_Data"] = df_all["Date"].dt.strftime("%B")
        df_all["Day"] = df_all["Date"].dt.strftime("%d")
        df_all["Ship Date"] = pd.to_datetime(df_all["Ship Date"])
        df_all["Delivered Date"] = pd.to_datetime(df_all["Delivered Date"])

        # Query the data
        df_all = df_all[df_all["Month_Report"] == df_all["Month_In_Data"]]
        df_all = df_all[pd.to_datetime(df_all["Date"]).dt.strftime("%Y") == year]

        # Standarize Columns
        df_all["No Order"] = df_all["No Order"].astype(str)

        df_all["Sub Status"] = df_all["Sub Status"].fillna("")
        df_all["Sub Status"] = df_all["Sub Status"].astype(str)
        df_all["Sub Status"] = df_all["Sub Status"].str.upper()

        df_all["Alasan Pembatalan"] = df_all["Alasan Pembatalan"].fillna("")

        df_all["Status Pembatalan/Pengembalian"] = df_all[
            "Status Pembatalan/Pengembalian"
        ].fillna("")

        if "Cancel By" in df_all.columns:
            df_all["Cancel By"] = df_all["Cancel By"].fillna("")
        else:
            df_all["Cancel By"] = ""

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
            df_all[c] = df_all[c].astype(str)
            df_all[c] = df_all[c].fillna("")
            df_all[c] = df_all[c].replace("nan", "")

        # Standarize Columns
        df_all["Resi/Pin"] = df_all["Resi/Pin"].apply(lambda x: string_standarize(x))

        df_all["Recipient Name"] = df_all["Recipient Name"].apply(
            lambda x: clear_string(x)
        )

        df_all["Recipient Phone"] = df_all["Recipient Phone"].apply(
            lambda x: string_standarize(x)
        )

        df_all["Recipient Phone"] = np.where(
            df_all["Recipient Phone"].str.startswith("62"),
            "0" + df_all["Recipient Phone"].str[2:],
            df_all["Recipient Phone"],
        )

        df_all["Recipient Phone"] = np.where(
            ~df_all["Recipient Phone"].str.startswith("0"),
            "0" + df_all["Recipient Phone"],
            df_all["Recipient Phone"],
        )

        df_all["Recipient Phone"] = np.where(
            df_all["Recipient Phone"].str.len() < 10,
            "*******",
            df_all["Recipient Phone"],
        )

        df_all["Recipient Postcode"] = (
            df_all["Recipient Postcode"]
            .str.replace(".0", "", regex=False)
            .str.replace("'", "", regex=False)
        )

        df_all["Recipient Postcode"] = df_all["Recipient Postcode"].str.replace(
            "None", ""
        )

        # Added Status Sales
        df_all["Status Sales"] = df_all.apply(status_sales, axis=1)

        # Added Category Product
        df_all["SUBCATEGORY"] = df_all["SUBCATEGORY"].fillna("")
        df_all["Category Product"] = np.where(
            df_all["SUBCATEGORY"].str.contains(
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

        df_all = df_all.merge(
            margin_all[["Brand", "Marketplace", "Margin %"]],
            "left",
            on=["Brand", "Marketplace"],
        )

        df_all.loc[
            (~df_all["monthnum"].isin(["01", "02", "03", "04"]))
            & (df_all["Marketplace"] == "TIKTOK"),
            "Margin %",
        ] = 0.06500

        # df_all["Margin %"] = df_all.apply(
        #     lambda x: 0.114 if x["Brand"] == "SZ" else x["Margin %"], axis=1
        # )

        df_all["Existing HPP + 11%"] = df_all["Existing HPP"] + (
            df_all["Existing HPP"] * 0.11
        )
        df_all["DPP"] = df_all["Value After Voucher"] / 1.11
        df_all["Gross Profit (With Hpp + 11%)"] = (
            df_all["Value After Voucher"] - df_all["Existing HPP + 11%"]
        )
        df_all["Gross Profit (With DPP)"] = df_all["DPP"] - df_all["Existing HPP"]

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

        df_all[list_round] = df_all[list_round].round()

        # Clean up the columns
        df_all = df_all[
            [
                "Brand",
                "Marketplace",
                "Month_Report",
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
                "PRICECATEGORY",
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

        # Status sales tiktok
        df_all["Status Sales"] = np.where(
            (df_all["Status Pesanan"] == "SHIPPED")
            & (df_all["Sub Status"] == "DELIVERED"),
            "DELIVERED",
            df_all["Status Sales"],
        )

        df_all["Ship Date"] = np.where(
            (df_all["Status Sales"] == "ONPROGRESS") & (df_all["Ship Date"].isna()),
            df_all["Date"],
            df_all["Ship Date"],
        )
        df_all["Delivered Date"] = np.where(
            (df_all["Status Sales"] == "DELIVERED") & (df_all["Delivered Date"].isna()),
            df_all["Ship Date"],
            df_all["Delivered Date"],
        )

        if df_all["Delivered Date"].isna().sum() > 0:
            df_all["Delivered Date"] = df_all["Delivered Date"].fillna(
                df_all["Ship Date"]
            )
            df_all["Delivered Date"] = df_all["Delivered Date"].fillna(df_all["Date"])

        # Save to parquet
        month_num = df_all["monthnum"].unique()
        for m in month_num:
            df_all[df_all["monthnum"] == m].to_parquet(
                f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{m}/data.parquet"
            )

        print(f"Finish Preprocessing Data Detail {year} {month}")


# ------------------------------------------ Template ----------------------------------------------- #


# def getAutomatedDate():
#     today = datetime.datetime.now() - pd.DateOffset(months=1)

#     # today = datetime.datetime(2023, 10, 10)
#     this_month = today.strftime("%m")
#     last_month = today.replace(month=today.month - 1).strftime("%m")
#     date = today.day
#     year = today.strftime("%Y")

#     if date == 1:
#         month_num = [last_month]
#     elif date > 1 & date < 5:
#         month_num = [last_month, this_month]
#     else:
#         month_num = [this_month]
#     return year, month_num


def automated_date():
    today = pd.to_datetime(datetime.datetime.today())
    today_year = today.strftime("%Y")
    today_month = today.strftime("%m")

    last = today - pd.DateOffset(months=1)
    last_year = last.strftime("%Y")
    last_month = last.strftime("%m")

    if today.day == 1:
        dict_month = {"list_month": [last_year, last_month.zfill(2)]}
    elif today.day > 1 and today.day <= 19:
        dict_month = {
            "list_last_month": [last_year, last_month.zfill(2)],
            "list_month": [today_year, today_month.zfill(2)],
        }
    elif today.day > 6:
        dict_month = {"list_month": [today_year, today_month.zfill(2)]}
    return dict_month


def getMasterOnline(month_name):
    master = pd.read_parquet(
        "s3://mega-dev-lake/Staging/Master/Master Store/2024/data.parquet"
    ).query(f"month == '{month_name}'")

    master_online = master[master["main_channel"] == "ONLINE"]
    master_online["marketplace"] = master_online["stdname"].str.split(" ").str.get(2)
    master_online["brand"] = master_online["stdname"].str.split(" ").str.get(1)
    master_online["marketplace"] = master_online["marketplace"].fillna("")

    master_online["marketplace"] = np.where(
        master_online["marketplace"].isin(
            [
                "MANZONESTORE.ID",
                "MINIMALSTORE.ID",
                "MOCSTORE.ID",
                "MATAHARISTORE.COM",
                "WEBSITE",
            ]
        ),
        "SHOPIFY",
        master_online["marketplace"],
    )

    master_online = master_online[master_online["openstatus"] == "OPEN"]
    return master_online


def send_to_excel():
    # Condition
    # year, month_num = getAutomatedDate()

    dict_month = automated_date()
    # dict_month = {
    #     "January": ["2023", "01"],
    #     "February": ["2023", "02"],
    # }

    for key in dict_month.keys():
        year = dict_month[key][0]
        b = dict_month[key][1]
        month_name = pd.to_datetime(b, format="%m").month_name()
        master = getMasterOnline("April")

        # Load Data
        df_all = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{b}/data.parquet",
            engine="pyarrow",
        ).query("`Status Sales` != 'CANCELED'")
        list_brand = df_all["Brand"].unique()
        month = df_all["Month_Report"].unique()[0]

        for x in list_brand:
            df_out = df_all[(df_all["Brand"] == x)]
            day = pd.to_datetime(df_out["Date"].max()).strftime("%d")
            day_min = pd.to_datetime(df_out["Date"].min()).strftime("%d")
            df_out.drop(columns=["Status Sales"]).to_excel(
                f"s3://mp-users-report/e-commerce/download/{x}/{year}/{month}/Daily Sales {x} {day_min} - {day} {month} {year}.xlsx",
                index=False,
            )

        df_all.columns = df_all.columns.str.lower()
        df_all = df_all[
            [
                "brand",
                "marketplace",
                "date",
                "ship date",
                "delivered date",
                "status pesanan",
                "sub status",
                "no order",
                "barcode",
                "qty sold",
                "basic price",
                "discount",
                "voucher",
                "net sales",
                "hpp",
                "existing discount",
                "existing voucher",
                "existing net sales",
                "value after voucher",
                "existing hpp",
                "codebars",
                "umbrellabrand",
                "alucode",
                "season",
                "category",
                "subcategory",
                "pricecategory",
                "yearmonth",
            ]
        ]

        df_all = master[["brand", "marketplace", "axcode"]].merge(
            df_all, "right", on=["brand", "marketplace"]
        )

        df_all.query("brand == 'SZ'").to_excel(
            f"s3://report-deliverables/sales-report/sales_detail_new/{year}/{b}/Online Mitrel/data.xlsx",
            index=False,
        )

        df_all.query("brand != 'SZ'").to_excel(
            f"s3://report-deliverables/sales-report/sales_detail_new/{year}/{b}/Online MP/data.xlsx",
            index=False,
        )
        print(f"finish send to excel {year} {month}")


def send_to_excel_posindi():
    # Condition
    dict_month = automated_date()
    for key in dict_month.keys():
        year = dict_month[key][0]
        month = dict_month[key][1]

        posindi_temp_excel(year, month)
        print(f"finish send to excel posindie {year} {month}")


def report_daily_online():
    today = datetime.datetime.today()

    report_date = pd.to_datetime(
        (datetime.datetime.now() + datetime.timedelta(hours=7)).date()
        - pd.DateOffset(days=1)
    )

    dict_month = automated_date()
    for key in dict_month.keys():
        year = dict_month[key][0]
        month = dict_month[key][1]

        export(month, report_date)


with DAG(
    dag_id="sales_online",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["sales", "online"],
) as dag:
    sales_online_ingestion = PythonOperator(
        task_id="sales_online_raw_datatemp",
        python_callable=preprocessing_online,
        provide_context=True,
    )

    sales_online_send_to_excel = PythonOperator(
        task_id="sales_online_datatemp_to_excel",
        python_callable=send_to_excel,
        provide_context=True,
    )

    sales_online_posindi_temp_to_excel = PythonOperator(
        task_id="sales_online_posindi_temp_to_excel",
        python_callable=send_to_excel_posindi,
        provide_context=True,
    )

    sales_online_daily_report = PythonOperator(
        task_id="report_daily_online",
        python_callable=report_daily_online,
        provide_context=True,
    )

    # Trigger Dags
    trigger_main = DummyOperator(
        task_id="trigger_main",
    )

    trigger_sales_accumulation = TriggerDagRunOperator(
        task_id="trigger_sales_accumulation", trigger_dag_id="sales_accumulation"
    )

    trigger_sales_merchandise = TriggerDagRunOperator(
        task_id="trigger_sales_merchandise", trigger_dag_id="sales_online_reporting"
    )

    # Task Flow
    (
        sales_online_ingestion
        >> trigger_main
        >> [
            sales_online_send_to_excel,
            sales_online_posindi_temp_to_excel,
            sales_online_daily_report,
            trigger_sales_accumulation,
            trigger_sales_merchandise,
        ]
    )
