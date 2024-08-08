import pandas as pd
import numpy as np
import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_tokopedia(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"

    month_name = pd.to_datetime(month, format="%m").month_name()

    df_raw = pd.read_excel(
        f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Tokopedia_{month_name}_{brand}.xlsx",
        header=[4],
        dtype={"Nomor SKU": "str"},
    )

    df = df_raw[
        [
            "Nomor Invoice",
            "Tanggal Pembayaran",
            "Status Terakhir",
            "Tanggal Pesanan Selesai",
            "Tanggal Pengiriman Barang",
            "Nomor SKU",
            "Jumlah Produk Dibeli",
            "Harga Awal (IDR)",
            "Harga Jual (IDR)",
            "Nilai Kupon Toko Terpakai (IDR)",
            "Nama Kurir",
            "No Resi / Kode Booking",
            "Nama Penerima",
            "No Telp Penerima",
            "Alamat Pengiriman",
            "Provinsi",
            "Kota",
        ]
    ].rename(
        columns={
            "Nomor Invoice": "No Order",
            "Tanggal Pembayaran": "Date",
            "Status Terakhir": "Status Pesanan",
            "Tanggal Pengiriman Barang": "Ship Date",
            "Tanggal Pesanan Selesai": "Delivered Date",
            "Nomor SKU": "Barcode",
            "Jumlah Produk Dibeli": "Qty Sold",
            "Harga Awal (IDR)": "Basic Price",
            "Harga Jual (IDR)": "Sale Price",
            "Nilai Kupon Toko Terpakai (IDR)": "Total Voucher",
            "Nama Kurir": "Pickup & Courier",
            "No Resi / Kode Booking": "Resi/Pin",
            "Nama Penerima": "Recipient Name",
            "No Telp Penerima": "Recipient Phone",
            "Alamat Pengiriman": "Recipient Address",
            "Provinsi": "Recipient District",
            "Kota": "Recipient City",
        }
    )

    df = df.dropna(subset="No Order")
    df["Brand"] = brand
    df["Marketplace"] = "TOKOPEDIA"
    df["Barcode"] = df["Barcode"].astype(str)
    df["Recipient Address"] = df["Recipient Address"].astype(str)
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y %H:%M:%S").dt.strftime(
        "%Y-%m-%d"
    )
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], format="%d-%m-%Y").dt.strftime(
        "%Y-%m-%d"
    )
    df["Delivered Date"] = pd.to_datetime(
        df["Delivered Date"], format="%d-%m-%Y"
    ).dt.strftime("%Y-%m-%d")

    df["Status Pesanan"] = df["Status Pesanan"].astype(str)
    df["Alasan Pembatalan"] = df["Status Pesanan"].apply(
        lambda x: x.split("-\n")[1] if len(x.split(" -\n")) > 1 else ""
    )
    df["Status Pesanan"] = df["Status Pesanan"].apply(lambda x: x.split(" -\n")[0])

    # Status yang tidak dibawa
    df["Status Pesanan"] = df["Status Pesanan"].str.upper()

    df = mapping_barcode(
        df_marketplace=df, columns_marketplace="Barcode", company="mpr"
    )

    # Ambil Nilai Voucher
    df_tokpedvoucher = df.groupby(["No Order"], as_index=False).agg(
        {"Qty Sold": "sum", "Total Voucher": "mean"}
    )
    df_tokpedvoucher["Voucher"] = (
        df_tokpedvoucher["Total Voucher"] / df_tokpedvoucher["Qty Sold"]
    )
    df = df.merge(
        df_tokpedvoucher[["No Order", "Voucher"]], "left", on=["No Order"]
    ).drop(columns=["Total Voucher"])

    # Rumus
    df["Discount"] = df["Basic Price"] - df["Sale Price"]

    df["Net Sales"] = df["Sale Price"]

    df["Existing Basic Price"] = df["Basic Price"] * df["Qty Sold"]

    df["Existing Discount"] = df["Discount"] * df["Qty Sold"]

    df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]

    df["Existing Net Sales"] = df["Existing Basic Price"] - df["Existing Discount"]

    df["Value After Voucher"] = df["Existing Net Sales"] - df["Existing Voucher"]

    df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

    df = df.drop(columns=["Sale Price"])

    return df


def run_tokopedia(year, month):
    list_brand = ["MZ", "MN", "MC", "MT", "ED", "SZ"]
    df = pd.concat(
        [
            transform_tokopedia(year=year, month=month, brand=brand)
            for brand in list_brand
        ]
    )
    loader_marketplace(df, year, month)
