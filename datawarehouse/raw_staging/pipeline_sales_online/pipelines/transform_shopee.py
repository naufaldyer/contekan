import pandas as pd
import numpy as np
import sys

sys.path.append("../common/")
from mapping_barcode import mapping_barcode
from loader import loader_marketplace


def transform_shopee(year, month, brand):
    if brand == "SZ":
        company = "mgl"
    else:
        company = "mpr"

    month_name = pd.to_datetime(month, format="%m").month_name()

    df_raw = pd.read_excel(
        f"s3://mp-users-report/e-commerce/raw/{brand}/{year}/{month_name}/Shopee_{month_name}_{brand}.xlsx",
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
    )

    df = df_raw[
        [
            "Waktu Pesanan Dibuat",
            "Waktu Pengiriman Diatur",
            "Waktu Pesanan Selesai",
            "Status Pesanan",
            "Status Pembatalan/ Pengembalian",
            "No. Pesanan",
            "Nomor Referensi SKU",
            "Jumlah",
            "Jumlah Produk di Pesan",
            "Harga Awal",
            "Total Diskon",
            "Diskon Dari Shopee",
            "Voucher Ditanggung Shopee",
            "Voucher Ditanggung Penjual",
            "Alasan Pembatalan",
            "Opsi Pengiriman",
            "No. Resi",
            "Ongkos Kirim Dibayar oleh Pembeli",
            "Estimasi Potongan Biaya Pengiriman",
            "Perkiraan Ongkos Kirim",
            "Nama Penerima",
            "No. Telepon",
            "Alamat Pengiriman",
            "Provinsi",
            "Kota/Kabupaten",
        ]
    ].rename(
        columns={
            "Waktu Pesanan Dibuat": "Date",
            "Waktu Pengiriman Diatur": "Ship Date",
            "Waktu Pesanan Selesai": "Delivered Date",
            "No. Pesanan": "No Order",
            "Nomor Referensi SKU": "Barcode",
            "Jumlah": "Qty Sold",
            "Harga Awal": "Basic Price",
            "Total Diskon": "Existing Discount",
            "Status Pembatalan/ Pengembalian": "Sub Status",
            "Opsi Pengiriman": "Pickup & Courier",
            "No. Resi": "Resi/Pin",
            "Ongkos Kirim Dibayar oleh Pembeli": "Delivery Fee",
            "Nama Penerima": "Recipient Name",
            "No. Telepon": "Recepient Phone",
            "Alamat Pengiriman": "Recipient Address",
            "Provinsi": "Recipient District",
            "Kota/Kabupaten": "Recipient City",
            # "Voucher Ditanggung Shopee": "Subsidi",
            "Diskon Dari Shopee": "Subsidi",
        }
    )

    # Standarisasi Kolom - kolom
    df["Brand"] = brand
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df["Ship Date"] = pd.to_datetime(df["Ship Date"]).dt.date
    df["Delivered Date"] = pd.to_datetime(df["Delivered Date"]).dt.date
    df["Status Pesanan"] = df["Status Pesanan"].str.upper()
    df["Recepient Phone"] = df["Recepient Phone"].astype(str)

    if df["Barcode"].str.startswith("\n").any():
        df["Barcode"] = df["Barcode"].str.replace("\n", "")

    if df["Barcode"].str.contains("\n").any():
        df["Barcode"] = df["Barcode"].str.split("\n").str[0]
        df["Barcode"] = df["Barcode"].apply(
            lambda x: x.replace("", "0000") if x == "" else x
        )
    else:
        pass

    # Standarisasi Kolom - kolom
    columns_to_change = [
        "Basic Price",
        "Existing Discount",
        "Voucher Ditanggung Penjual",
        "Delivery Fee",
        "Subsidi",
        # "Diskon Dari Shopee",
        "Estimasi Potongan Biaya Pengiriman",
        "Perkiraan Ongkos Kirim",
    ]
    df[columns_to_change] = df[columns_to_change].astype(str)
    df[columns_to_change] = df[columns_to_change].apply(
        lambda x: x.str.replace(".", "", regex=False)
    )
    df[columns_to_change] = df[columns_to_change].astype(int)
    df["Marketplace"] = "SHOPEE"
    df["Barcode"] = df["Barcode"].astype("str")
    df["Barcode"] = df["Barcode"].str.replace("\n", "")
    df["Voucher"] = df["Voucher Ditanggung Penjual"] / df["Jumlah Produk di Pesan"]

    df = mapping_barcode(
        df_marketplace=df,
        columns_marketplace="Barcode",
        company=company,
    )

    # Rumus
    df["Existing Basic Price"] = df["Basic Price"] * df["Qty Sold"]
    df["Discount"] = df["Existing Discount"] / df["Qty Sold"]
    df["Existing Voucher"] = df["Voucher"] * df["Qty Sold"]
    df["Net Sales"] = df["Basic Price"] - df["Discount"]
    df["Existing Net Sales"] = df["Existing Basic Price"] - df["Existing Discount"]
    df["Delivery Fee"] = (df["Delivery Fee"] / df["Jumlah Produk di Pesan"]) * df[
        "Qty Sold"
    ]

    df["Estimasi Potongan Biaya Pengiriman"] = (
        df["Estimasi Potongan Biaya Pengiriman"] / df["Jumlah Produk di Pesan"]
    ) * df["Qty Sold"]

    df["Perkiraan Ongkos Kirim"] = (
        df["Perkiraan Ongkos Kirim"] / df["Jumlah Produk di Pesan"]
    ) * df["Qty Sold"]

    df["Value After Voucher"] = (
        df["Existing Net Sales"]
        - df["Existing Voucher"]
        + df["Subsidi"]
        + df["Delivery Fee"]
        + df["Estimasi Potongan Biaya Pengiriman"]
        - df["Perkiraan Ongkos Kirim"]
    )
    df["Existing HPP"] = df["HPP"] * df["Qty Sold"]

    df.drop(
        columns=[
            "Jumlah Produk di Pesan",
            # "Diskon Dari Shopee",
            "Voucher Ditanggung Penjual",
            "Estimasi Potongan Biaya Pengiriman",
            "Perkiraan Ongkos Kirim",
        ],
        inplace=True,
    )
    return df


def run_shopee(year, month):
    list_brand = ["MZ", "MN", "MC", "MT", "ED", "SZ"]
    df = pd.concat(
        [transform_shopee(year=year, month=month, brand=brand) for brand in list_brand]
    )
    loader_marketplace(df, year, month)
