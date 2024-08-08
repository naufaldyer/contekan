import pandas as pd
import numpy as np


def join_codebars(df_online, df_invent):
    if "SZ" in df_online["Brand"].unique():
        invent_mg = df_invent[df_invent["DATAAREAID"] == "mgl"]
        invent_mp = df_invent[df_invent["DATAAREAID"] == "mpr"]

        invent_mp = invent_mp[
            [
                "CODEBARS",
                "ITEMID",
                "UMBRELLABRAND",
                "ALUCODE",
                "WORLD",
                "SEASON",
                "CATEGORY",
                "SUBCATEGORY",
                "PRICECATEGORY",
                "YEARMONTH",
                "HPP",
                "BASICPRICE",
            ]
        ]

        invent_mg = invent_mg[
            [
                "CODEBARS",
                "ITEMID",
                "UMBRELLABRAND",
                "ALUCODE",
                "WORLD",
                "SEASON",
                "CATEGORY",
                "SUBCATEGORY",
                "PRICECATEGORY",
                "YEARMONTH",
                "HPP",
                "BASICPRICE",
            ]
        ]

        # Join for item mg with bacode / codebars ---------------------------------
        df_online = df_online.merge(
            invent_mg, "left", left_on=["Barcode"], right_on=["CODEBARS"]
        )

        # Join for item mg with Alucode -------------------------------------------
        df_online = df_online.merge(
            invent_mg,
            how="left",
            left_on=["Barcode"],
            right_on="ALUCODE",
            suffixes=["_x", "_y"],
            indicator=True,
        )
        df_online["_merge"] = df_online["_merge"].map(
            {"right_only": "not alu", "left_only": "not alu", "both": "alu"}
        )

        df_online[
            [
                "CODEBARS",
                "ITEMID",
                "UMBRELLABRAND",
                "ALUCODE",
                "WORLD",
                "SEASON",
                "CATEGORY",
                "SUBCATEGORY",
                "PRICECATEGORY",
                "YEARMONTH",
                "HPP",
                "BASICPRICE",
            ]
        ] = df_online[
            [
                "CODEBARS_x",
                "ITEMID_x",
                "UMBRELLABRAND_x",
                "ALUCODE_x",
                "WORLD_x",
                "SEASON_x",
                "CATEGORY_x",
                "SUBCATEGORY_x",
                "PRICECATEGORY_x",
                "YEARMONTH_x",
                "HPP_x",
                "BASICPRICE_x",
            ]
        ].copy()

        columns_to_check = [
            "CODEBARS",
            "ITEMID",
            "UMBRELLABRAND",
            "ALUCODE",
            "WORLD",
            "SEASON",
            "CATEGORY",
            "SUBCATEGORY",
            "PRICECATEGORY",
            "YEARMONTH",
            "HPP",
            "BASICPRICE",
        ]

        # replace column again
        columns_to_replace = [
            "CODEBARS_y",
            "ITEMID_y",
            "UMBRELLABRAND_y",
            "ALUCODE_y",
            "WORLD_y",
            "SEASON_y",
            "CATEGORY_y",
            "SUBCATEGORY_y",
            "PRICECATEGORY_y",
            "YEARMONTH_y",
            "HPP_y",
            "BASICPRICE_y",
        ]

        # Iterate through each pair of columns
        for check_col, replace_col in zip(columns_to_check, columns_to_replace):
            df_online.loc[df_online["_merge"] == "alu", check_col] = df_online.loc[
                df_online["_merge"] == "alu", replace_col
            ]

        # Join for item mp with barcode --------------------------------------------
        df_online = df_online.merge(
            invent_mp,
            how="left",
            left_on=["Barcode"],
            right_on="CODEBARS",
            suffixes=[None, "_z"],
        )

        # replace column again
        columns_to_replace = [
            "CODEBARS_z",
            "ITEMID_z",
            "UMBRELLABRAND_z",
            "ALUCODE_z",
            "WORLD_z",
            "SEASON_z",
            "CATEGORY_z",
            "SUBCATEGORY_z",
            "PRICECATEGORY_z",
            "YEARMONTH_z",
            "HPP_z",
            "BASICPRICE_z",
        ]

        # Iterate through each pair of columns
        for check_col, replace_col in zip(columns_to_check, columns_to_replace):
            df_online.loc[df_online["HPP"].isnull(), check_col] = df_online.loc[
                df_online["HPP"].isnull(), replace_col
            ]

        # Drop Column ------------------------------------------------------------
        df_online = df_online.drop(
            columns=[
                "CODEBARS_x",
                "ITEMID_x",
                "UMBRELLABRAND_x",
                "ALUCODE_x",
                "SEASON_x",
                "WORLD_x",
                "CATEGORY_x",
                "SUBCATEGORY_x",
                "PRICECATEGORY_x",
                "YEARMONTH_x",
                "HPP_x",
                "BASICPRICE_x",
                "CODEBARS_y",
                "ITEMID_y",
                "UMBRELLABRAND_y",
                "ALUCODE_y",
                "SEASON_y",
                "WORLD_y",
                "CATEGORY_y",
                "SUBCATEGORY_y",
                "PRICECATEGORY_y",
                "YEARMONTH_y",
                "HPP_y",
                "BASICPRICE_y",
                "CODEBARS_z",
                "ITEMID_z",
                "UMBRELLABRAND_z",
                "ALUCODE_z",
                "SEASON_z",
                "WORLD_z",
                "CATEGORY_z",
                "SUBCATEGORY_z",
                "PRICECATEGORY_z",
                "YEARMONTH_z",
                "HPP_z",
                "BASICPRICE_z",
                "_merge",
            ]
        )

        df_online["CODEBARS"] = df_online["CODEBARS"].astype(str)
        invent_mp["CODEBARS"] = invent_mp["CODEBARS"].astype(str)

        df_online.loc[df_online["CODEBARS"].isnull(), "CODEBARS"] = df_online.loc[
            df_online["CODEBARS"].isnull(), "CODEBARS"
        ].apply(lambda x: x[1:] if x.startswith("0") else "0" + x)

        # Join again with inventtable mg --------------------------------------
        df_online = df_online.merge(
            invent_mg,
            how="left",
            on="CODEBARS",
            suffixes=[None, "_x"],
        )

        # replace column again
        columns_to_replace = [
            "ITEMID_x",
            "UMBRELLABRAND_x",
            "ALUCODE_x",
            "SEASON_x",
            "WORLD_x",
            "CATEGORY_x",
            "SUBCATEGORY_x",
            "PRICECATEGORY_x",
            "YEARMONTH_x",
            "HPP_x",
            "BASICPRICE_x",
        ]

        # Iterate through each pair of columns
        for check_col, replace_col in zip(columns_to_check, columns_to_replace):
            df_online.loc[df_online["HPP"].isnull(), check_col] = df_online.loc[
                df_online["HPP"].isnull(), replace_col
            ]

        # Join again with inventtable mp -----------------------------------------
        df_online = df_online.merge(
            invent_mp,
            how="left",
            on="CODEBARS",
            suffixes=[None, "_y"],
        )

        # replace column again
        columns_to_replace = [
            "ITEMID_y",
            "UMBRELLABRAND_y",
            "ALUCODE_y",
            "SEASON_y",
            "WORLD_y",
            "CATEGORY_y",
            "SUBCATEGORY_y",
            "PRICECATEGORY_y",
            "YEARMONTH_y",
            "HPP_y",
            "BASICPRICE_y",
        ]

        # Iterate through each pair of columns
        for check_col, replace_col in zip(columns_to_check, columns_to_replace):
            df_online.loc[df_online["HPP"].isnull(), check_col] = df_online.loc[
                df_online["HPP"].isnull(), replace_col
            ]

        # Drop Column ---------------------------------------------------------------
        df_online = df_online.drop(
            columns=[
                "ITEMID_x",
                "UMBRELLABRAND_x",
                "ALUCODE_x",
                "SEASON_x",
                "WORLD_x",
                "CATEGORY_x",
                "SUBCATEGORY_x",
                "PRICECATEGORY_x",
                "YEARMONTH_x",
                "HPP_x",
                "BASICPRICE_x",
                "ITEMID_y",
                "UMBRELLABRAND_y",
                "ALUCODE_y",
                "WORLD_y",
                "SEASON_y",
                "CATEGORY_y",
                "SUBCATEGORY_y",
                "PRICECATEGORY_y",
                "YEARMONTH_y",
                "HPP_y",
                "BASICPRICE_y",
            ]
        )

        # Join with barcode + 0
        df_done = df_online[~(df_online["CODEBARS"].isna())]
        df_online = df_online[df_online["CODEBARS"].isna()]
        df_online = df_online.drop(
            columns=[
                "ITEMID",
                "UMBRELLABRAND",
                "ALUCODE",
                "SEASON",
                "WORLD",
                "CATEGORY",
                "SUBCATEGORY",
                "PRICECATEGORY",
                "YEARMONTH",
                "HPP",
                "BASICPRICE",
            ]
        )
        df_online["CODEBARS"] = "0" + df_online["Barcode"]
        df_online = df_online.merge(
            invent_mg,
            "left",
            on=["CODEBARS"],
        )

        df_online = pd.concat([df_done, df_online])

    else:
        df_online = df_online.merge(
            df_invent["CODEBARS"], "left", left_on=["Barcode"], right_on=["CODEBARS"]
        )

        df_online.loc[df_online["CODEBARS"].isnull(), "CODEBARS"] = df_online.loc[
            df_online["CODEBARS"].isnull(), "Barcode"
        ].apply(lambda x: x if x.startswith("0") else "0" + x)
        # df_online["CODEBARS"] = df_online["CODEBARS"].fillna("0" + df_online["Barcode"])
        df_online["Barcode"] = df_online["CODEBARS"]

        df_online = df_online.merge(
            df_invent[
                [
                    "ITEMID",
                    "UMBRELLABRAND",
                    "CODEBARS",
                    "ALUCODE",
                    "SEASON",
                    "YEARMONTH",
                    "WORLD",
                    "CATEGORY",
                    "SUBCATEGORY",
                    "PRICECATEGORY",
                    "HPP",
                    "BASICPRICE",
                ]
            ],
            "left",
            on=["CODEBARS"],
        )

    return df_online


# For E-commerce Shopee
def preprocessing_raw_shopee(df_shopee, df_invent, brand):
    df_shopee1 = df_shopee[
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
            "No. Telepon": "Recpient Phone",
            "Alamat Pengiriman": "Recipient Address",
            "Provinsi": "Recipient District",
            "Kota/Kabupaten": "Recipient City",
            # "Voucher Ditanggung Shopee": "Subsidi",
            "Diskon Dari Shopee": "Subsidi"
        }
    )

    df_shopee1["Brand"] = brand
    df_shopee1["Date"] = pd.to_datetime(df_shopee1["Date"]).dt.date
    df_shopee1["Ship Date"] = pd.to_datetime(df_shopee1["Ship Date"]).dt.date
    df_shopee1["Delivered Date"] = pd.to_datetime(df_shopee1["Delivered Date"]).dt.date

    if df_shopee1["Barcode"].str.startswith("\n").any():
        df_shopee1["Barcode"] = df_shopee1["Barcode"].str.replace("\n", "")

    if df_shopee1["Barcode"].str.contains("\n").any():
        df_shopee1["Barcode"] = df_shopee1["Barcode"].str.split("\n").str[0]
        df_shopee1["Barcode"] = df_shopee1["Barcode"].apply(
            lambda x: x.replace("", "0000") if x == "" else x
        )
    else:
        pass

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
    df_shopee1[columns_to_change] = df_shopee1[columns_to_change].astype(str)
    df_shopee1[columns_to_change] = df_shopee1[columns_to_change].apply(
        lambda x: x.str.replace(".", "", regex=False)
    )

    # df_shopee1["Basic Price"] = [
    #     str(value).ljust(6, "0") for value in df_shopee1["Basic Price"]
    # ]

    df_shopee1[columns_to_change] = df_shopee1[columns_to_change].astype(int)
    df_shopee1["Marketplace"] = "SHOPEE"
    df_shopee1["Barcode"] = df_shopee1["Barcode"].astype("str")
    df_shopee1["Barcode"] = df_shopee1["Barcode"].str.replace('\n', '')
    df_shopee1["Voucher"] = (
        df_shopee1["Voucher Ditanggung Penjual"] / df_shopee1["Jumlah Produk di Pesan"]
    )

    # replace value
    to_replace = {
        "BZY23-07FLMASS020MA-L": "0100000005408",
        "BZY23-07FLMASS021WH-M": "0100000005446",
        "BZY23-07FLMASS021WH-L": "0100000005453",
        "BZY23-07FLMASS022WH-XXL": "0100000005521",
        "BZY23-07FLMASS021WH-L": "0100000005453",
        "BZY23-07FLMASS023WH-L": "0100000005552",
        "BZY23-07FLMASS024NA-L": "0100000005606",
        "BZY23-07FLMASS021WH-M": "0100000005446",
        "BZY23-07FLMASS022WH-XXL": "0100000005521",
        "BZY23-07FLMASS020MA-XL": "0100000005415",
        "BZY23-07FLMASS021WH-XL": "0100000005460",
        "BZY23-07FLMASS022WH-L": "0100000005507",
    }

    df_shopee1["Barcode"] = df_shopee1["Barcode"].replace(to_replace)

    df_shopee1 = join_codebars(df_shopee1, df_invent)

    df_shopee1["Status Pesanan"] = df_shopee1["Status Pesanan"].str.upper()

    # Rumus
    df_shopee1["Existing Basic Price"] = (
        df_shopee1["Basic Price"] * df_shopee1["Qty Sold"]
    )
    df_shopee1["Discount"] = df_shopee1["Existing Discount"] / df_shopee1["Qty Sold"]
    df_shopee1["Existing Voucher"] = df_shopee1["Voucher"] * df_shopee1["Qty Sold"]
    df_shopee1["Net Sales"] = df_shopee1["Basic Price"] - df_shopee1["Discount"]
    df_shopee1["Existing Net Sales"] = (
        df_shopee1["Existing Basic Price"] - df_shopee1["Existing Discount"]
    )

    # df_shopee1["Diskon Dari Shopee"] = (
    #     df_shopee1["Diskon Dari Shopee"] / df_shopee1["Jumlah Produk di Pesan"]
    # ) * df_shopee1["Qty Sold"]

    # df_shopee1["Subsidi"] = (
    #     df_shopee1["Subsidi"] / df_shopee1["Jumlah Produk di Pesan"]
    # ) * df_shopee1["Qty Sold"]

    df_shopee1["Delivery Fee"] = (
        df_shopee1["Delivery Fee"] / df_shopee1["Jumlah Produk di Pesan"]
    ) * df_shopee1["Qty Sold"]

    df_shopee1["Estimasi Potongan Biaya Pengiriman"] = (
        df_shopee1["Estimasi Potongan Biaya Pengiriman"]
        / df_shopee1["Jumlah Produk di Pesan"]
    ) * df_shopee1["Qty Sold"]

    df_shopee1["Perkiraan Ongkos Kirim"] = (
        df_shopee1["Perkiraan Ongkos Kirim"] / df_shopee1["Jumlah Produk di Pesan"]
    ) * df_shopee1["Qty Sold"]

    df_shopee1["Value After Voucher"] = (
        df_shopee1["Existing Net Sales"]
        - df_shopee1["Existing Voucher"]
        + df_shopee1["Subsidi"]
    )
    df_shopee1["Existing HPP"] = df_shopee1["HPP"] * df_shopee1["Qty Sold"]

    df_shopee1.drop(
        columns=[
            "Jumlah Produk di Pesan",
            # "Diskon Dari Shopee",
            "Voucher Ditanggung Penjual",
            "Estimasi Potongan Biaya Pengiriman",
            "Perkiraan Ongkos Kirim",
        ],
        inplace=True,
    )

    return df_shopee1


# For E-commerce Lazada
def preprocessing_raw_lazada(df_lazada, df_invent, brand):
    df_lazada1 = df_lazada[
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

    df_lazada1["Brand"] = brand

    try:
        df_lazada1["Date"] = pd.to_datetime(df_lazada1["Date"]).dt.date
        df_lazada1["Ship Date"] = pd.to_datetime(df_lazada1["Ship Date"]).dt.date
        df_lazada1["Delivered Date"] = pd.to_datetime(
            df_lazada1["Delivered Date"]
        ).dt.date
    except:
        df_lazada1["Date"] = pd.to_datetime(
            df_lazada1["Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df_lazada1["Ship Date"] = pd.to_datetime(
            df_lazada1["Ship Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df_lazada1["Delivered Date"] = pd.to_datetime(
            df_lazada1["Delivered Date"], format="%d %m %Y %H:%M:%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    df_lazada1["Barcode"] = df_lazada1["Barcode"].astype("str")

    df_lazada1["Voucher"] = df_lazada1["Voucher"].fillna(0)
    df_lazada1["Voucher"] = df_lazada1["Voucher"].apply(
        lambda x: x * -1 if x < 0 else x
    )
    df_lazada1["Marketplace"] = "LAZADA"
    df_lazada1["Qty Sold"] = 1

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

    df_lazada1["Barcode"] = df_lazada1["Barcode"].replace(to_replace)
    df_lazada1 = join_codebars(df_lazada1, df_invent)
    df_lazada1["Basic Price"] = df_lazada1["BASICPRICE"]

    df_lazada1["Status Pesanan"] = df_lazada1["Status Pesanan"].str.upper()

    if not df_lazada1["Status Pembatalan/Pengembalian"].isnull().all():
        df_lazada1["Status Pembatalan/Pengembalian"] = df_lazada1[
            "Status Pembatalan/Pengembalian"
        ].str.upper()

    # Rumus
    df_lazada1["Discount"] = df_lazada1["Basic Price"] - df_lazada1["Net Sales"]
    df_lazada1["Existing Basic Price"] = (
        df_lazada1["Basic Price"] * df_lazada1["Qty Sold"]
    )
    df_lazada1["Existing Discount"] = df_lazada1["Discount"] * df_lazada1["Qty Sold"]
    df_lazada1["Existing Net Sales"] = df_lazada1["Net Sales"] * df_lazada1["Qty Sold"]
    df_lazada1["Existing Voucher"] = df_lazada1["Voucher"] * df_lazada1["Qty Sold"]
    df_lazada1["Value After Voucher"] = (
        df_lazada1["Existing Net Sales"] - df_lazada1["Existing Voucher"]
    )
    df_lazada1["Existing HPP"] = df_lazada1["HPP"] * df_lazada1["Qty Sold"]

    return df_lazada1


# For E-commerce Tiktok
def preprocessing_raw_tiktok(df_tiktok, df_invent, brand):
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
        df_tiktok[col] = df_tiktok[col].fillna("IDR 0")
        df_tiktok[col] = (
            df_tiktok[col]
            .astype(str)
            .str.replace("IDR ", "")
            .str.replace(".", "", regex=False)
        ).astype(int)

    df_tiktok["Detail Address"] = df_tiktok["Detail Address"].str.replace("=", "")
    df_tiktok["Seller SKU"] = df_tiktok["Seller SKU"].astype(str)
    if df_tiktok["Seller SKU"].str.contains(".").any():
        df_tiktok["Seller SKU"] = df_tiktok["Seller SKU"].str.split(".").str[0]
    else:
        pass

    df_tiktok = df_tiktok[
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
    df_tiktok[numeric_columns] = df_tiktok[numeric_columns].fillna(0).astype(int)

    df_tiktok["Brand"] = brand

    df_tiktok["Date"] = df_tiktok["Date"].str.replace("\t", "")
    df_tiktok["Ship Date"] = df_tiktok["Ship Date"].str.replace("\t", "")
    df_tiktok["Delivered Date"] = df_tiktok["Delivered Date"].str.replace("\t", "")

    df_tiktok["Date"] = pd.to_datetime(
        df_tiktok["Date"], format="%d/%m/%Y %H:%M:%S"
    ).dt.strftime("%Y-%m-%d")
    df_tiktok["Ship Date"] = pd.to_datetime(
        df_tiktok["Ship Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df_tiktok["Delivered Date"] = pd.to_datetime(
        df_tiktok["Delivered Date"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df_tiktok["Barcode"] = df_tiktok["Barcode"].astype(str)
    df_tiktok["Marketplace"] = "TIKTOK"
    df_tiktok["Voucher"] = 0
    df_tiktok["Existing Voucher"] = 0

    df_tiktok = join_codebars(df_tiktok, df_invent)

    # Status dan Sub Status yang tidak dibawa

    # status_tiktok = ["Canceled", "Unpaid"]

    # df_tiktok = df_tiktok[~(df_tiktok["Status Pesanan"].isin(status_tiktok))]
    df_tiktok["Status Pesanan"] = df_tiktok["Status Pesanan"].str.upper()

    # Rumus
    # df_tiktok["Existing Discount"] = (df_tiktok["SKU Seller Discount"] + df_tiktok["SKU Platform Discount"])
    # Moc SKU Plarform discount nya dijadiin discount

    # df_tiktok["Existing Net Sales"] = (
    #     df_tiktok["Existing Basic Price"] - df_tiktok["Existing Discount"]
    # )

    df_tiktok["Existing Discount"] = df_tiktok["SKU Seller Discount"]
    df_tiktok["Discount"] = df_tiktok["Existing Discount"] / df_tiktok["Qty Sold"]
    df_tiktok["Net Sales"] = df_tiktok["Existing Net Sales"] / df_tiktok["Qty Sold"]
    df_tiktok["Value After Voucher"] = (
        df_tiktok["Existing Net Sales"]
        - df_tiktok["Existing Voucher"]
        + df_tiktok["Subsidi"]
    )
    df_tiktok["Existing HPP"] = df_tiktok["HPP"] * df_tiktok["Qty Sold"]

    df_tiktok = df_tiktok.drop(columns=["SKU Seller Discount", "Order Amount"])

    return df_tiktok


# For E-commerce Shopify/Website
def preprocessing_raw_shopify(df_shopify, df_invent, brand):
    df_shopify["Lineitem sku"] = (
        df_shopify["Lineitem sku"].astype("str").str.split(".").str[0]
    )
    df_shopify[["Email", "Shipping Province Name"]] = df_shopify[
        ["Email", "Shipping Province Name"]
    ].fillna("")
    df_header = df_shopify.groupby(
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
    df_detail = df_shopify[
        ["Name", "Lineitem quantity", "Lineitem price", "Lineitem sku"]
    ]

    # Voucher
    df_header = df_header.merge(
        df_detail.groupby(["Name"], as_index=False)[["Lineitem quantity"]].sum(),
        "left",
        on=["Name"],
    )
    df_header["Voucher"] = df_header["Discount Amount"] / df_header["Lineitem quantity"]
    df_shopify = df_header.rename(
        columns={"Lineitem quantity": "Total quantity"}
    ).merge(df_detail, "left", on=["Name"])

    df_shopify = df_shopify.rename(
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

    df_shopify["Brand"] = brand
    df_shopify["Status Pesanan"] = df_shopify["Status Pesanan"].str.upper()
    df_shopify["Sub Status"] = df_shopify["Sub Status"].str.upper()
    df_shopify["No Order"] = df_shopify["Payment Reference"] + df_shopify["Name"]
    df_shopify = df_shopify.drop(columns={"Payment Reference", "Name"})
    df_shopify["Date"] = pd.to_datetime(pd.to_datetime(df_shopify["Date"]).dt.date)
    df_shopify["Ship Date"] = pd.to_datetime(
        pd.to_datetime(df_shopify["Ship Date"]).dt.date
    )
    df_shopify["Ship Date"] = df_shopify["Ship Date"].fillna(
        pd.to_datetime("1900-01-01")
    )
    df_shopify["Marketplace"] = "SHOPIFY"

    df_shopify = join_codebars(df_shopify, df_invent)
    df_shopify["Basic Price"] = df_shopify["BASICPRICE"]

    # Rumus
    df_shopify["Existing Net Sales"] = df_shopify["Net Sales"] * df_shopify["Qty Sold"]

    df_shopify["Existing Basic Price"] = (
        df_shopify["Basic Price"] * df_shopify["Qty Sold"]
    )

    df_shopify["Existing Voucher"] = df_shopify["Voucher"] * df_shopify["Qty Sold"]

    df_shopify["Discount"] = df_shopify["Basic Price"] - df_shopify["Net Sales"]
    df_shopify["Existing Discount"] = df_shopify["Discount"] * df_shopify["Qty Sold"]
    df_shopify["Existing HPP"] = df_shopify["HPP"] * df_shopify["Qty Sold"]
    df_shopify["Value After Voucher"] = (
        df_shopify["Existing Net Sales"] - df_shopify["Existing Voucher"]
    )

    df_shopify = df_shopify.drop(columns=["Subtotal", "Total", "Total quantity"])

    return df_shopify


# For E-Commerce Tokopedia
def preprocessing_raw_tokopedia(df_tokopedia, df_invent, brand):
    df_tokopedia1 = df_tokopedia[
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
            "Jumlah Subsidi Tokopedia (IDR)",
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
            "Jumlah Subsidi Tokopedia (IDR)": "Subsidi",
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

    df_tokopedia1 = df_tokopedia1.dropna(subset="No Order")
    df_tokopedia1["Brand"] = brand
    df_tokopedia1["Barcode"] = df_tokopedia1["Barcode"].astype(str)
    df_tokopedia1["Date"] = pd.to_datetime(
        df_tokopedia1["Date"], format="%d-%m-%Y %H:%M:%S"
    ).dt.strftime("%Y-%m-%d")
    df_tokopedia1["Ship Date"] = pd.to_datetime(
        df_tokopedia1["Ship Date"], format="%d-%m-%Y"
    ).dt.strftime("%Y-%m-%d")
    df_tokopedia1["Delivered Date"] = pd.to_datetime(
        df_tokopedia1["Delivered Date"], format="%d-%m-%Y"
    ).dt.strftime("%Y-%m-%d")
    df_tokopedia1["Marketplace"] = "TOKOPEDIA"

    df_tokopedia1 = join_codebars(df_tokopedia1, df_invent)

    df_tokopedia1["Status Pesanan"] = df_tokopedia1["Status Pesanan"].astype(str)
    df_tokopedia1["Alasan Pembatalan"] = df_tokopedia1["Status Pesanan"].apply(
        lambda x: x.split("-\n")[1] if len(x.split(" -\n")) > 1 else ""
    )
    df_tokopedia1["Status Pesanan"] = df_tokopedia1["Status Pesanan"].apply(
        lambda x: x.split(" -\n")[0]
    )

    # Status yang tidak dibawa
    df_tokopedia1["Status Pesanan"] = df_tokopedia1["Status Pesanan"].str.upper()

    # Ambil Nilai Voucher
    df_tokopedia1["Voucher"] = (
        df_tokopedia1["Total Voucher"] / df_tokopedia1["Qty Sold"]
    )

    # Rumus
    df_tokopedia1["Discount"] = (
        df_tokopedia1["Basic Price"] - df_tokopedia1["Sale Price"]
    )

    df_tokopedia1["Net Sales"] = df_tokopedia1["Sale Price"]

    df_tokopedia1["Existing Basic Price"] = (
        df_tokopedia1["Basic Price"] * df_tokopedia1["Qty Sold"]
    )

    df_tokopedia1["Existing Discount"] = (
        df_tokopedia1["Discount"] * df_tokopedia1["Qty Sold"]
    )

    df_tokopedia1["Existing Voucher"] = (
        df_tokopedia1["Voucher"] * df_tokopedia1["Qty Sold"]
    )

    df_tokopedia1["Existing Net Sales"] = (
        df_tokopedia1["Existing Basic Price"] - df_tokopedia1["Existing Discount"]
    )

    df_tokopedia1["Value After Voucher"] = (
        df_tokopedia1["Existing Net Sales"] 
        - df_tokopedia1["Existing Voucher"]
        + df_tokopedia1['Subsidi']
    )

    df_tokopedia1["Existing HPP"] = df_tokopedia1["HPP"] * df_tokopedia1["Qty Sold"]

    df_tokopedia1 = df_tokopedia1.drop(columns=["Sale Price"])

    return df_tokopedia1


# For E-commerce Zalora
def preprocessing_raw_zalora(df_zalora, df_invent, brand):
    df_zalora1 = df_zalora[
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

    df_zalora1["Brand"] = brand
    df_zalora1["Date"] = pd.to_datetime(df_zalora1["Date"]).dt.date
    df_zalora1["Delivered Date"] = pd.to_datetime(df_zalora1["Delivered Date"]).dt.date
    df_zalora1["Barcode"] = df_zalora1["Barcode"].astype(str)
    df_zalora1["Marketplace"] = "ZALORA"
    df_zalora1["Qty Sold"] = 1
    df_zalora1["Voucher"] = (
        df_zalora1["Net Sales"]
        - df_zalora1["Paid Price"]
        - df_zalora1["Store Credits"]
        - df_zalora1["Wallet Credits"]
    )

    df_zalora1["Existing Voucher"] = df_zalora1["Voucher"] * df_zalora1["Qty Sold"]

    df_zalora1 = join_codebars(df_zalora1, df_invent)
    df_zalora1["Basic Price"] = df_zalora1["BASICPRICE"]

    # Status yang tidak dibawa

    # status_zalora = ["canceled", "delivery failed", "returned"]

    # df_zalora1 = df_zalora1[~(df_zalora1["Status Pesanan"].isin(status_zalora))]
    df_zalora1["Status Pesanan"] = df_zalora1["Status Pesanan"].str.upper()

    # Rumus
    df_zalora1["Discount"] = df_zalora1["Basic Price"] - df_zalora1["Net Sales"]
    df_zalora1["Existing Basic Price"] = (
        df_zalora1["Basic Price"] * df_zalora1["Qty Sold"]
    )
    df_zalora1["Existing Discount"] = df_zalora1["Discount"] * df_zalora1["Qty Sold"]

    df_zalora1["Existing Net Sales"] = (
        df_zalora1["Existing Basic Price"] - df_zalora1["Existing Discount"]
    )
    df_zalora1["Value After Voucher"] = (
        df_zalora1["Existing Net Sales"] - df_zalora1["Existing Voucher"]
    )
    df_zalora1["Existing HPP"] = df_zalora1["HPP"] * df_zalora1["Qty Sold"]

    df_zalora1 = df_zalora1.drop(
        columns=["Paid Price", "Store Credits", "Wallet Credits"]
    )
    return df_zalora1


# For E-commerce Bli bli
def preprocessing_raw_blibli(df_blibli, df_invent, brand):
    df_blibli1 = df_blibli[
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

    df_blibli1["Brand"] = brand
    df_blibli1["Date"] = pd.to_datetime(
        pd.to_datetime(df_blibli["Tanggal Order"], format="%d/%m/%Y %H:%M").dt.strftime(
            "%Y-%m-%d"
        )
    )

    process_columns = ["No Order", "Barcode"]
    for c in process_columns:
        df_blibli1[c] = df_blibli1[c].astype(str)
        if df_blibli1[c].str.contains('"').any():
            df_blibli1[c] = df_blibli1[c].str.split('"').str[1]
        else:
            pass

    df_blibli1["Marketplace"] = "BLIBLI"

    df_blibli1 = join_codebars(df_blibli1, df_invent)

    # Status
    # status_blibli = ["Order Batal"]
    # df_blibli1 = df_blibli1[~(df_blibli1["Status Pesanan"].isin(status_blibli))]
    df_blibli1["Status Pesanan"] = df_blibli1["Status Pesanan"].str.upper()

    # Rumus
    # df_blibli1["Discount"] = df_blibli1["Basic Price"] - df_blibli1["Net Sales"]
    # df_blibli1["Existing Basic Price"] = (
    #     df_blibli1["Basic Price"] * df_blibli1["Qty Sold"]
    # )
    df_blibli1["Net Sales"] = df_blibli1["Basic Price"] - df_blibli1["Discount"]
    df_blibli1["Existing Discount"] = df_blibli1["Discount"] * df_blibli1["Qty Sold"]
    df_blibli1["Existing Net Sales"] = (
        df_blibli1["Existing Basic Price"] - df_blibli1["Existing Discount"]
    )
    df_blibli1['Voucher'] = df_blibli1['Voucher'].abs()
    df_blibli1["Existing Voucher"] = df_blibli1["Voucher"] * df_blibli1["Qty Sold"]
    df_blibli1["Value After Voucher"] = (
        df_blibli1["Existing Net Sales"] - df_blibli1["Existing Voucher"]
    )
    df_blibli1["Existing HPP"] = df_blibli1["HPP"] * df_blibli1["Qty Sold"]

    return df_blibli1
