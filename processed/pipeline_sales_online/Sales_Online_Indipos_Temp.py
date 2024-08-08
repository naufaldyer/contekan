import pandas as pd
import numpy as np
import datetime


def online_temp_indipos(year, m):
    last_month = str(int(m) - 1).zfill(2)
    df = (
        pd.concat(
            [
                pd.read_parquet(
                    f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{month}/data.parquet"
                )
                for month in [last_month, m]
            ]
        )
        .query("`Status Sales` != 'ONPROGRESS'")[
            [
                "Brand",
                "Marketplace",
                "Delivered Date",
                "No Order",
                "Status Sales",
                "CODEBARS",
                "Qty Sold",
                "Basic Price",
                "Discount",
                "Voucher",
                "Existing Discount",  # dicount per item
                "Existing Voucher",  # discount per so
                "Net Sales",  # basic price - discount per item
                "Existing Net Sales",  # if "existing" it already multiply by qty
                "Value After Voucher",  # cust paid (dont subsctract delivery fee)
                "Pickup & Courier",
                "Resi/Pin",
                "Delivery Fee",  # delivery fee (make sure if it per item or per so)
                "Recipient Name",
                "Recipient Phone",
                "Recipient Address",
                "Recipient District",
                "Recipient City",
                "Recipient Postcode",
            ]
        ]
        .rename(columns={"CODEBARS": "Barcode"})
    )
    # list_object = [
    #     "Pickup & Courier",
    #     "Resi/Pin",
    #     "Recipient Name",
    #     "Recipient Phone",
    #     "Recipient Address",
    #     "Recipient District",
    #     "Recipient City",
    #     "Recipient Postcode",
    # ]
    # for c in list_object:
    #     df[c] = df[c].fillna("")
    #     df[c] = df[c].replace("nan", "")

    df_agg = df.groupby(
        [
            "Brand",
            "Marketplace",
            "Delivered Date",
            "No Order",
            "Status Sales",
            "Barcode",
            "Basic Price",
            "Pickup & Courier",
            "Resi/Pin",
            "Delivery Fee",
            "Recipient Name",
            "Recipient Phone",
            "Recipient Address",
            "Recipient District",
            "Recipient City",
            "Recipient Postcode",
        ],
        as_index=False,
    ).agg(
        {
            "Qty Sold": "sum",
            "Discount": "sum",
            "Voucher": "sum",
            "Existing Discount": "sum",
            "Existing Voucher": "sum",
            "Net Sales": "sum",
            "Existing Net Sales": "sum",
            "Value After Voucher": "sum",
            # "Delivery Fee": "sum",
        }
    )

    df_agg = df_agg.rename(
        columns={
            "Delivered Date": "Transaction Order Date",
            "No Order": "Bon Number",
            "Status Sales": "Transaction Status",
            "Barcode": "Product SKU Barcode",
            "Qty Sold": "Order Total Qty",
            "Basic Price": "Product Unit Price",
        }
    )

    # Mandatory Columns
    df_agg["No"] = df_agg.index + 1
    df_agg["Product Store Price"] = df_agg["Product Unit Price"]
    df_agg["Product Brand Promo"] = (
        df_agg["Existing Discount"] + df_agg["Existing Voucher"]
    )
    df_agg["Product Store Promo"] = 0
    df_agg["Product Nett Price"] = (
        df_agg.get("Product Store Price", 0)
        - df_agg.get("Discount", 0)
        - df_agg.get("Voucher")
    )
    df_agg["Subtotal Nett Value Order"] = (
        df_agg["Product Nett Price"] * df_agg["Order Total Qty"]
    )
    
    df_agg["Delivery Fee"] = ""
    df_agg["Total Delivery Cost"] = "" #df_agg["Delivery Fee"]

    df_agg["Grand Total Order"] = (
        df_agg["Subtotal Nett Value Order"] #+ df_agg["Delivery Fee"]
    )

    df_agg["Service Charges"] = (
        df_agg["Value After Voucher"] - df_agg["Grand Total Order"]
    )

    total_agg = df_agg.groupby(["Bon Number"]).agg({"Grand Total Order": "sum", "Service Charges": 'sum'})
    df_agg = df_agg.merge(total_agg, how="left", on="Bon Number", suffixes=["_x", None]).drop(columns=["Grand Total Order_x", "Service Charges_x"])

    df_agg["Total Payment"] = df_agg["Grand Total Order"] + df_agg["Service Charges"]

    df_agg["Payment Method"] = "BCA Virtual Account"
    df_agg["Channel Type Name"] = "Offline Store"
    df_agg["Channel Name"] = "ONLINE"
    df_agg["Indie Transaction Status"] = df_agg[
        "Transaction Status"
    ]  # status delivered

    # Non Mandatory Columns
    df_agg["Transaction Order ID"] = ""
    df_agg["Insurance Fee"] = ""
    df_agg["Customer Name"] = ""

    df_agg = df_agg[
        [
            "Brand",
            "No",
            "Transaction Order Date",
            "Transaction Order ID",
            "Bon Number",
            "Channel Type Name",
            "Channel Name",
            "Transaction Status",
            "Indie Transaction Status",
            "Product SKU Barcode",
            "Order Total Qty",
            "Product Unit Price",
            "Product Store Price",
            "Product Brand Promo",
            "Product Store Promo",
            "Product Nett Price",
            "Subtotal Nett Value Order",
            "Pickup & Courier",
            "Resi/Pin",
            "Delivery Fee",
            "Insurance Fee",
            "Total Delivery Cost",
            "Grand Total Order",
            "Service Charges",
            "Total Payment",
            "Payment Method",
            "Customer Name",
            "Recipient Name",
            "Recipient Phone",
            "Recipient Address",
            "Recipient District",
            "Recipient City",
            "Recipient Postcode",
            "Marketplace",
        ]
    ]
    
    for c in [
        "Pickup & Courier",
        "Customer Name",
        "Recipient Name",
        "Recipient Phone",
        "Recipient Address",
        "Recipient District",
        "Recipient City",
        "Recipient Postcode",
    ]:
        df_agg[c] = df_agg[c].map(lambda x: "nan" if x == "" else x)

    # Adjustment for Cancel Order
    for x in [
        "Order Total Qty",
        "Product Nett Price",
        "Subtotal Nett Value Order",
        "Delivery Fee",
        "Total Delivery Cost",
        "Total Payment",
    ]:
        df_agg[x] = np.where(
            df_agg["Transaction Status"] == "CANCELED", df_agg[x] * -1, df_agg[x]
        )
        
    df_agg["Bon Number"] = np.where(
        df_agg["Transaction Status"] == "CANCELED",
        df_agg["Bon Number"] + "-C",
        df_agg["Bon Number"],
    )

    df_agg = df_agg[df_agg["Transaction Order Date"].dt.strftime("%m") == m]

    return df_agg


def posindi_temp_excel(year, month):
    df = online_temp_indipos(year, month)
    date = df["Transaction Order Date"].max().strftime("%Y-%m-%d")

    df.to_parquet(
        f"s3://mega-dev-lake/ProcessedData/Sales/report_deliverables/online_indie_temp/{year}/{month}/data.parquet"
    )

    for b in df["Brand"].unique():
        for e in df["Marketplace"].unique():
            data = (
                df[(df["Brand"] == b) & (df["Marketplace"] == e)]
                .drop(columns=["Marketplace", "Brand"])
                .reset_index(drop=True)
            )

            data_sales = data.query("`Transaction Status` == 'DELIVERED'").reset_index(
                drop=True
            )
            data_sales["No"] = data_sales.index + 1

            data_cancel = data.query("`Transaction Status` == 'CANCELED'").reset_index(
                drop=True
            )
            data_cancel["No"] = data_cancel.index + 1

            # Export For Sales Temp
            data_sales.to_excel(
                f"s3://mp-users-report/e-commerce/indiepos_temp/{b}/{year}/{month}/{date}/{b}_{e}_Postindie_Temp_{date}.xlsx",
                index=False,
            )

            # Export For Cancel Temp
            data_cancel.to_excel(
                f"s3://mp-users-report/e-commerce/indiepos_temp/{b}/{year}/{month}/{date}/Cancel_{b}_{e}_Postindie_Temp_{date}.xlsx",
                index=False,
            )
