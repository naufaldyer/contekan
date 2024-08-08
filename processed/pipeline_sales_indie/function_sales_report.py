import pandas as pd
import numpy as np
import calendar
import datetime
from production_pipeline.common.getSource import getMasterStore


def compare_to_staging(df_detail, year, month):

    df_staging = pd.concat(
        [
            pd.read_parquet(
                f"s3://mega-dev-lake/RawData/D365/Sales/SalesOrderStaging/{company}/year={year}/month={month}/"
            ).assign(dataareaid=company)
            for company in ["mgl", "mpr"]
        ]
    )

    df_staging = df_staging[df_staging["PullStatus"] == "Current"]

    df_staging = df_staging[
        [
            "dataareaid",
            "TransNumber",
            "StagingNumber",
            "Barcode",
            "Quantity",
            "SubTotalDetail",
        ]
    ].rename(
        columns={
            "TransNumber": "ordernumber",
            "StagingNumber": "staging_number_365",
            "Quantity": "quantity_staging_365",
            "SubTotalDetail": "subtotal_staging_365",
            "Barcode": "barcode",
        }
    )

    df = df_detail.merge(
        df_staging, "left", on=["ordernumber", "barcode", "dataareaid"]
    )
    df["is_sync"] = np.where(
        df["quantity"] == df["quantity_staging_365"], "sync", "not sync"
    )

    df = df[
        [
            "dataareaid",
            "cfcode",
            "cscode",
            "axcode",
            "whcode",
            "channel",
            "main_channel",
            "groupstore",
            "brand",
            "stdname",
            "city",
            "so dept head",
            "area head",
            "city head",
            "order_create_date",
            "id",
            "ordernumber",
            "status_order",
            "productdata_id",
            "barcode",
            "itemid",
            "quantity",
            "unit_price",
            "subtotal_unit_price",
            "subtotal_discount_product",
            "compliment_value",
            "undifine_discount_keyaccount",
            "transaction_price",
            "invoice_discount",
            "point",
            "voucher",
            "return_value",
            "cust_paid",
            "total_value_payment",
            "hpp",
            "cost_of_margin",
            "subtotal_hpp",
            "staging_number_365",
            "quantity_staging_365",
            "subtotal_staging_365",
            "is_sync",
            "promoid",
            "promocode",
            "remarks",
            "codebars",
            "alucode",
            "articlecode",
            "umbrellabrand",
            "world",
            "category",
            "subcategory",
            "style",
            "size",
            "season",
            "yearmonth",
            "basicprice",
            "saleprice",
            "datasource",
            "last_ax_gr",
            "age",
            "gr_vs_sales",
            "dpp",
            "updated",
        ]
    ]

    return df


def get_sales_report(year, month, df):

    if "gr_vs_sales" not in df.columns:
        df["gr_vs_sales"] = np.nan
    else:
        pass

    df[
        [
            "payment_contribution",
            "invoice_discount_peritem",
            "cust_paid_peritem",
            "voucher_peritem",
            "compliment_value",
            "return_value",
        ]
    ] = df[
        [
            "payment_contribution",
            "invoice_discount_peritem",
            "cust_paid_peritem",
            "voucher_peritem",
            "compliment_value",
            "return_value",
        ]
    ].fillna(
        0
    )

    df["undifine_discount_keyaccount"] = np.where(
        df["channel"] == "KEY ACCOUNT",
        df["subtotal_unit_price"]
        - df["subtotal_discount_product"]
        - df["compliment_value"]
        - df["transaction_price"],
        0,
    )

    df["cost_of_margin"] = df["cost_of_margin"].fillna(0)
    df["subtotal_hpp"] = (df["hpp"] + df["cost_of_margin"]) * df["quantity"]

    df["total_value_payment"] = np.where(
        df["channel"] == "KEY ACCOUNT",
        df["cust_paid_peritem"],
        df["invoice_discount_peritem"]
        + df["point_peritem"]
        + df["voucher_peritem"]
        + df["return_value"]
        + df["cust_paid_peritem"],
    )
    df["total_value_payment"] = np.where(
        df["quantity"] < 0,
        df["transaction_price"],
        df["total_value_payment"],
    )

    df["umbrellabrand"] = np.where(
        df["umbrellabrand"].isin(["Stocklot", "Stock Lot", "StockLot", "Stock lot"]),
        df["subbrand"],
        df["umbrellabrand"],
    )

    df = df[
        [
            "dataareaid",
            "cfcode",
            "cscode",
            "axcode",
            "whcode",
            "channel",
            "main_channel",
            "groupstore",
            "brand",
            "stdname",
            "city",
            "so dept head",
            "area head",
            "city head",
            "order_create_date",
            "id",
            "ordernumber",
            "status_order",
            "productdata_id",
            "productdata_barcode",
            "itemid",
            "quantity",
            "unit_price",
            "subtotal_unit_price",
            "subtotal_discount_product",
            "compliment_value",
            "undifine_discount_keyaccount",
            "transaction_price",
            "invoice_discount_peritem",
            "point_peritem",
            "voucher_peritem",
            "return_value",
            "cust_paid_peritem",
            "total_value_payment",
            "hpp",
            "cost_of_margin",
            "subtotal_hpp",
            "promoid",
            "promocode",
            "remarks",
            "codebars",
            "alucode",
            "articlecode",
            "umbrellabrand",
            "world",
            "category",
            "subcategory",
            "style",
            "size",
            "season",
            "yearmonth",
            "basicprice",
            "saleprice",
            "datasource",
            "last_ax_gr",
            "age",
            "gr_vs_sales",
        ]
    ].rename(
        columns={
            "productdata_barcode": "barcode",
            "invoice_discount_peritem": "invoice_discount",
            "point_peritem": "point",
            "voucher_peritem": "voucher",
            "cust_paid_peritem": "cust_paid",
        }
    )

    df["status_order"] = df["status_order"].map({6: "Finish", 5: "Received"})
    df = df[~(df["channel"].isna()) & (df["channel"] != "ONLINE")]

    df["dpp"] = np.where(
        df["city"].str.contains("BATAM"),
        df["cust_paid"],
        df["cust_paid"] / 1.11,
    )
    df["barcode"] = df["barcode"].astype("str")
    df["updated"] = pd.to_datetime("now") + pd.DateOffset(hours=7)

    df = compare_to_staging(df_detail=df, year=year, month=month)

    for company in df["dataareaid"].unique():
        if company == "mpr":
            for channel in ["SHOWROOM", "KEY ACCOUNT"]:
                print(f"export {channel}")
                df.query(
                    f"dataareaid == '{company}' & main_channel == '{channel}'"
                ).to_excel(
                    f"s3://report-deliverables/sales-report/sales_detail_new/{year}/{month}/{channel.title().replace(' ', '')}/data.xlsx",
                    index=False,
                )
                print(f"done {channel}")
        else:
            print(f"export {company}")
            df.query(f"dataareaid == '{company}' & main_channel != 'ONLINE'").to_excel(
                f"s3://report-deliverables/sales-report/sales_detail_new/{year}/{month}/Mitrel/data.xlsx",
                index=False,
            )
            print(f"done {company}")

    report_name = "sales_detail"
    for sodepthead in df["so dept head"].unique():
        path = f"s3://report-deliverables/report_broadcast/{report_name}/{year}/{month}/{sodepthead}/data.xlsx"
        df[df["so dept head"] == sodepthead].to_excel(path, index=False)
