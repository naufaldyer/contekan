import pandas as pd
import numpy as np
import datetime
import warnings
import boto3
import time
import calendar
from production_pipeline.common.getSource import getMasterStore, getInventable, getSalesDetail
# from function_mapping_master_item import *

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


def source(year, month, company, channel):
    """Get order and transaction data with this function.
    This function will looping order and transaction for each day
    and insert into dictionary who have two keys : order and transactions

    Args:
        year (str): running year
        month (str): running month
        company (str): choose mgl or mpr
        channel (str): choose online, keyaccount or showroom

    Returns:
        dataframe: will return 2 dataframe order and  transaction
    """
    s3 = boto3.resource("s3", region_name="ap-southeast-3")
    bucket = s3.Bucket("mega-dev-lake")
    prefix_objs = bucket.objects.filter(Prefix="RawData/indie_on_develop/")
    uri = "s3://mega-dev-lake/"

    df = {}
    for source in ["orders", "transactions"]:
        data = pd.DataFrame()

        for obj in prefix_objs:
            if obj.key.endswith(".parquet"):
                if (
                    obj.key.split("/")[3] == company
                    and obj.key.split("/")[4] == channel
                    and obj.key.split("/")[2] == source
                    and obj.key.split("/")[5].split("=")[1] == year
                    and obj.key.split("/")[6].split("=")[1] == month
                ):
                    temp = pd.read_parquet(f"{uri + obj.key}")
                    data = pd.concat([data, temp])

        data["time_created"] = pd.to_datetime(data["time_created"]).dt.strftime(
            "%Y-%m-%d"
        )

        if source == "orders":
            data.loc[data["order_number"] == "OOFL030424119700005", "order_status"] = 7
            data.loc[data["order_number"] == "OOFL270424116900013", "order_status"] = 7
            data.loc[data["order_number"] == "OOFL200524114600006", "order_status"] = 7
            data.loc[data["order_number"] == "OOFL1506241570700034", "order_status"] = 7
            data.loc[data["order_number"] == "OOFL250624119600006", "order_status"] = 7
            data.loc[
                data["order_number"] == "OOFL120725072024111314", "order_issettled"
            ] = True
        else:
            pass

        # data["pull_date"] = pd.to_datetime(data["pull_date"]).dt.strftime("%Y-%m-%d")

        # data["pull_status"] = np.where(
        #     (data["time_created"] == "2024-03-11")
        #     & (data["pull_date"] == "2024-04-05"),
        #     "latest",
        #     data["pull_status"],
        # )

        df[source] = data

    return df["orders"], df["transactions"]


def select_order_columns(df_order):
    return df_order.query("pull_status == 'latest'")[
        [
            "store_branch_name",
            "store_branch_id",
            "store_branch_extcode",
            "store_branch_description",
            "time_created",
            "time_modified",
            "order_id",
            "transaction_id",
            "invoice_id",
            "order_number",
            "order_total_product",
            "order_total_quantity",
            "order_subtotal",
            "order_discount",
            "order_total",
            "order_isrecord",
            "order_issettled",
            "order_status",
            "order_product_id",
            "order_product_quantity",
            "order_product_subtotal",
            "order_product_discount",
            "order_product_total",
            "order_product_promotion_value",
            "order_product_promotion_id",
            "promotion_id",
            "promotion_title",
            "promotion_description",
            "compliment_value",
            "product_id",
            "product_barcode",
            "product_price",
        ]
    ]


def select_transaction_columns(df_trans):
    return df_trans.query("pull_status == 'latest'")[
        [
            "store_branch_extcode",
            "time_created",
            "transaction_id",
            "order_id",
            "transaction_number",
            "payment_id",
            "payment_total",
            "payment_change",
            "payment_fee",
            "payment_amount",
            "payment_method_id",
            "payment_method_name",
        ]
    ]


def select_master_store_columns(masterstore):
    return masterstore[
        [
            "dataareaid",
            "cfcode",
            "cscode",
            "whcode",
            "axcode",
            "stdname",
            "brand",
            "channel",
            "groupstore",
            "deptstore",
            "province",
            "region",
            "city",
            "so dept head",
            "area head",
            "city head",
        ]
    ]


def select_inventtable_columns(itable):
    itable.columns = itable.columns.str.lower()
    itable = itable.drop(columns=["inventdimid", "recid", "dataareaid"])
    return itable


def select_master_item(company):
    df = pd.read_parquet(
        f"s3://mega-dev-lake/ProcessedData/D365/Product/{company}/data.parquet"
    ).assign(dataareaid=company)
    df = df[df["PullStatus"] == "Current"]
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates(subset=["codebars"], keep="last")
    df = df[
        [
            "dataareaid",
            "itemid",
            "codebars",
            "alucode",
            "articlecode",
            "umbrellabrand",
            "subbrand",
            "department",
            "category",
            "subcategory",
            "world",
            "season",
            "hpp",
            "size",
            "style",
            "monthyear",
            "basicprice",
            "saleprice",
        ]
    ].rename(columns={"monthyear": "yearmonth"})
    return df


def create_payment_dataframe(df_trans):
    # Get Payment df
    df_payment = (
        df_trans.pivot_table(
            index=[
                "store_branch_extcode",
                "time_created",
                "transaction_id",
                "order_id",
            ],
            columns="payment_method_name",
            values="payment_total",
            aggfunc="sum",
        )
        .reset_index()
        .fillna(0)
        .merge(
            df_trans.groupby(["transaction_id"], as_index=False).agg(
                {
                    "payment_change": "sum",
                    "payment_fee": "sum",
                    "payment_amount": "mean",
                    "payment_total": "sum",
                }
            ),
            "left",
            on="transaction_id",
        )
    )

    df_payment["Voucher"] = df_payment.get("Gift Card", 0) + df_payment.get(
        "External Gift Card", 0
    )

    df_payment["Return Payment"] = df_payment.get("Return Payment", 0)

    df_payment["cust_paid"] = (
        df_payment.get("payment_total", 0)
        - df_payment.get("Voucher", 0)
        - df_payment.get("Member Point", 0)
        - df_payment.get("Return Payment", 0)
        - df_payment.get("payment_change", 0)
    )
    return df_payment


def calculate_payment_contribution(df_order):
    df_order["payment_contribution"] = np.where(
        df_order["order_product_quantity"] > 0,
        df_order["order_product_total"] / df_order["total_transaction_price"],
        0,
    )
    df_order["payment_contribution"] = df_order["payment_contribution"].apply(
        lambda x: 0 if x < 0 else x
    )
    return df_order


def calculate_detail_transaction(df_order, company):
    df_order["cust_paid_peritem"] = (
        df_order["cust_paid"] * df_order["payment_contribution"]
    )
    df_order["voucher_peritem"] = df_order["Voucher"] * df_order["payment_contribution"]
    df_order["point_peritem"] = (
        df_order.get("Member Point", 0) * df_order["payment_contribution"]
    )
    df_order["return_value"] = (
        df_order["Return Payment"] * df_order["payment_contribution"]
    )
    df_order["invoice_discount_peritem"] = np.where(
        df_order["cust_paid_peritem"] != 0,
        df_order["order_product_total"]
        - (
            df_order["cust_paid_peritem"]
            + df_order["voucher_peritem"]
            + df_order["return_value"]
        ),
        0,
    )
    df_order["invoice_discount_peritem"] = df_order["invoice_discount_peritem"].apply(
        lambda x: 0 if x < 100 else x
    )
    df_order["discount"] = (
        df_order["order_product_discount"] / df_order["order_product_quantity"]
    )
    df_order["unit_price"] = (
        df_order["order_product_subtotal"] / df_order["order_product_quantity"]
    )

    if company == "mgl":
        # MARGIN ITEM PER UMBRELLABRAND
        margin = pd.read_csv("s3://mega-lake/ProcessedData/master/margin_mgl.csv")
        df_order = df_order.merge(
            margin[["brand_name", "cost_of_margin"]].rename(
                columns={"brand_name": "umbrellabrand", "cost_of_margin": "margin"}
            ),
            "left",
            on=["umbrellabrand"],
        )
        df_order["margin"] = df_order["margin"].fillna(0)

        df_order["cost_of_margin"] = df_order.apply(
            lambda x: (
                x["order_product_total"] * x["margin"] if x["margin"] != 0 else 0
            ),
            axis=1,
        )

    else:
        df_order["margin"] = df_order.apply(
            lambda x: (
                0.3 if isinstance(x["category"], str) and "CSG" in x["category"] else 0
            ),
            axis=1,
        )

        df_order["cost_of_margin"] = df_order.apply(
            lambda x: x["cust_paid_peritem"] * x["margin"] if x["margin"] != 0 else 0,
            axis=1,
        )

    # df_order["cost_of_margin"] = df_order.apply(
    #     lambda x: x["transaction_price"] * x["margin"] if x["margin"] != 0 else 0,
    #     axis=1,
    # )

    df_order["hpp_total"] = (
        df_order["hpp"] * df_order["order_product_quantity"]
    ) + df_order["cost_of_margin"]
    return df_order


def select_columns_detail(df_order):
    list_columns = [
        "dataareaid",
        "axcode",
        "cfcode",
        "cscode",
        "whcode",
        "stdname",
        "brand",
        "main_channel",
        "channel",
        "groupstore",
        "deptstore",
        "province",
        "region",
        "city",
        "so dept head",
        "area head",
        "city head",
        "store_branch_id",
        "store_branch_description",
        "time_created",
        "timecreated_hour",
        "timecreated_minute",
        "order_create_date",
        "time_modified",
        "transaction_id",
        "order_id",
        "order_number",
        "order_status",
        "order_issettled",
        "istransaction",
        "order_total_quantity",
        "order_total",
        "order_discount",
        "order_subtotal",
        "total_transaction_price",
        "cust_paid",
        "product_id",
        "product_barcode",
        "order_product_quantity",
        "product_price",
        "discount",
        "compliment_value",
        "order_product_promotion_value",
        "order_product_subtotal",
        "order_product_discount",
        "order_product_total",
        "hpp",
        "hpp_total",
        "margin",
        "cost_of_margin",
        "payment_contribution",
        "point_peritem",
        "voucher_peritem",
        "return_value",
        "invoice_discount_peritem",
        "cust_paid_peritem",
        "itemid",
        "codebars",
        "alucode",
        "articlecode",
        "umbrellabrand",
        "subbrand",
        "department",
        "category",
        "subcategory",
        "world",
        "style",
        "size",
        "basicprice",
        "saleprice",
        "season",
        "yearmonth",
        "payment_amount",
        "promotion_id",
        "promocode",
        "promotion_description",
        "age",
        "last_ax_gr",
        "gr_vs_sales",
        "datasource",
    ]

    rename_columns = {
        "store_branch_id": "storebranchdata_id",
        "store_branch_description": "description",
        "order_id": "id",
        "transaction_id": "id_transaction",
        "order_number": "ordernumber",
        "order_total_quantity": "totalquantity",
        "order_discount": "total_discount",
        "order_subtotal": "subtotal",
        "order_total": "total",
        "product_id": "productdata_id",
        "product_barcode": "productdata_barcode",
        "order_product_quantity": "quantity",
        "product_price": "unit_price",
        "order_product_subtotal": "subtotal_unit_price",
        "order_product_discount": "subtotal_discount_product",
        "order_product_promotion_value": "promotion_value",
        "order_product_total": "transaction_price",
        "promotion_id": "promoid",
        "promotion_description": "remarks",
        "order_status": "status_order",
        "time_modified": "timemodified",
        "order_issettled": "issettled",
    }
    return df_order[list_columns].rename(columns=rename_columns)


def main_processing_order_detail(
    df_order, df_trans, promotion, master, itable, company, gr_ax, channel
):
    # Create df_payment
    df_payment = create_payment_dataframe(df_trans)

    # Get Order Detail
    df_order["order_product_promotion_id"] = (
        df_order["order_product_promotion_id"].astype(int).astype(str)
    )
    df_order["order_create_date"] = pd.to_datetime(
        df_order["time_created"]
    ).dt.strftime("%Y-%m-%d")
    df_order["timecreated_hour"] = pd.to_datetime(df_order["time_created"]).dt.strftime(
        "%H"
    )
    df_order["timecreated_minute"] = pd.to_datetime(
        df_order["time_created"]
    ).dt.strftime("%M")
    df_order["compliment_value"] = (
        df_order["compliment_value"].replace("None", "0.0").astype(float)
    )
    df_order["promotion_id"] = df_order["promotion_id"].astype(int).astype(str)
    promotion["id"] = promotion["id"].astype(str)
    df_order = df_order.rename(columns={"store_branch_extcode": "axcode"})

    # check total transaction without payment min quantity
    df_order = df_order.merge(
        df_order.query("order_product_quantity > 0")
        .groupby(["order_id"], as_index=False)
        .agg({"order_product_total": "sum"})
        .rename(columns={"order_product_total": "total_transaction_price"}),
        "left",
        on="order_id",
    )

    # Join order with masterstore, prmotion,  payment and inventtable
    df_order = (
        master.merge(
            df_order,
            "right",
            on=["axcode"],
        )
        .merge(
            promotion[["id", "detail.usedKey"]].rename(
                columns={"id": "promotion_id", "detail.usedKey": "promocode"}
            ),
            "left",
            on=["promotion_id"],
        )
        .fillna({"promocode": "regular", "promotion_description": "Regular"})
        .merge(
            df_payment.drop(columns=["time_created"]),
            "left",
            on=["transaction_id", "order_id"],
            indicator=("istransaction"),
        )
        .merge(
            itable,
            "left",
            left_on=["product_barcode"],
            right_on=["codebars"],
        )
        .fillna({"itemid": "undetected", "subcategory": "undetected"})
        .merge(
            gr_ax.query(f"dataareaid == '{company}'").drop(columns=["dataareaid"]),
            "left",
            on=["itemid"],
        )
    )

    df_order["istransaction"] = df_order["istransaction"].map(
        {"both": "1", "right_only": "0", "left_only": "0"}
    )

    df_order["gr_vs_sales"] = abs(
        df_order["last_ax_gr"] - pd.to_datetime(df_order["order_create_date"])
    )

    df_order = df_order.fillna({"age": "0", "last_ax_gr": "", "gr_vs_sales": ""})
    df_order[["last_ax_gr", "gr_vs_sales", "age"]] = df_order[
        ["last_ax_gr", "gr_vs_sales", "age"]
    ].astype(str)

    # df_order["main_channel"] = (
    #     channel.upper() if channel != "keyaccount" else "KEY ACCOUNT"
    # )

    # df_order["main_channel"] = np.where(
    #     df_order["stdname"].str.contains("SHOWROOM"),
    #     "SHOWROOM",
    #     df_order["main_channel"],
    # )

    df_order["main_channel"] = df_order["channel"]

    df_order["order_issettled"] = np.where(
        ~df_order["main_channel"].isin(["SHOWROOM"]),
        True,
        df_order["order_issettled"],
    )

    # ini transaksi manual yang di upload di showroom
    # df_order.iloc[
    #     df_order["ordernumber"] == "OOFL120725072024111314", "order_issettled"
    # ] = True

    print(df_order.columns)

    # calculate
    df_order = calculate_detail_transaction(
        calculate_payment_contribution(df_order), company
    )

    df_order["dataareaid"] = company
    df_order["datasource"] = "indiepos"
    df_order["employee_nik"] = ""
    df_order["employee_position"] = ""
    df_order["employee_name"] = ""

    return select_columns_detail(df_order)


def getGR():
    gr_ax = pd.read_parquet(
        "s3://mega-lake/ProcessedData/DO-Monitoring/last_gr.parquet"
    )
    gr_ax.columns = gr_ax.columns.str.lower()
    gr_ax["age"] = gr_ax["last_ax_gr"] - datetime.datetime.now()
    gr_ax.age = abs(gr_ax.age.dt.days)
    return gr_ax


def logging_sales(df):
    print(
        df.groupby("channel", as_index=False)
        .agg(
            row=("quantity", "size"),
            quantity=("quantity", "sum"),
            cust_paid_peritem=("cust_paid_peritem", "sum"),
        )
        .assign(
            date_process=datetime.datetime.today().strftime("%Y-%m-%d"),
            time_process=datetime.datetime.today().strftime("%H:%M:%S"),
        )
    )


def main_process(year, month):
    month_name = calendar.month_name[int(month)]

    gr_ax = getGR()

    master_store = select_master_store_columns(
        pd.read_parquet(
            f"s3://mega-dev-lake/Staging/Master/Master Store/{year}/data.parquet"
        ).query(f"month == '{month_name}'")
    )

    df = pd.DataFrame()
    print(f"=== Start {year} {month} ===")

    for company in ["mpr", "mgl"]:
        print(f"=== process {company} ===")
        # itable = select_inventtable_columns(getInventable(company))
        # itable = itable.drop_duplicates(subset=["codebars"], keep="last")
        itable = select_master_item(company=company)
        master = master_store[master_store["dataareaid"] == company].drop(
            columns="dataareaid"
        )
        promotion = pd.read_parquet(
            f"s3://mega-dev-lake/RawData/indie_on_develop/promotions/{company}/promotions.parquet"
        )

        for channel in ["showroom", "keyaccount", "online"]:
            try:
                print(f"--- process {channel} ---")
                df_order, df_trans = source(year, month, company, channel)
                temp_detail = main_processing_order_detail(
                    select_order_columns(df_order),  # Select order columns
                    select_transaction_columns(df_trans),  # Select transaction columns
                    promotion,
                    master,
                    itable,
                    company,
                    gr_ax,
                    channel,
                )

                # tambah mapping ke new detail
                # temp_detail = mapping_new_master_item(company, temp_detail)

                df = pd.concat([df, temp_detail])  # Concat data with initial dataframe
            except Exception as e:
                print(e)
                print(f"--- {channel} is no data ---")

        # logging_sales(df)  # Check the information from data

    df.to_parquet(
        f"s3://mega-dev-lake/ProcessedData/Sales/sales_detail_indie/{year}/{month}/data.parquet"
    )
    print(f"=== Finish ===")
