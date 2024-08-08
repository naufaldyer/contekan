import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class DataTransform:
    def __init__(self, df_order, df_transaction, df_master_product):
        self.df_order = df_order
        self.df_transaction = df_transaction
        self.df_master_product = df_master_product
        self.df_payment = None  # Inisiasi self.df_payment

    def createPaymentDataFrame(self):
        self.df_payment = (
            self.df_transaction.pivot_table(
                index=[
                    "store_branch_id",
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
                self.df_transaction.groupby(["transaction_id"], as_index=False).agg(
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

        self.df_payment["Voucher"] = self.df_payment.get(
            "Gift Card", 0
        ) + self.df_payment.get("External Gift Card", 0)

        self.df_payment["Return Payment"] = self.df_payment.get("Return Payment", 0)

        self.df_payment["cust_paid"] = (
            self.df_payment.get("payment_total", 0)
            - self.df_payment.get("Voucher", 0)
            - self.df_payment.get("Member Point", 0)
            - self.df_payment.get("Return Payment", 0)
            - self.df_payment.get("payment_change", 0)
        )
        self.df_payment.columns = self.df_payment.columns.str.lower().str.replace(
            " ", "_"
        )
        return self.df_payment

    def transformSalesDetail(self):
        # Select columns for order
        self.df_order = self.df_order.query("pull_status == 'latest'")[
            [
                "dataareaid",
                "store_branch_name",
                "store_branch_id",
                "store_branch_extcode",
                "store_branch_description",
                "time_created",
                "time_modified",
                "order_id",
                "transaction_id",
                "invoice_id",
                "customer_id",
                "employment_id",
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

        # Select columns for transaction
        self.df_transaction = self.df_transaction.query("pull_status == 'latest'")[
            [
                "store_branch_id",
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

        # Create df_payment
        self.df_payment = self.createPaymentDataFrame()

        # Drop duplicated from master product by barcode
        self.df_master_product = self.df_master_product[
            self.df_master_product["PullStatus"] == "Current"
        ]
        self.df_master_product = self.df_master_product.drop_duplicates(
            subset="CodeBars", keep="last"
        )

        # Standarize order_columns
        self.df_order["order_product_promotion_id"] = (
            self.df_order["order_product_promotion_id"].astype(int).astype(str)
        )
        self.df_order["order_create_date"] = pd.to_datetime(
            self.df_order["time_created"]
        ).dt.strftime("%Y-%m-%d")
        self.df_order["order_create_hour"] = pd.to_datetime(
            self.df_order["time_created"]
        ).dt.strftime("%H")
        self.df_order["order_create_minute"] = pd.to_datetime(
            self.df_order["time_created"]
        ).dt.strftime("%M")
        self.df_order["compliment_value"] = (
            self.df_order["compliment_value"].replace("None", "0.0").astype(float)
        )
        self.df_order["promotion_id"] = (
            self.df_order["promotion_id"].astype(int).astype(str)
        )

        # Join with aggragate order for sum transfaction price
        df_sales_detail = self.df_order.merge(
            self.df_order.query("order_product_quantity > 0")
            .groupby(["order_id"], as_index=False)
            .agg({"order_product_total": "sum"})
            .rename(columns={"order_product_total": "order_transaction_price"}),
            "left",
            on="order_id",
        )

        # Join with df_payment
        df_sales_detail = df_sales_detail.merge(
            self.df_payment.drop(columns=["time_created"]),
            "left",
            on=["transaction_id", "order_id", "store_branch_id"],
            indicator=("istransaction"),
        ).rename(
            columns={
                "voucher": "order_voucher_payment",
                "return_payment": "order_return_payment",
                "cust_paid": "order_cust_payment",
            }
        )

        # Join with master product
        df_sales_detail = df_sales_detail.merge(
            self.df_master_product[
                [
                    "VariantNumber",
                    "ItemId",
                    "CodeBars",
                    "HPP",
                    "UmbrellaBrand",
                    "SubBrand",
                    "ItemModelGroup",
                ]
            ].rename(
                columns={
                    "VariantNumber": "product_variant_number",
                    "ItemId": "product_item_id",
                    "CodeBars": "product_barcode",
                    "HPP": "product_cost",
                    "UmbrellaBrand": "product_brand",
                    "SubBrand": "product_sub_brand",
                    "ItemModelGroup": "product_item_model_group",
                }
            ),
            "left",
            on="product_barcode",
        )[
            [
                "dataareaid",
                "store_branch_name",
                "store_branch_id",
                "store_branch_extcode",
                "time_created",
                "time_modified",
                "order_create_date",
                "order_create_hour",
                "order_create_minute",
                "order_id",
                "transaction_id",
                "invoice_id",
                "employment_id",
                "customer_id",
                "order_number",
                "order_issettled",
                "order_status",
                "order_total_product",
                "order_total_quantity",
                "order_subtotal",
                "order_discount",
                "order_total",
                "order_transaction_price",
                "order_voucher_payment",
                "order_return_payment",
                "order_cust_payment",
                "promotion_id",
                "promotion_title",
                "promotion_description",
                "product_id",
                "product_item_id",
                "product_variant_number",
                "product_barcode",
                "product_brand",
                "product_sub_brand",
                "product_item_model_group",
                "product_price",
                "order_product_quantity",
                "order_product_subtotal",
                "order_product_promotion_value",
                "order_product_discount",
                "order_product_total",
                "product_cost",
            ]
        ]

        df_sales_detail = df_sales_detail.rename(
            columns={
                "dataareaid": "data_area_id",
                "store_branch_extcode": "store_code",
                "order_product_quantity": "product_quantity",
                "order_product_subtotal": "product_subtotal",
                "order_product_promotion_value": "product_promotion_value",
                "order_product_discount": "product_discount",
                "order_product_cost": "product_cost",
                "order_product_total": "product_transaction_price",
            }
        )

        conditions = [
            (df_sales_detail["data_area_id"] == "mpr")
            & (df_sales_detail["product_item_model_group"] == "CSG")
        ]

        choose = [0.30]

        df_sales_detail["product_margin"] = np.select(
            condlist=conditions, choicelist=choose, default=0
        )

        df_sales_detail["product_cost_of_margin"] = (
            df_sales_detail["product_price"] * df_sales_detail["product_margin"]
        )

        df_sales_detail["product_total_cost"] = np.where(
            (df_sales_detail["product_cost"] != 0)
            & (df_sales_detail["product_cost_of_margin"] != 0),
            df_sales_detail["product_cost_of_margin"]
            * df_sales_detail["product_quantity"],
            (
                df_sales_detail["product_cost"]
                + df_sales_detail["product_cost_of_margin"]
            )
            * df_sales_detail["product_quantity"],
        )

        df_sales_detail["product_payment_contribution"] = np.where(
            df_sales_detail["product_quantity"] > 0,
            df_sales_detail["product_transaction_price"]
            / df_sales_detail["order_transaction_price"],
            0,
        )

        df_sales_detail["product_payment_contribution"] = np.where(
            df_sales_detail["product_payment_contribution"] < 0,
            0,
            df_sales_detail["product_payment_contribution"],
        )

        df_sales_detail["product_voucher_payment"] = (
            df_sales_detail["product_payment_contribution"]
            * df_sales_detail["order_voucher_payment"]
        )
        df_sales_detail["product_cust_payment"] = (
            df_sales_detail["product_payment_contribution"]
            * df_sales_detail["order_cust_payment"]
        )

        df_sales_detail["product_return_value"] = (
            df_sales_detail["order_return_payment"]
            * df_sales_detail["product_payment_contribution"]
        )

        df_sales_detail["product_invoice_discount"] = np.where(
            df_sales_detail["product_cust_payment"] != 0,
            df_sales_detail["product_transaction_price"]
            - (
                df_sales_detail["product_cust_payment"]
                + df_sales_detail["product_voucher_payment"]
                + df_sales_detail["product_return_value"]
            ),
            0,
        )

        df_sales_detail["product_invoice_discount"] = np.where(
            df_sales_detail["product_invoice_discount"] < 100,
            0,
            df_sales_detail["product_invoice_discount"],
        )

        return df_sales_detail
