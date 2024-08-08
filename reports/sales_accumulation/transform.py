import pandas as pd
import numpy as np
import warnings
from sqlalchemy import create_engine, text
from extract import *

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class TransformDataSalesAccum:
    def __init__(self, month, year):
        self.month = month
        self.year = year
        self.last_year = str(int(self.year) - 1)

    def transform_pattern(selfm, pattern):
        # Create id by concanate all date in pattern_date, pattern_lm, pattern_last_year
        pattern["pattern_date"] = pattern["pattern_date"].astype(str)
        pattern["pattern_lm"] = pattern["pattern_lm"].astype(str)
        pattern["pattern_last_year"] = pattern["pattern_last_year"].astype(str)

        pattern["id"] = (
            pattern["pattern_date"]
            + "/"
            + pattern["pattern_lm"]
            + "/"
            + pattern["pattern_last_year"]
        )

        pattern = pattern[["id", "pattern_date", "pattern_lm", "pattern_last_year"]]

        # Create date dimension for calculate score perdays depends rules on company's
        dim_date = pattern[["id", "pattern_date"]].assign(
            day_name=lambda x: pd.to_datetime(x["pattern_date"]).dt.day_name()
        )

        conditions = [
            dim_date["day_name"].isin(["Saturday", "Sunday"]),
            dim_date["day_name"] == "Friday",
        ]
        chooses = [2, 1.5]

        dim_date["day_score"] = np.select(
            condlist=conditions, choicelist=chooses, default=1
        )
        dim_date["month_score"] = dim_date["day_score"].sum()

        return pattern, dim_date

    def transform_master_online(self, master_store):
        master_store["channel"] = master_store["channel"].str.upper()
        master_online = master_store[master_store["channel"] == "ONLINE"]
        master_online["marketplace"] = (
            master_online["stdname"].str.split(" ").str.get(2)
        )
        master_online["brand"] = master_online["stdname"].str.split(" ").str.get(1)
        master_online["marketplace"] = master_online["marketplace"].fillna("")

        master_online["marketplace"] = np.where(
            master_online["marketplace"].isin(
                [
                    "MANZONESTORE.ID",
                    "MINIMALSTORE.ID",
                    "MOCSTORE.ID",
                    "WEBSITE",
                ]
            ),
            "SHOPIFY",
            master_online["marketplace"],
        )
        return master_online

    def transform_sales(self, master_store, df_sales_offline, df_sales_online):
        master_online = self.transform_master_online(master_store)

        df_sales_online = (
            master_online[["dataareaid", "axcode", "brand", "marketplace"]]
            .merge(df_sales_online, "right", on=["brand", "marketplace"])
            .drop(columns=["brand", "marketplace"])
        )

        df = pd.concat([df_sales_offline, df_sales_online])

        df["codebars"] = np.where(
            df["codebars"].isin(["undetected", "Undetected"])
            & ~df["productdata_barcode"].isna(),
            df["productdata_barcode"],
            df["codebars"],
        )

        df = df.groupby(
            [
                "dataareaid",
                "axcode",
                "order_create_date",
                "codebars",
            ],
            as_index=False,
        ).agg(
            {
                "ordernumber": "nunique",
                "quantity": "sum",
                "cust_paid_peritem": "sum",
                "hpp_total": "sum",
            }
        )

        df[["cust_paid_peritem", "hpp_total"]] = df[
            ["cust_paid_peritem", "hpp_total"]
        ].round(2)
        return df

    def transform_sales_akum(
        self,
        pattern,
        master_store,
        master_product,
        df_this_month,
        df_last_month,
        df_last_year,
    ):
        df_pattern, dim_date = self.transform_pattern(pattern)
        df_target = master_store

        df_this_month_p = df_this_month.merge(
            df_pattern[["id", "pattern_date"]].rename(
                columns={"pattern_date": "order_create_date"}
            ),
            "left",
            on=["order_create_date"],
        )

        list_pattern = df_this_month_p["id"].unique()

        df_last_month_p = df_pattern[df_pattern["id"].isin(list_pattern)][
            ["id", "pattern_lm"]
        ].merge(
            df_last_month.rename(columns={"order_create_date": "pattern_lm"}),
            "left",
            on="pattern_lm",
        )

        df_last_year_p = df_pattern[df_pattern["id"].isin(list_pattern)][
            ["id", "pattern_last_year"]
        ].merge(
            df_last_year.rename(columns={"order_create_date": "pattern_last_year"}),
            "left",
            on="pattern_last_year",
        )

        df_akum = df_this_month_p.merge(
            df_last_month_p,
            "outer",
            on=["dataareaid", "axcode", "id", "codebars"],
            suffixes=("", "_lm"),
        )

        df_akum = df_akum.merge(
            df_last_year_p,
            "outer",
            on=["dataareaid", "axcode", "id", "codebars"],
            suffixes=("", "_ly"),
        )

        df_akum["order_create_date"] = df_akum["id"].str.split("/").str.get(0)
        df_akum["order_create_date_lm"] = df_akum["id"].str.split("/").str.get(1)
        df_akum["order_create_date_ly"] = df_akum["id"].str.split("/").str.get(2)

        df_akum["day"] = pd.to_datetime(df_akum["order_create_date"]).dt.day
        df_akum["month"] = pd.to_datetime(df_akum["order_create_date"]).dt.strftime(
            "%m"
        )
        df_akum["week"] = (df_akum["day"] - 1) // 7 + 1
        df_akum["week"] = df_akum["week"].astype(str)
        df_akum["week"] = "Week" + " " + df_akum["week"]

        df_akum = df_akum.merge(
            dim_date[["id", "day_name", "day_score", "month_score"]], "left", on="id"
        )

        df_akum = df_akum[
            [
                "dataareaid",
                "axcode",
                "id",
                "order_create_date",
                "day",
                "day_name",
                "day_score",
                "week",
                "month",
                "month_score",
                "codebars",
                "ordernumber",
                "quantity",
                "cust_paid_peritem",
                "hpp_total",
                "order_create_date_lm",
                "ordernumber_lm",
                "quantity_lm",
                "cust_paid_peritem_lm",
                "hpp_total_lm",
                "order_create_date_ly",
                "ordernumber_ly",
                "quantity_ly",
                "cust_paid_peritem_ly",
                "hpp_total_ly",
            ]
        ]

        df_akum[
            [
                "ordernumber",
                "quantity",
                "cust_paid_peritem",
                "hpp_total",
                "ordernumber_lm",
                "quantity_lm",
                "cust_paid_peritem_lm",
                "hpp_total_lm",
                "ordernumber_ly",
                "quantity_ly",
                "cust_paid_peritem_ly",
                "hpp_total_ly",
            ]
        ] = df_akum[
            [
                "ordernumber",
                "quantity",
                "cust_paid_peritem",
                "hpp_total",
                "ordernumber_lm",
                "quantity_lm",
                "cust_paid_peritem_lm",
                "hpp_total_lm",
                "ordernumber_ly",
                "quantity_ly",
                "cust_paid_peritem_ly",
                "hpp_total_ly",
            ]
        ].fillna(
            0
        )

        df_akum = df_akum.merge(master_product, "left", on=["dataareaid", "codebars"])

        df_akum[master_product.columns] = df_akum[master_product.columns].fillna(
            "Undetected"
        )

        df_akum_percat = df_akum.groupby(
            [
                "dataareaid",
                "axcode",
                "order_create_date",
                "day",
                "day_name",
                "day_score",
                "week",
                "month",
                "month_score",
                "itemid",
                "umbrellabrand",
                "subbrand",
                "world",
                "category",
                "subcategory",
            ],
            as_index=False,
        ).agg(
            {
                "ordernumber": "sum",
                "quantity": "sum",
                "cust_paid_peritem": "sum",
                "hpp_total": "sum",
                "ordernumber_lm": "sum",
                "quantity_lm": "sum",
                "cust_paid_peritem_lm": "sum",
                "hpp_total_lm": "sum",
                "ordernumber_ly": "sum",
                "quantity_ly": "sum",
                "cust_paid_peritem_ly": "sum",
                "hpp_total_ly": "sum",
            }
        )

        df_akum_percat = df_akum_percat.merge(
            df_target, "left", on=["dataareaid", "axcode"]
        )

        df_akum_percat["target_daily"] = (
            df_akum_percat["target"] / df_akum_percat["month_score"]
        ) * df_akum_percat["day_score"]

        df_akum_percat.loc[
            df_akum_percat.duplicated(subset=["dataareaid", "axcode", "target"]),
            "target",
        ] = 0

        df_akum_percat.loc[
            df_akum_percat.duplicated(
                subset=["dataareaid", "axcode", "order_create_date", "target_daily"]
            ),
            "target_daily",
        ] = 0

        df_akum_percat = df_akum_percat[
            [
                "dataareaid",
                "brand",
                "channel",
                "deptstore",
                "groupstore",
                "cscode",
                "axcode",
                "whcode",
                "stdname",
                "sqm",
                "sssg",
                "openstatus",
                "so_dept_head",
                "area_head",
                "city_head",
                "target",
                "target_daily",
                "order_create_date",
                "day",
                "day_name",
                "day_score",
                "week",
                "month",
                "month_score",
                "itemid",
                "umbrellabrand",
                "subbrand",
                "world",
                "category",
                "subcategory",
                "ordernumber",
                "quantity",
                "cust_paid_peritem",
                "hpp_total",
                "ordernumber_lm",
                "quantity_lm",
                "cust_paid_peritem_lm",
                "hpp_total_lm",
                "ordernumber_ly",
                "quantity_ly",
                "cust_paid_peritem_ly",
                "hpp_total_ly",
            ]
        ]

        return df_akum_percat

    def transform_sales_promo(self, master_store, df_sales_offline):
        df_promo = df_sales_offline.groupby(
            ["dataareaid", "axcode", "order_create_date", "promocode", "remarks"],
            as_index=False,
        )[["cust_paid_peritem"]].sum()

        df_promo["is_promo"] = np.where(
            df_promo["promocode"].isin(["", "regular"]), 0, 1
        )

        df_promo = master_store[
            [
                "dataareaid",
                "channel",
                "brand",
                "groupstore",
                "axcode",
                "stdname",
                "sqm",
                "sssg",
                "so_dept_head",
                "area_head",
                "city_head",
            ]
        ].merge(df_promo, "right", on=["dataareaid", "axcode"])

        return df_promo

    def transform_data_report(self):
        extractor = SalesAccumDataExtractor(month=self.month, year=self.year)
        pattern = extractor.extract_pattern()
        master_store = extractor.extract_master_store()
        master_product = extractor.extract_master_product(
            columns_list=[
                "dataareaid",
                "codebars",
                "itemid",
                "umbrellabrand",
                "subbrand",
                "world",
                "category",
                "subcategory",
            ]
        )

        # Get the last_month and last_year_month from the pattern
        last_month = pd.to_datetime(pattern["pattern_lm"]).dt.strftime("%m").unique()
        last_month = [x for x in last_month if x is not np.nan]
        last_year_month = (
            pd.to_datetime(pattern["pattern_last_year"]).dt.strftime("%m").unique()
        )
        last_year_month = [x for x in last_year_month if x is not np.nan]

        df_sales_offline = extractor.extract_sales_offline(
            month=self.month, year=self.year
        )
        df_sales_this_month = self.transform_sales(
            master_store=master_store,
            df_sales_offline=df_sales_offline,
            df_sales_online=extractor.extract_sales_online(
                month=self.month, year=self.year
            ),
        )

        df_sales_last_month = pd.concat(
            [
                self.transform_sales(
                    master_store=master_store,
                    df_sales_offline=extractor.extract_sales_offline(
                        month=month, year=self.year if month != "12" else self.last_year
                    ),
                    df_sales_online=extractor.extract_sales_online(
                        month=month, year=self.year if month != "12" else self.last_year
                    ),
                )
                for month in last_month
            ]
        )

        df_sales_last_year = pd.concat(
            [
                self.transform_sales(
                    master_store=master_store,
                    df_sales_offline=extractor.extract_sales_offline(
                        month=month, year=self.last_year
                    ),
                    df_sales_online=extractor.extract_sales_online(
                        month=month, year=self.last_year
                    ),
                )
                for month in last_year_month
            ]
        )

        df_sales_akum = self.transform_sales_akum(
            pattern=pattern,
            master_store=master_store,
            master_product=master_product,
            df_this_month=df_sales_this_month,
            df_last_month=df_sales_last_month,
            df_last_year=df_sales_last_year,
        )

        df_sales_promo = self.transform_sales_promo(
            master_store=master_store, df_sales_offline=df_sales_offline
        )

        print("Data This Month")
        self.data_validator(
            df_before=df_sales_this_month,
            columns_before="cust_paid_peritem",
            df_after=df_sales_akum,
            columns_after="cust_paid_peritem",
        )

        print("Data Last Month")
        self.data_validator(
            df_before=df_sales_last_month,
            columns_before="cust_paid_peritem",
            df_after=df_sales_akum,
            columns_after="cust_paid_peritem_lm",
        )

        print("Data Last Year")
        self.data_validator(
            df_before=df_sales_last_year,
            columns_before="cust_paid_peritem",
            df_after=df_sales_akum,
            columns_after="cust_paid_peritem_ly",
        )

        return df_sales_akum, df_sales_promo

    def data_validator(self, df_before, columns_before, df_after, columns_after):
        amount_before = df_before[columns_before].sum().round(2)
        amount_after = df_after[columns_after].sum().round(2)
        if amount_before - amount_after != 0:
            print("please check the data there is unmatched amount")
            print(f"Amount Before :", amount_before)
            print(f"Amount After :", amount_after)
        else:
            print("Amount is Matched")
