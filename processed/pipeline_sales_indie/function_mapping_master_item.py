import pandas as pd
import numpy as np

import warnings

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


def mapping_new_master_item(company, df):
    df_product = df_master_product = pd.read_parquet(
        f"s3://mega-dev-lake/RawData/D365/Product/{company}/data.parquet"
    )
    df_master_product = df_master_product[df_master_product["PullStatus"] == "Current"]
    df_master_product = df_master_product.drop_duplicates(
        subset="VariantNumber", keep="last"
    )
    df_master_product = df_master_product.drop_duplicates(
        subset="CodeBars", keep="last"
    )

    df_master_product_selected = df_master_product[
        [
            "ItemId",
            "AluCode",
            "ArticleCode",
            "ItemName",
            "CodeBars",
            "HPP",
            "UmbrellaBrand",
            "SubBrand",
            "Department",
            "Style",
            "World",
            "Category",
            "SubCategory",
            "Size",
            "Season",
            "MonthYear",
            "BasicPrice",
            "SalePrice",
        ]
    ]

    df_master_product_selected.columns = df_master_product_selected.columns.str.lower()
    df_master_product_selected = df_master_product_selected.rename(
        columns={"monthyear": "yearmonth", "size": "size_"}
    )
    df_master_product_selected["yearmonth"] = pd.to_datetime(
        df_master_product_selected["yearmonth"]
    )

    df_joined = df.merge(
        df_master_product_selected,
        "left",
        left_on="productdata_barcode",
        right_on="codebars",
        suffixes=("_drop", ""),
    )

    df_joined[df_joined.select_dtypes(include="number").columns] = df_joined[
        df_joined.select_dtypes(include="number").columns
    ].fillna(0)

    df_joined[df_joined.select_dtypes(include="object").columns] = df_joined[
        df_joined.select_dtypes(include="object").columns
    ].fillna("undetected")

    df_joined["hpp"] = np.where(
        df_joined["hpp"] == 0, df_joined["hpp_drop"], df_joined["hpp"]
    )

    df_joined["hpp"] = np.where(
        (df_joined["hpp"] != 0) & (df_joined["cost_of_margin"] != 0),
        0,
        df_joined["hpp"],
    )

    df_joined["hpp_total"] = (
        df_joined["hpp"] + df_joined["cost_of_margin"]
    ) * df_joined["quantity"]

    df_joined = df_joined[[x for x in df_joined.columns if ("_drop") not in x]]

    return df_joined
