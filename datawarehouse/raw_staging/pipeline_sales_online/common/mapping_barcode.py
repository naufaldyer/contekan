import pandas as pd
import numpy as np


def mapping_hpp(df, company):
    itable = pd.read_parquet(
        f"s3://mega-lake/RawData/SOH/INVENTTABLE/{company}/data.parquet"
    )[["CODEBARS", "HPP"]]
    itable = itable.drop_duplicates(subset="CODEBARS")
    df = df.merge(itable, "left", on=["CODEBARS"], suffixes=("", "_inplace")).fillna(
        {"HPP_inplace": 0}
    )
    df["HPP"] = np.where(df["HPP"] == 0, df["HPP_inplace"], df["HPP"])
    df = df.drop(columns="HPP_inplace")
    return df


def mapping_barcode(
    df_marketplace: pd.DataFrame,
    columns_marketplace: str,
    company: str,
):
    df_product = pd.read_parquet(
        f"s3://mega-dev-lake/RawData/D365/Product/{company}/data.parquet"
    )
    df_product = df_product[df_product["PullStatus"] == "Current"]
    df_product = df_product.drop_duplicates(subset=["CodeBars"], keep="last")

    df_test = df_marketplace.merge(
        df_product[["CodeBars"]],
        "left",
        left_on=columns_marketplace,
        right_on="CodeBars",
    )

    if df_test["CodeBars"].isna().sum() > 0:
        df_test["CodeBars"] = np.where(
            df_test["CodeBars"].isna(),
            "0" + df_test[columns_marketplace],
            df_test["CodeBars"],
        )

        df_test = df_test.rename(columns={"CodeBars": "CodeBars_drop"}).merge(
            df_product[["CodeBars"]],
            "left",
            left_on=["CodeBars_drop"],
            right_on=["CodeBars"],
        )

        df_test = df_test.drop(columns="CodeBars_drop")

    columns_product = [
        "ItemId",
        "AluCode",
        "CodeBars",
        "UmbrellaBrand",
        "SubBrand",
        "Season",
        "World",
        "Style",
        "Category",
        "SubCategory",
        "MonthYear",
        "Size",
        "HPP",
        "BasicPrice",
        "SalePrice",
    ]
    df_test = df_test.merge(
        df_product[~df_product["CodeBars"].isna()][columns_product],
        "left",
        on=["CodeBars"],
    )
    df_test.columns = [
        x.upper() if x in columns_product else x for x in df_test.columns
    ]
    df_test["MONTHYEAR"] = pd.to_datetime(df_test["MONTHYEAR"]).dt.strftime("%Y-%m-%d")
    df_test = df_test.rename(columns={"MONTHYEAR": "YEARMONTH"})

    df_test = mapping_hpp(df_test, company)

    return df_test
