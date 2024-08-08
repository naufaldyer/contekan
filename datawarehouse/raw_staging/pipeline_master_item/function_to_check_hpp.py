import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")


def check_hpp_in_mp_master(df_final):
    """
    Process this function to check HPP of item megaperitis
    who register in master item mitrel

    Args:
        df (_type_): Master Item Mitral

    Returns:
        Dataframe : Dataframe master item mitrel
    """

    df_mp = pd.read_parquet(f"s3://mega-dev-lake/RawData/D365/Product/mpr/data.parquet")
    df_mp = df_mp[df_mp["PullStatus"] == "Current"]
    df_mp = df_mp.drop_duplicates(subset=["CodeBars"])

    df_current = df_final[
        (df_final["PullStatus"] == "Current")
        & (
            df_final["UmbrellaBrand"].isin(
                [
                    "Minimal",
                    "Mens top",
                    "Menstop",
                    "MOC",
                    "Moc",
                    "Manzone",
                    "Edwin",
                    "Due",
                ]
            )
        )
    ]

    df_current = df_current.merge(
        df_mp[["CodeBars", "HPP"]],
        "left",
        on=["CodeBars"],
        suffixes=("", "_inplace"),
        indicator=True,
    ).fillna({"HPP_inplace": 0})

    # HPP mp di MP naik 3%
    df_current["HPP_inplace"] = df_current["HPP_inplace"] * 1.03

    df_current["HPP"] = np.where(
        df_current["HPP"] == 0,
        df_current["HPP_inplace"],
        df_current["HPP"],
    )

    df_final = pd.concat(
        [
            df_final[
                ~(
                    (df_final["PullStatus"] == "Current")
                    & (
                        df_final["UmbrellaBrand"].isin(
                            [
                                "Minimal",
                                "Mens top",
                                "Menstop",
                                "MOC",
                                "Moc",
                                "Manzone",
                                "Edwin",
                                "Due",
                            ]
                        )
                    )
                )
            ],
            df_current,
        ]
    ).drop(columns=["HPP_inplace", "_merge"])
    return df_final


def transform_master_item(df, company):
    itable = pd.read_parquet(
        f"s3://mega-lake/RawData/SOH/INVENTTABLE/{company}/data.parquet"
    )
    itable = itable.drop_duplicates(subset=["CODEBARS"], keep="last")

    df_current = df[df["PullStatus"] == "Current"]
    df_current = df_current.merge(
        itable[["CODEBARS", "HPP"]].rename(columns={"CODEBARS": "CodeBars"}),
        "left",
        on=["CodeBars"],
        suffixes=("", "Ax"),
    ).fillna({"HPPAx": 0})

    df_current["HPP"] = np.where(
        df_current["HPP"] == 0, df_current["HPPAx"], df_current["HPP"]
    )

    df_final = pd.concat([df[df["PullStatus"] != "Current"], df_current]).fillna(
        {"HPPAx": 0}
    )

    if company == "mgl":
        df_final = check_hpp_in_mp_master(df_final)

    df_final = df_final[
        [
            "ItemId",
            "AluCode",
            "ArticleCode",
            "ItemName",
            "SearchName",
            "Style",
            "Colour",
            "ProductNumber",
            "VariantNumber",
            "AluCodeDetail",
            "CodeBars",
            "Size",
            "Gramasi",
            "Unit",
            "UmbrellaBrand",
            "SubBrand",
            "Department",
            "World",
            "Category",
            "SubCategory",
            "SubCategory2",
            "Season",
            "MonthYear",
            "HPPInput",
            "AverageHPP",
            "AvgHppFinalCosting",
            "HPPAx",
            "HPP",
            "BasicPrice",
            "SalePrice",
            "Stopped",
            "ItemGroup",
            "ItemModelGroup",
            "ProductType",
            "ProductSubType",
            "InventoryType",
            "ProductCategory",
            "ProductLooks",
            "Fitting",
            "Fitting2",
            "ModulePriceInventory",
            "ModulePriceSales",
            "ModulePricePurch",
            "ModuleInventoryUnit",
            "ModuleSalesUnit",
            "ModulePurchUnit",
            "ModifiedDateTimeDetail",
            "CreatedDateTime",
            "ModifiedDateTime",
            "RecId",
            "Notes",
            "PullStatus",
            "PullDate",
        ]
    ]

    return df_final


def main():
    for company in ["mpr", "mgl"]:
        df = pd.read_parquet(
            f"s3://mega-dev-lake/RawData/D365/Product/{company}/data.parquet"
        )

        df = transform_master_item(df=df, company=company)
        df.to_parquet(
            f"s3://mega-dev-lake/ProcessedData/D365/Product/{company}/data.parquet"
        )
