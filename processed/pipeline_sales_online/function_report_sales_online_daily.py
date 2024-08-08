import pandas as pd
import numpy as np
import warnings
import datetime

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


def get_df(month, date):
    if month == "03":
        df = pd.concat(
            [
                pd.read_parquet(
                    f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/2024/{m}/data.parquet"
                )
                for m in ["03", "04"]
            ]
        )
    else:
        df = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/2024/{month}/data.parquet"
        )

    df = df[df.Date <= date]
    df["SUBCATEGORY"] = df["SUBCATEGORY"].fillna("NA")
    df["SUBCATEGORY"] = df["SUBCATEGORY"].str.replace(
        "PANTS - SARUNG", "SARUNG - PANTS"
    )

    # df["DescCategory"] = np.where(
    #     df["pc"].isin(["KAFTAN", "GAMIS", "SHANGHAI", "SARUNG", "KOKO"]), "FESTIVE", "REGULAR"
    # )

    df["DescCategory"] = np.where(
        ((df.SEASON == "OT") | (df.SUBCATEGORY.str.contains("SHANGHAI"))),
        "FESTIVE",
        "REGULAR",
    )

    df["pc"] = df["SUBCATEGORY"].str.split(" - ").str.get(0).str.strip()
    df["ProductCategory"] = np.where(
        ~df["pc"].isin(["KAFTAN", "GAMIS", "SHANGHAI", "SARUNG", "KOKO"]),
        "REGULAR",
        df["pc"],
    )

    if month == "03":
        df["day"] = np.where(
            pd.to_datetime(df["Date"]).dt.month == 3,
            pd.to_datetime(df["Date"]).dt.day,
            pd.to_datetime(
                df[pd.to_datetime(df["Date"]).dt.month == 3]["Date"]
            ).dt.day.max()
            + pd.to_datetime(df["Date"]).dt.day,
        )

        df["Week"] = (df["day"] - 1) // 7 + 1
        # df["Week"] = (pd.to_datetime(df["Date"]).dt.day - 1) // 7 + 1
        df["Date"] = df["Date"].astype(str)
    else:
        df["day"] = pd.to_datetime(df["Date"]).dt.day
        df["Week"] = (df["day"] - 1) // 7 + 1
        # df["Week"] = (pd.to_datetime(df["Date"]).dt.day - 1) // 7 + 1
        df["Date"] = df["Date"].astype(str)

    df = df[
        [
            "Brand",
            "Marketplace",
            "Week",
            "Date",
            "No Order",
            "Status Sales",
            "CATEGORY",
            "SUBCATEGORY",
            "SEASON",
            "DescCategory",
            "ProductCategory",
            "Qty Sold",
            "Discount",
            "Voucher",
            "Subsidi",
            "Value After Voucher",
            "HPP",
            "DPP",
            "Gross Profit (With DPP)",
        ]
    ]
    return df


def count_GP_pct(df):
    df["%GrossProfit"] = df["GrossProfit"] / df["DPP"]
    return df


def get_sales_status(df):
    daily = (
        df.groupby(
            ["Brand", "Marketplace", "Week", "Date", "Status Sales"], as_index=False
        )
        .agg(
            {
                "No Order": "nunique",
                "Qty Sold": "sum",
                "Discount": "sum",
                "Voucher": "sum",
                "Subsidi": "sum",
                "Value After Voucher": "sum",
                "HPP": "sum",
                "DPP": "sum",
                "Gross Profit (With DPP)": "sum",
            }
        )
        .rename(
            columns={
                "Status Sales": "StatusSales",
                "Value After Voucher": "Sales",
                "Gross Profit (With DPP)": "GrossProfit",
                "No Order": "OrderNumber",
                "Qty Sold": "Quantity",
            }
        )
    )
    daily = count_GP_pct(daily)
    return daily


def get_sales_daily(df):

    daily = (
        df[df["Status Sales"] != "CANCELED"]
        .groupby(["Brand", "Marketplace", "Week", "Date"], as_index=False)
        .agg(
            {
                "No Order": "nunique",
                "Qty Sold": "sum",
                "Discount": "sum",
                "Voucher": "sum",
                "Subsidi": "sum",
                "Value After Voucher": "sum",
                "HPP": "sum",
                "DPP": "sum",
                "Gross Profit (With DPP)": "sum",
            }
        )
        .rename(
            columns={
                "Value After Voucher": "Sales",
                "Gross Profit (With DPP)": "GrossProfit",
                "No Order": "OrderNumber",
                "Qty Sold": "Quantity",
            }
        )
    )

    daily = count_GP_pct(daily)

    # dataedwin = pd.DataFrame(
    #     {
    #         "Brand": "ED",
    #         "Marketplace": "SHOPIFY",
    #         "Week": 1,
    #         "Date": "2024-03-01",
    #         "Quantity": 0,
    #         "OrderNumber": 0,
    #         "Discount": 0,
    #         "Voucher": 0,
    #         "Subsidi": 0,
    #         "Sales": 0,
    #         "HPP": 0,
    #         "DPP": 0,
    #         "GrossProfit": 0,
    #         "%GrossProfit": 0.0,
    #     },
    #     index=[0],
    # )
    # datasaleszone = pd.DataFrame(
    #     {
    #         "Brand": "SZ",
    #         "Marketplace": "TIKTOK",
    #         "Week": 1,
    #         "Date": "2024-03-01",
    #         "Quantity": 0,
    #         "OrderNumber": 0,
    #         "Discount": 0,
    #         "Voucher": 0,
    #         "Subsidi": 0,
    #         "Sales": 0,
    #         "HPP": 0,
    #         "DPP": 0,
    #         "GrossProfit": 0,
    #         "%GrossProfit": 0.0,
    #     },
    #     index=[0],
    # )
    # daily = pd.concat([daily, dataedwin, datasaleszone])

    return daily


def get_sales_item(df):

    daily = (
        df[df["Status Sales"] != "CANCELED"]
        .groupby(
            ["Brand", "Marketplace", "Week", "Date", "DescCategory", "ProductCategory"],
            as_index=False,
        )
        .agg(
            {
                "No Order": "nunique",
                "Qty Sold": "sum",
                "Discount": "sum",
                "Voucher": "sum",
                "Subsidi": "sum",
                "Value After Voucher": "sum",
                "HPP": "sum",
                "DPP": "sum",
                "Gross Profit (With DPP)": "sum",
            }
        )
        .rename(
            columns={
                "Value After Voucher": "Sales",
                "Gross Profit (With DPP)": "GrossProfit",
                "No Order": "OrderNumber",
                "Qty Sold": "Quantity",
            }
        )
    )

    daily = count_GP_pct(daily)

    return daily


def get_pattern():
    pattern = pd.read_excel(
        "s3://mega-lake/ProcessedData/Sales/pattern/pattern_24_with_festive.xlsx",
        sheet_name="online_festive",
    ).drop(columns="pattern_lm")
    pattern[pattern.columns] = pattern[pattern.columns].astype(str)
    return pattern


def get_target():

    target_online = pd.read_excel(
        "s3://mega-dev-lake/Staging/Master/TargetOnlineFestive/Target Online.xlsx"
    )
    target_daily = target_online.drop(columns="Grandtotal").melt(
        id_vars=["STDNAME", "AREAID", "BRAND", "CHANNEL"],
        var_name="Date",
        value_name="Target",
    )

    target_daily["CHANNEL"] = np.where(
        target_daily["CHANNEL"].isin(
            [
                "MANZONESTORE.ID",
                "MINIMALSTORE.ID",
                "MOCSTORE.ID",
                "MATAHARISTORE.COM",
                "WEBSITE",
            ]
        ),
        "SHOPIFY",
        target_daily["CHANNEL"],
    )
    target_daily["Week"] = (pd.to_datetime(target_daily["Date"]).dt.day - 1) // 7 + 1
    target_daily["Month"] = pd.to_datetime(target_daily["Date"]).dt.strftime("%B")
    target_daily = target_daily.rename(
        columns={
            "AREAID": "dataareaid",
            "CHANNEL": "Marketplace",
        }
    ).drop(columns="STDNAME")

    project_festive = (
        target_daily.groupby(["BRAND", "Marketplace"], as_index=False)
        .agg({"Target": "sum"})
        .rename(columns={"Target": "Target Project"})
    )

    project_monthly = (
        target_daily.groupby(["Month", "BRAND", "Marketplace"], as_index=False)
        .agg({"Target": "sum"})
        .rename(columns={"Target": "Target Monthly"})
    )

    target = target_daily.merge(
        project_festive, "left", on=["BRAND", "Marketplace"]
    ).merge(project_monthly, "left", on=["Month", "BRAND", "Marketplace"])
    target.columns = target.columns.str.title().str.replace(" ", "")
    target["Date"] = pd.to_datetime(target["Date"]).dt.strftime("%Y-%m-%d")

    target["TargetMonthlyMarketplace"] = 0
    target["TargetProjectMarketplace"] = 0

    return target


def get_last_year_sales(list_month, list_year):
    df_ly = pd.DataFrame()

    for year in list_year:
        for month in list_month:
            try:
                data = pd.read_parquet(
                    f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{month}/data.parquet"
                )
                data["Date"] = data["Date"].astype(str)
                data["Status Sales"] = data["Status Sales"].str.replace(" ", "")
                data = (
                    data.groupby(["Brand", "Marketplace", "Date"], as_index=False)
                    .agg({"Value After Voucher": "sum"})
                    .rename(
                        columns={"Date": "DateLY", "Value After Voucher": "SalesLY"}
                    )
                )
                data["Marketplace"] = data["Marketplace"].str.upper()
                data["Marketplace"] = np.where(
                    data["Marketplace"].isin(
                        ["MANZONESTORE.ID", "MOCSTORE.ID", "MINIMALSTORE.ID", "WEBSITE"]
                    ),
                    "SHOPIFY",
                    data["Marketplace"],
                )
                df_ly = pd.concat([df_ly, data])
                # print(df_ly.columns)
            except:
                print(f"Data {year} {month} not here")

    return df_ly


def processing_main_data(month, date):
    df = get_df(month, date)

    daily = get_sales_daily(df)
    status = get_sales_status(df)
    item = get_sales_item(df)
    pattern = get_pattern()
    target = get_target()

    daily = daily.merge(pattern, "left", left_on=["Date"], right_on=["pattern_date"])

    list_month = pd.to_datetime(daily["pattern_last_year"]).dt.strftime("%m").unique()
    list_year = pd.to_datetime(daily["pattern_last_year"]).dt.strftime("%Y").unique()
    df_ly = get_last_year_sales(list_month=list_month, list_year=list_year)

    daily = (
        daily.merge(
            df_ly,
            "left",
            left_on=["pattern_last_year", "Brand", "Marketplace"],
            right_on=["DateLY", "Brand", "Marketplace"],
        )
        .drop(columns=["DateLY", "pattern_date"])
        .fillna({"SalesLY": 0})
    )

    daily = daily.merge(
        target[
            [
                "Brand",
                "Marketplace",
                "Date",
                "Target",
                "TargetMonthlyMarketplace",
                "TargetMonthly",
                "TargetProjectMarketplace",
                "TargetProject",
            ]
        ],
        "left",
        on=["Brand", "Marketplace", "Date"],
    ).fillna(0)

    daily["Month"] = pd.to_datetime(daily["Date"]).dt.strftime("%B")

    daily.loc[
        daily.duplicated(subset=["TargetProject", "Brand", "Marketplace"]),
        "TargetProject",
    ] = 0

    daily.loc[
        daily.duplicated(subset=["TargetMonthly", "Month", "Brand", "Marketplace"]),
        "TargetMonthly",
    ] = 0

    daily = daily.rename(columns={"pattern_last_year": "PatternLastYear"})

    return daily, status, item


def export(month, date):
    daily, status, item = processing_main_data(month, date)

    if daily["Sales"].sum() == item["Sales"].sum():
        if daily["Quantity"].sum() == item["Quantity"].sum():
            with pd.ExcelWriter(
                f"s3://report-deliverables/sales-report/online_daily_report/{month}/data.xlsx"
            ) as wr:
                item.to_excel(wr, sheet_name="RawItem", index=False)
                status.to_excel(wr, sheet_name="RawSalesStatus", index=False)
                daily.to_excel(wr, sheet_name="RawDailySales", index=False)
            print("Success")
        else:
            print("Please Check Quantity")
    else:
        print("Please Check Sales")
