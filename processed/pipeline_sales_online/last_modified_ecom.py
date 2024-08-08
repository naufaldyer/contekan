import pandas as pd
import numpy as np
import sys
import boto3
import datetime
import openpyxl
import s3fs
import warnings
import re
import calendar

warnings.filterwarnings("ignore")


# Define a function to remove symbols using regex, excluding hyphen
def remove_symbols(text):
    # Define a regex pattern to match non-alphanumeric characters and spaces, excluding hyphen
    pattern = r"[^a-zA-Z0-9\s-]"
    # Use re.sub to replace matched patterns with an empty string
    return re.sub(pattern, "", text)


def getCheckLastUpdated():
    # Initialize an S3 client
    s3 = boto3.client("s3", region_name="ap-southeast-3")

    # Specify the S3 bucket and prefix
    bucket_name = "mp-users-report"
    prefix = "e-commerce/raw/"

    # List objects in the S3 bucket
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    now = datetime.datetime.now()
    this_month = now.strftime("%B")
    month_num = now.strftime("%m")
    month_name = [this_month]
    year = now.year
    today = now.strftime("%Y-%m-%d")

    if now.day == 1:
        last_month_num = now.month - 1
        max_day = calendar.monthrange(year, last_month_num)[1]
        last_month_date = (
            now.replace(month=last_month_num).replace(day=max_day).strftime("%Y-%m-%d")
        )
        last_month_name = pd.to_datetime(last_month_date).strftime("%B")
        month_name = [last_month_name]

    # Create empty lists to store the data

    brands = []
    months = []
    last_modified_times = []
    files = []
    ecoms = []

    for m in month_name:
        # Iterate through the S3 objects and collect data
        for obj in objects.get("Contents", []):
            key = obj["Key"]
            # Extract brand and month information from the S3 object key
            if key.endswith("csv") or key.endswith("xlsx"):
                if key.split("/")[3] == "2024" and key.split("/")[4] == m:
                    brands.append(key.split("/")[-4])
                    months.append(key.split("/")[4])
                    last_modified_times.append(obj["LastModified"])
                    files.append(key.split("/")[-1])
                    ecoms.append(key.split("/")[-1].split("_")[0])

        # Create Dataframe
        df = pd.DataFrame(
            {
                "Month": months,
                "Brand": brands,
                "Ecommerce": ecoms,
                "Files": files,
                "Last Upload": last_modified_times,
            }
        )

        # Normalize ecommerce name
        df["Ecommerce"] = (
            df["Ecommerce"].apply(remove_symbols).str.replace("-", "").str.title()
        )

        # Standarize Date and Time Columns
        df["Last Upload"] = (
            df["Last Upload"] + datetime.timedelta(hours=7)
        ).dt.strftime("%Y-%m-%d %H:%M")
        df["Last Upload Date"] = pd.to_datetime(df["Last Upload"]).dt.strftime(
            "%Y-%m-%d"
        )
        df["Last Upload Time"] = pd.to_datetime(df["Last Upload"]).dt.strftime("%H:%M")

        # Create columns for check uptodate the files
        df["Check Upload"] = np.where(
            pd.to_datetime(df["Last Upload Date"]) < pd.to_datetime(today),
            "Not Up To Date",
            "Up To Date",
        )

        # Create columns for checking late or ontime the files
        df["Is Late"] = np.where(
            df["Last Upload Date"] == today,
            np.where(
                pd.to_datetime(df["Last Upload Time"]) > "10:00", "Late", "Ontime"
            ),
            "Late",
        )

        df_agg = df.groupby(["Month", "Brand", "Ecommerce"], as_index=()).size()
        df_agg["Is Duplicated"] = np.where(
            df_agg["size"] > 1, "Duplicated", "Not Duplicated"
        )
        df = df.merge(df_agg, "left", on=["Month", "Brand", "Ecommerce"]).rename(
            columns={"size": "Total Files"}
        )

        df.to_excel(
            f"s3://report-deliverables/sales-report/online_upload_monitoring/{year}/{month_num}/data.xlsx",
            index=False,
        )
        df.to_excel(
            f"s3://production-it/online_upload_monitoring/{year}/{month_num}/{today}/data.xlsx",
            index=False,
        )
