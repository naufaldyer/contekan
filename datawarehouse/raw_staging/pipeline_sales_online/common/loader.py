import pandas as pd
import numpy as np


def loader_marketplace(df, year, month):
    marketplace = df["Marketplace"].unique()[0]
    path = f"s3://mega-dev-lake/Staging/Sales/sales_online/marketplace/{marketplace.lower()}/{year}/{month}/data.parquet"
    df.to_parquet(path)
    print(f"Data Saved to {path}")
